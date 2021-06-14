package main

// VERSION v2

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"

	"cloud.google.com/go/datastore"
	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/googleapi"
	lifesciences "google.golang.org/api/lifesciences/v2beta"
	"google.golang.org/api/option"
)

const PROJECT_ID = "andersen-lab"
const XCloudtasksQueuename = "nscalc"

const SERVICE_ACCOUNT = "nscalc-201573431837@andersen-lab.iam.gserviceaccount.com"
const SCOPE = "https://www.googleapis.com/auth/cloud-platform"

const IMAGE_URI = "northwesternmti/nemarun:0.49"
const PUB_SUB_TOPIC = "projects/andersen-lab/topics/nemarun"

const MACHINE_TYPE = "n1-standard-4"
const REGION = "us-central1"
const TIMEOUT = "86400s"
const BOOT_IMAGE = "projects/cos-cloud/global/images/family/cos-stable"

const PARENT = "projects/201573431837/locations/us-central1"

type Payload struct {
	Hash    string
	Ds_id   string
	Ds_kind string
}

type dsInfo struct {
	Kind      string
	Id        string
	Msg       string
	Data_hash string
}

type dsEntry struct {
	Username    string         `datastore:"username"`
	Label       string         `datastore:"label"`
	Data_hash   string         `datastore:"data_hash"`
	Operation   string         `datastore:"operation"`
	Trait       string         `datastore:"trait"`
	Status      string         `datastore:"status"`
	Status_msg  string         `datastore:"status_msg,noindex"`
	Report_path string         `datastore:"report_path,noindex"`
	Modified_on time.Time      `datastore:"modified_on"`
	Created_on  time.Time      `datastore:"created_on"`
	K           *datastore.Key `datastore:"__key__"`
}

type Attr struct {
	Operation string    `json:"operation"`
	Timestamp time.Time `json:"timestamp"`
}

// PubSubMessage is the payload of a Pub/Sub event.
// See the documentation for more details:
// https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage
type PubSubMessage struct {
	Message struct {
		Attributes Attr   `json:"attributes"`
		Data       []byte `json:"data,omitempty"`
		ID         string `json:"id"`
	} `json:"message"`
	Subscription string `json:"subscription"`
}

// Check error and report failure in datastore
func check(e error, i *dsInfo) {
	if e != nil {
		msg := e.Error() + "\n" + i.Msg
		log.Printf("ERROR: %s", e.Error())
		setDatastoreStatus(i, "ERROR", msg, "")
		panic(e)
	}
}

// helper function to update status in datastore
func setDatastoreStatus(i *dsInfo, status string, msg string, op string) {
	ctx := context.Background()
	dsClient, err := datastore.NewClient(ctx, PROJECT_ID)
	if err != nil {
		log.Fatal(err)
	}
	defer dsClient.Close()

	k := datastore.NameKey(i.Kind, i.Id, nil)
	e := new(dsEntry)
	if err := dsClient.Get(ctx, k, e); err != nil {
		log.Fatal(err)
	}

	e.Status = status
	e.Status_msg = msg
	if op != "" {
		e.Operation = op
	}

	if _, err := dsClient.Put(ctx, k, e); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Updated status: %q\n", e.Status)
	fmt.Printf("Updated status msg: %q\n", e.Status_msg)
}

func verifyHeader(r *http.Request) bool {
	// Check header to verify the task name and queue are present and queue is correct
	var result bool = true
	taskName := r.Header.Get("X-Cloudtasks-Taskname")
	if taskName == "" {
		log.Println("Invalid Task: No X-Appengine-Taskname request header found")
		result = false
	}

	queueName := r.Header.Get("X-Cloudtasks-Queuename")
	if queueName != XCloudtasksQueuename {
		log.Printf("Invalid Task Queue: X-Cloudtasks-Queuename request header (%s) does not match expected value (%s)", queueName, XCloudtasksQueuename)
		result = false
	}

	log.Printf("Received Task: %s:%s", queueName, taskName)
	return result
}

func extractPayload(r *http.Request) (dsInfo, error) {
	var info dsInfo

	// extract post body from request
	body, readErr := ioutil.ReadAll(r.Body)
	if readErr != nil {
		log.Printf("ReadAll: %v", readErr)
		return info, readErr
	}

	// Marshal json body to Payload struct
	var p Payload
	errMarshall := json.Unmarshal([]byte(string(body)), &p)
	if errMarshall != nil {
		log.Printf("Error parsing payload JSON: %v", errMarshall)
		return info, errMarshall
	}

	// Convert payload to dsInfo
	log.Printf("payload: %+v", p)
	info = dsInfo{Kind: p.Ds_kind, Id: p.Ds_id, Data_hash: p.Hash}

	return info, nil
}

func nsHandler(w http.ResponseWriter, r *http.Request) {
	//	Handler for google cloud task which starts the NemaScan nextflow pipeline
	if !verifyHeader(r) {
		http.Error(w, "Bad Request - Invalid Task", http.StatusBadRequest)
	}

	info, err := extractPayload(r)
	if err != nil {
		http.Error(w, "Internal Error", http.StatusInternalServerError)
	}

	// Log & output details of the task.
	setDatastoreStatus(&info, "STARTING", "", "")
	log.Printf("INITIALIZING task: KIND: %s, ID: %s, HASH: %s", info.Kind, info.Id, info.Data_hash)

	// Start the nextflow pipeline
	operationID := executeRunPipelineRequest(&info)

	// Log & output details of the task.
	setDatastoreStatus(&info, "RUNNING", "", operationID)
	log.Printf("RUNNING task: KIND: %s, ID: %s, HASH: %s", info.Kind, info.Id, info.Data_hash)

	if errJSONEncoder := json.NewEncoder(w).Encode("Submitted NemaScan"); errJSONEncoder != nil {
		log.Printf("Error sending response: %v", errJSONEncoder)
	}

	// Set a non-2xx status code to indicate a failure in task processing that should be retried.
	// For example, http.Error(w, "Internal Server Error: Task Processing", http.StatusInternalServerError)
	fmt.Fprintln(w, "200 OK")
}

// statusHandler receives and processes a Pub/Sub push message from the
// Google Life Sciences pipeline to update the status of a long-running task (Nextflow/NemaScan)
func statusHandler(w http.ResponseWriter, r *http.Request) {
	var m PubSubMessage
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Printf("ioutil.ReadAll: %v", err)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
	if err := json.Unmarshal(body, &m); err != nil {
		log.Printf("json.Unmarshal: %v", err)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
	log.Printf("PUBSUB:\t%s", string(body))
	getPipelineStatus(m.Message.Attributes.Operation)
}

func generateRunPipelineRequest(i *dsInfo) *lifesciences.RunPipelineRequest {
	NS_ID := i.Data_hash
	NS_DATA_PATH := "gs://elegansvariation.org/reports/nemascan"
	NS_WORK_PATH := "gs://nf-pipelines/workdir"
	NS_CONTAINER_NAME := fmt.Sprintf("nemarun-%s", i.Data_hash)
	LOCAL_WORK_PATH := "/workdir"

	pServiceAccount := &lifesciences.ServiceAccount{
		Email:           SERVICE_ACCOUNT,
		Scopes:          []string{SCOPE},
		ForceSendFields: []string{},
		NullFields:      []string{},
	}

	pMount := &lifesciences.Mount{
		Disk:            "nf-pipeline-work",
		Path:            LOCAL_WORK_PATH,
		ReadOnly:        false,
		ForceSendFields: []string{},
		NullFields:      []string{},
	}

	pPersistentDisk := &lifesciences.PersistentDisk{
		SizeGb:          500,
		ForceSendFields: []string{},
		NullFields:      []string{},
	}

	pVolume := &lifesciences.Volume{
		PersistentDisk:  pPersistentDisk,
		Volume:          "nf-pipeline-work",
		ForceSendFields: []string{},
		NullFields:      []string{},
	}

	pVirtualMachine := &lifesciences.VirtualMachine{
		BootDiskSizeGb:              10,
		BootImage:                   BOOT_IMAGE,
		DockerCacheImages:           []string{},
		EnableStackdriverMonitoring: true,
		Labels:                      map[string]string{},
		MachineType:                 MACHINE_TYPE,
		Preemptible:                 true,
		ServiceAccount:              pServiceAccount,
		Volumes:                     []*lifesciences.Volume{pVolume},
		ForceSendFields:             []string{},
		NullFields:                  []string{},
	}

	pResources := &lifesciences.Resources{
		Regions:         []string{REGION},
		VirtualMachine:  pVirtualMachine,
		Zones:           []string{},
		ForceSendFields: []string{},
		NullFields:      []string{},
	}

	pAction := &lifesciences.Action{
		AlwaysRun:                   false,
		BlockExternalNetwork:        false,
		Commands:                    []string{"nemarun.sh", NS_ID, NS_DATA_PATH, NS_WORK_PATH},
		ContainerName:               NS_CONTAINER_NAME,
		DisableImagePrefetch:        false,
		DisableStandardErrorCapture: false,
		EnableFuse:                  false,
		Entrypoint:                  "/bin/bash",
		Environment:                 map[string]string{},
		IgnoreExitStatus:            false,
		ImageUri:                    IMAGE_URI,
		Labels:                      map[string]string{},
		Mounts:                      []*lifesciences.Mount{pMount},
		PortMappings:                map[string]int64{},
		PublishExposedPorts:         false,
		RunInBackground:             false,
		Timeout:                     TIMEOUT,
		ForceSendFields:             []string{},
		NullFields:                  []string{},
	}

	pPipeline := &lifesciences.Pipeline{
		Actions:         []*lifesciences.Action{pAction},
		Environment:     map[string]string{},
		Resources:       pResources,
		Timeout:         TIMEOUT,
		ForceSendFields: []string{},
		NullFields:      []string{},
	}

	return &lifesciences.RunPipelineRequest{
		Labels:          map[string]string{},
		Pipeline:        pPipeline,
		PubSubTopic:     PUB_SUB_TOPIC,
		ForceSendFields: []string{},
		NullFields:      []string{},
	}
}

func getPipelineStatus(op string) {
	ctx := context.Background()

	gClient, gClientErr := google.DefaultClient(ctx, lifesciences.CloudPlatformScope)
	if gClientErr != nil {
		log.Fatal(gClientErr)
	}

	glsService, glsServiceErr := lifesciences.NewService(ctx, option.WithHTTPClient(gClient))
	if glsServiceErr != nil {
		log.Fatal(glsServiceErr)
	}

	resp, err := glsService.Projects.Locations.Operations.Get(op).Context(ctx).Do()
	if err != nil {
		log.Fatal(err)
	}
	var opDone = "FALSE"
	if resp.Done {
		opDone = "TRUE"
	}
	opError := resp.Error
	opName := resp.Name

	// TODO: Change code below to process the `resp` object:
	fmt.Printf("STATUS - NAME:%s DONE:%s ERROR:%#v\n", opName, opDone, opError)
}

func executeRunPipelineRequest(i *dsInfo) string {
	ctx := context.Background()

	gClient, gClientErr := google.DefaultClient(ctx, lifesciences.CloudPlatformScope)
	check(gClientErr, i)

	glsService, glsServiceErr := lifesciences.NewService(ctx, option.WithHTTPClient(gClient))
	check(glsServiceErr, i)

	runPipelineRequest := generateRunPipelineRequest(i)
	pOperation := &lifesciences.Operation{
		Done:            false,
		Error:           &lifesciences.Status{},
		Metadata:        []byte{},
		Response:        []byte{},
		ServerResponse:  googleapi.ServerResponse{},
		ForceSendFields: []string{},
		NullFields:      []string{},
	}

	pOperation, pipelineRunErr := glsService.Projects.Locations.Pipelines.Run(PARENT, runPipelineRequest).Context(ctx).Do()
	check(pipelineRunErr, i)

	// TODO: check server response code
	operationID := pOperation.Name
	return operationID
}

// indexHandler responds to requests with our greeting.
func indexHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	fmt.Fprint(w, "200 OK")
}

func main() {
	http.HandleFunc("/", indexHandler)
	http.HandleFunc("/ns", nsHandler)
	http.HandleFunc("/status", statusHandler)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("listening on %s", port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", port), nil))
}
