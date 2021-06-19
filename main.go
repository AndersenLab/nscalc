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
	"google.golang.org/api/iterator"
	lifesciences "google.golang.org/api/lifesciences/v2beta"
	"google.golang.org/api/option"
)

const PROJECT_ID = "andersen-lab"
const XCloudtasksQueuename = "nscalc"

const SERVICE_ACCOUNT = "nscalc-201573431837@andersen-lab.iam.gserviceaccount.com"
const SCOPE = "https://www.googleapis.com/auth/cloud-platform"

const IMAGE_URI = "northwesternmti/nemarun:0.61"
const PUB_SUB_TOPIC = "projects/andersen-lab/topics/nemarun"

const MACHINE_TYPE = "n1-standard-1"
const PREEMPTIBLE = false
const ZONE = "us-central1-a"
const TIMEOUT = "86400s"
const BOOT_IMAGE = "projects/cos-cloud/global/images/family/cos-stable"
const PARENT = "projects/201573431837/locations/us-central1"
const NS_DATA_PATH = "gs://elegansvariation.org/reports/nemascan"
const NS_WORK_PATH = "gs://nf-pipelines/workdir"
const LOCAL_WORK_PATH = "/workdir"
const VOLUME_NAME = "nf-pipeline-work"

const DS_OPERATION_KIND = "gls_operation"

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

type operationOptions struct {
	Data_path       string `datastore:"data_path"`
	Work_path       string `datastore:"work_path"`
	Container_Name  string `datastore:"container_name"`
	Volume_Name     string `datastore:"volume_name"`
	Image_URI       string `datastore:"image_uri"`
	Machine_Type    string `datastore:"machine_type"`
	Preemptible     bool   `datastore:"preemptible"`
	Zone            string `datastore:"zone"`
	Timeout         string `datastore:"timeout"`
	Boot_Image      string `datastore:"boot_image"`
	Project_ID      string `datastore:"project_id"`
	Service_Account string `datastore:"service_account"`
	Scope           string `datastore:"scope"`
	Parent          string `datastore:"parent"`
	PubSub_Topic    string `datastore:"pubsub_topic"`
}

type dsOperationEntry struct {
	Options     *operationOptions `datastore:"options,noindex"`
	Done        bool              `datastore:"done"`
	Error       bool              `datastore:"error"`
	Data_hash   string            `datastore:"data_hash"`
	Operation   string            `datastore:"operation"`
	Report_path string            `datastore:"report_path,noindex"`
	Modified_on time.Time         `datastore:"modified_on"`
	Created_on  time.Time         `datastore:"created_on"`
	K           *datastore.Key    `datastore:"__key__"`
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
		setDatastoreStatus(i, "ERROR", msg)
		panic(e)
	}
}

func createOperationDSEntry(dsOp *dsOperationEntry) {
	ctx := context.Background()
	dsClient, err := datastore.NewClient(ctx, PROJECT_ID)
	if err != nil {
		log.Printf("dsClient.NewClient: %v", err)
	}
	defer dsClient.Close()

	taskKey := datastore.NameKey(DS_OPERATION_KIND, dsOp.Data_hash, nil)
	_, txErr := dsClient.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		// We first check that there is no entity stored with the given key.
		var empty dsOperationEntry
		if err := tx.Get(taskKey, &empty); err != datastore.ErrNoSuchEntity {
			return err
		}
		// If there was no matching entity, store it now.
		_, err := tx.Put(taskKey, dsOp)
		return err
	})
	if txErr != nil {
		log.Printf("ERROR CREATING OPERATION DS ENTRY: %s", txErr.Error())
	}
}

func updateOperationDSEntry(opID string, opDone bool, opErr bool) {
	ctx := context.Background()
	dsClient, err := datastore.NewClient(ctx, PROJECT_ID)
	if err != nil {
		log.Printf("dsClient.NewClient: %v", err)
	}
	defer dsClient.Close()

	// query for matching data_hash
	var data_hash = ""
	query := datastore.NewQuery(DS_OPERATION_KIND).Filter("Operation =", opID)
	it := dsClient.Run(ctx, query)
	for {
		var opEntry dsOperationEntry
		_, err := it.Next(&opEntry)
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Printf("Error fetching next task: %v", err)
		}
		log.Printf("OPERATION: %s\n HASH: %s\n", opEntry.Operation, opEntry.Data_hash)
		data_hash = opEntry.Data_hash
	}

	// Update DS entry
	if data_hash != "" {
		opKey := datastore.NameKey(DS_OPERATION_KIND, data_hash, nil)
		tx, err := dsClient.NewTransaction(ctx)
		if err != nil {
			log.Printf("client.NewTransaction: %v", err)
		}
		var dsOp dsOperationEntry
		if err := tx.Get(opKey, &dsOp); err != nil {
			log.Printf("tx.Get: %v", err)
		}

		dsOp.Modified_on = time.Now()
		dsOp.Done = opDone
		dsOp.Error = opErr
		if _, err := tx.Put(opKey, &dsOp); err != nil {
			log.Printf("tx.Put: %v", err)
		}
		if _, err := tx.Commit(); err != nil {
			log.Printf("tx.Commit: %v", err)
		}
	}

}

// helper function to update status in datastore
func setDatastoreStatus(i *dsInfo, status string, msg string) {
	ctx := context.Background()
	dsClient, err := datastore.NewClient(ctx, PROJECT_ID)
	if err != nil {
		log.Printf("client.NewClient: %v", err)
	}
	defer dsClient.Close()

	k := datastore.NameKey(i.Kind, i.Id, nil)
	e := new(dsEntry)
	if err := dsClient.Get(ctx, k, e); err != nil {
		log.Printf("client.Get: %v", err)
	}

	e.Status = status
	e.Status_msg = msg

	if _, err := dsClient.Put(ctx, k, e); err != nil {
		log.Printf("client.Put: %v", err)
	}

	log.Printf("Updated status: %q\n", e.Status)
	log.Printf("Updated status msg: %q\n", e.Status_msg)
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

func generatePipelineOpts(Data_hash string) *operationOptions {
	// Configure the options for the pipeline
	NS_CONTAINER_NAME := fmt.Sprintf("nemarun-%s", Data_hash)
	return &operationOptions{
		Data_path:       NS_DATA_PATH,
		Work_path:       NS_WORK_PATH,
		Container_Name:  NS_CONTAINER_NAME,
		Volume_Name:     VOLUME_NAME,
		Image_URI:       IMAGE_URI,
		Machine_Type:    MACHINE_TYPE,
		Preemptible:     PREEMPTIBLE,
		Zone:            ZONE,
		Timeout:         TIMEOUT,
		Boot_Image:      BOOT_IMAGE,
		Project_ID:      PROJECT_ID,
		Service_Account: SERVICE_ACCOUNT,
		Scope:           SCOPE,
		Parent:          PARENT,
		PubSub_Topic:    PUB_SUB_TOPIC,
	}
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
	setDatastoreStatus(&info, "STARTING", "")
	log.Printf("INITIALIZING task: KIND: %s, ID: %s, HASH: %s", info.Kind, info.Id, info.Data_hash)

	// Generate struct of options for the Pipeline
	pOpts := generatePipelineOpts(info.Data_hash)

	// Start the nextflow pipeline
	operationID := executeRunPipelineRequest(&info, pOpts)

	// Store the operation info in DataStore
	pDSOperations := &dsOperationEntry{
		Options:     pOpts,
		Done:        false,
		Error:       false,
		Data_hash:   info.Data_hash,
		Operation:   operationID,
		Report_path: "",
		Modified_on: time.Time{},
		Created_on:  time.Now(),
		K:           &datastore.Key{},
	}
	createOperationDSEntry(pDSOperations)

	// Log & output details of the task.
	setDatastoreStatus(&info, "RUNNING", operationID)
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

func generateRunPipelineRequest(i *dsInfo, pOpts *operationOptions) *lifesciences.RunPipelineRequest {
	NS_ID := i.Data_hash
	NS_CONTAINER_NAME := fmt.Sprintf("nemarun-%s", NS_ID)

	pServiceAccount := &lifesciences.ServiceAccount{
		Email:           SERVICE_ACCOUNT,
		Scopes:          []string{SCOPE},
		ForceSendFields: []string{},
		NullFields:      []string{},
	}

	pVirtualMachine := &lifesciences.VirtualMachine{
		BootDiskSizeGb:              100,
		BootImage:                   BOOT_IMAGE,
		EnableStackdriverMonitoring: true,
		MachineType:                 MACHINE_TYPE,
		Preemptible:                 PREEMPTIBLE,
		ServiceAccount:              pServiceAccount,
		ForceSendFields:             []string{},
		NullFields:                  []string{},
	}

	pResources := &lifesciences.Resources{
		VirtualMachine:  pVirtualMachine,
		Zones:           []string{ZONE},
		ForceSendFields: []string{},
		NullFields:      []string{},
	}

	var TRAIT_FILE = fmt.Sprintf("%s/%s/data.tsv", NS_DATA_PATH, NS_ID)
	var OUTPUT_DIR = fmt.Sprintf("%s/%s/results", NS_DATA_PATH, NS_ID)
	var WORK_DIR = fmt.Sprintf("%s/%s", NS_WORK_PATH, NS_ID)
	pAction := &lifesciences.Action{
		AlwaysRun:                   false,
		BlockExternalNetwork:        false,
		Commands:                    []string{"/nemarun/nemarun.sh"},
		ContainerName:               NS_CONTAINER_NAME,
		DisableImagePrefetch:        false,
		DisableStandardErrorCapture: false,
		EnableFuse:                  false,
		Environment:                 map[string]string{"TRAIT_FILE": TRAIT_FILE, "OUTPUT_DIR": OUTPUT_DIR, "WORK_DIR": WORK_DIR},
		IgnoreExitStatus:            false,
		ImageUri:                    IMAGE_URI,
		PublishExposedPorts:         false,
		RunInBackground:             false,
		Timeout:                     TIMEOUT,
		ForceSendFields:             []string{},
		NullFields:                  []string{},
	}

	pPipeline := &lifesciences.Pipeline{
		Actions:         []*lifesciences.Action{pAction},
		Resources:       pResources,
		Timeout:         TIMEOUT,
		ForceSendFields: []string{},
		NullFields:      []string{},
	}

	*pOpts = operationOptions{
		Data_path:       NS_DATA_PATH,
		Work_path:       NS_WORK_PATH,
		Container_Name:  NS_CONTAINER_NAME,
		Volume_Name:     VOLUME_NAME,
		Image_URI:       IMAGE_URI,
		Machine_Type:    MACHINE_TYPE,
		Preemptible:     PREEMPTIBLE,
		Zone:            ZONE,
		Timeout:         TIMEOUT,
		Boot_Image:      BOOT_IMAGE,
		Project_ID:      PROJECT_ID,
		Service_Account: SERVICE_ACCOUNT,
		Scope:           SCOPE,
		Parent:          PARENT,
		PubSub_Topic:    PUB_SUB_TOPIC,
	}

	return &lifesciences.RunPipelineRequest{
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
		log.Printf("glsClient.DefaultClient: %v", gClientErr)
	}

	glsService, glsServiceErr := lifesciences.NewService(ctx, option.WithHTTPClient(gClient))
	if glsServiceErr != nil {
		log.Printf("glsClient.NewService: %v", glsServiceErr)
	}

	resp, err := glsService.Projects.Locations.Operations.Get(op).Context(ctx).Do()
	if err != nil {
		log.Printf("glsClient.Projects.Locations.Operations.Get().Context().Do(): %v", err)
	}
	var opDone = false
	if resp.Done {
		opDone = true
	}
	var opErr = false
	if resp.Error != nil {
		opErr = true
	}
	opName := resp.Name

	// TODO: Change code below to process the `resp` object:
	fmt.Printf("STATUS - NAME:%s DONE:%v ERROR:%#v\n", opName, opDone, opErr)
	updateOperationDSEntry(op, opDone, opErr)

}

func executeRunPipelineRequest(pInfo *dsInfo, pOpts *operationOptions) string {
	ctx := context.Background()

	gClient, gClientErr := google.DefaultClient(ctx, lifesciences.CloudPlatformScope)
	check(gClientErr, pInfo)

	glsService, glsServiceErr := lifesciences.NewService(ctx, option.WithHTTPClient(gClient))
	check(glsServiceErr, pInfo)

	runPipelineRequest := generateRunPipelineRequest(pInfo, pOpts)
	pOperation := &lifesciences.Operation{
		Done:     false,
		Error:    &lifesciences.Status{},
		Metadata: []byte{},
		Response: []byte{},
		ServerResponse: googleapi.ServerResponse{
			HTTPStatusCode: 0,
			Header:         map[string][]string{},
		},
		ForceSendFields: []string{},
		NullFields:      []string{},
	}

	pOperation, pipelineRunErr := glsService.Projects.Locations.Pipelines.Run(PARENT, runPipelineRequest).Context(ctx).Do()
	check(pipelineRunErr, pInfo)

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
