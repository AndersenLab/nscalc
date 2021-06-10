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

const IMAGE_URI = "northwesternmti/nemarun:0.34"
const XCloudtasksQueuename = "nscalc"
const projectID = "andersen-lab"
const TIMEOUT = "86400s"
const SERVICE_ACCOUNT = "nscalc-201573431837@andersen-lab.iam.gserviceaccount.com"
const PUB_SUB_TOPIC = "projects/andersen-lab/topics/nemarun"
const MACHINE_TYPE = "n1-standard-1"
const REGION = "us-central1"
const SCOPE = "https://www.googleapis.com/auth/cloud-platform"

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
	Status      string         `datastore:"status"`
	Status_msg  string         `datastore:"status_msg,noindex"`
	Modified_on time.Time      `datastore:"modified_on"`
	Created_on  time.Time      `datastore:"created_on"`
	K           *datastore.Key `datastore:"__key__"`
}

// PubSubMessage is the payload of a Pub/Sub event.
// See the documentation for more details:
// https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage
type PubSubMessage struct {
	Message struct {
		Data []byte `json:"data,omitempty"`
		ID   string `json:"id"`
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

// helper function to update status in datastore
func setDatastoreStatus(i *dsInfo, status string, msg string) {
	ctx := context.Background()
	dsClient, err := datastore.NewClient(ctx, projectID)
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
	setDatastoreStatus(&info, "RUNNING", "")
	log.Printf("STARTED task: KIND: %s, ID: %s, HASH: %s", info.Kind, info.Id, info.Data_hash)

	// Start the nextflow pipeline

	// Log & output details of the task.
	setDatastoreStatus(&info, "COMPLETE", "")
	log.Printf("COMPLETED task: KIND: %s, ID: %s, HASH: %s", info.Kind, info.Id, info.Data_hash)

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

	name := string(m.Message.Data)
	if name == "" {
		name = "World"
	}
	log.Printf("Hello %s!", name)
}

func generateRunPipelineRequest(i *dsInfo) lifesciences.RunPipelineRequest {
	return lifesciences.RunPipelineRequest{
		Labels: map[string]string{},
		Pipeline: &lifesciences.Pipeline{
			Actions: []*lifesciences.Action{
				&lifesciences.Action{
					AlwaysRun:                   false,
					BlockExternalNetwork:        false,
					Commands:                    []string{fmt.Sprintf("nemarun.sh 'gs://elegansvariation.org/reports/nemascan/%s'", i.Data_hash)},
					ContainerName:               "nfhost",
					DisableImagePrefetch:        false,
					DisableStandardErrorCapture: false,
					EnableFuse:                  false,
					Environment:                 map[string]string{},
					IgnoreExitStatus:            false,
					ImageUri:                    IMAGE_URI,
					Labels:                      map[string]string{},
					PidNamespace:                "",
					PortMappings:                map[string]int64{},
					PublishExposedPorts:         false,
					RunInBackground:             false,
					Timeout:                     TIMEOUT,
					ForceSendFields:             []string{},
					NullFields:                  []string{},
				}},
			Environment: map[string]string{},
			Resources: &lifesciences.Resources{
				Regions: []string{REGION},
				VirtualMachine: &lifesciences.VirtualMachine{
					BootDiskSizeGb:              15,
					DockerCacheImages:           []string{},
					EnableStackdriverMonitoring: true,
					MachineType:                 MACHINE_TYPE,
					Preemptible:                 true,
					ServiceAccount: &lifesciences.ServiceAccount{
						Email:           SERVICE_ACCOUNT,
						Scopes:          []string{SCOPE},
						ForceSendFields: []string{},
						NullFields:      []string{},
					},
					ForceSendFields: []string{},
					NullFields:      []string{},
				},
				Zones:           []string{},
				ForceSendFields: []string{},
				NullFields:      []string{},
			},
			Timeout:         TIMEOUT,
			ForceSendFields: []string{},
			NullFields:      []string{},
		},
		PubSubTopic:     PUB_SUB_TOPIC,
		ForceSendFields: []string{},
		NullFields:      []string{},
	}
}

func executeRunPipelineRequest(i *dsInfo) lifesciences.Operation {
	ctx := context.Background()

	client, clientErr := google.DefaultClient(ctx, lifesciences.CloudPlatformScope)
	check(clientErr, i)

	lifesciencesService, glsServiceErr := lifesciences.NewService(ctx, option.WithHTTPClient(client))
	check(glsServiceErr, i)

	// The project and location that this request should be executed against.
	parent := "projects/201573431837/locations/us-central1"

	runPipelineRequest := generateRunPipelineRequest(i)

	pOperation := &lifesciences.Operation{
		Done:            false,
		Error:           &lifesciences.Status{},
		Metadata:        []byte{},
		Name:            XCloudtasksQueuename,
		Response:        []byte{},
		ServerResponse:  googleapi.ServerResponse{},
		ForceSendFields: []string{},
		NullFields:      []string{},
	}

	pOperation, pipelineRunErr := lifesciencesService.Projects.Locations.Pipelines.Run(parent, &runPipelineRequest).Context(ctx).Do()
	check(pipelineRunErr, i)

	log.Print(pOperation.Name)
	return *pOperation

}

func testHandler(w http.ResponseWriter, r *http.Request) {
	i := dsInfo{
		Kind:      "DEV_ns_calc",
		Id:        "123456",
		Msg:       "",
		Data_hash: "test",
	}
	executeRunPipelineRequest(&i)

	fmt.Fprint(w, "200 OK")
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

	http.HandleFunc("/testhandler", testHandler)
	http.HandleFunc("/ns", nsHandler)
	http.HandleFunc("/status", statusHandler)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("listening on %s", port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", port), nil))
}
