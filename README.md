# This directory contains the code to start the Google Cloud Run Microservice for the Heritability Tool

Build using:

gcloud config set project andersen-lab

gcloud builds submit --config cloudbuild.yaml . --timeout=3h

gcloud beta run deploy \
--image gcr.io/andersen-lab/nscalc \
--platform managed \
--service-account=nscalc-201573431837@andersen-lab.iam.gserviceaccount.com \
nscalc




gcloud beta lifesciences pipelines run --command-line \
"/nemarun/nemarun.sh gs://elegansvariation.org/reports/nemascan/d37a6511ac5170d3bf7d31d17ba872e0 1b9a8002850a28a1a1f9ffbfc86712985dd02ae8" \
--logging=gs://elegansvariation.org/reports/nemascan/d37a6511ac5170d3bf7d31d17ba872e0/gls.log \
--location=us-central1 \
--regions=us-central1 \
--docker-image=northwesternmti/nemarun:0.33 \
--preemptible \
--machine-type=n1-standard-1 \
--service-account-email=nscalc-201573431837@andersen-lab.iam.gserviceaccount.com



docker run -d northwesternmti/nemarun:0.33  /bin/bash --restart=unless-stopped --name nemarun


gcloud beta lifesciences pipelines run 

gcloud beta lifesciences pipelines run \
    --logging=gs://elegansvariation.org/reports/nemascan/{DATA_HASH}/gls.log \
    --pipeline-file {DATA_HASH}.json