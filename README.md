# Nemascan Pipeline Runner

This CloudRun server handles requests to complete a Genetic Mapping using the NemaScan Nextflow pipeline with Google Life Sciences

Build using:

gcloud config set project andersen-lab

gcloud builds submit --config cloudbuild.yaml . --timeout=3h

gcloud beta run deploy \
--image gcr.io/andersen-lab/nscalc \
--platform managed \
--service-account nscalc-201573431837@andersen-lab.iam.gserviceaccount.com \
nscalc
