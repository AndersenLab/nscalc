
##############################################################################
#                         build the builder image                           #
##############################################################################
# https://hub.docker.com/_/golang Use alpine to match nextflow docker image
FROM golang:alpine as builder

# Create and change to the app directory.
WORKDIR /app

# Retrieve application dependencies.
# This allows the container build to reuse cached dependencies.
# Expecting to copy go.mod and if present go.sum.
COPY main.go ./
COPY go.* ./
RUN go mod download

# Build the binary.
RUN go build -v -o server


##############################################################################
#                         build the deployed image                           #
##############################################################################
FROM gcr.io/google.com/cloudsdktool/cloud-sdk:alpine
RUN gcloud components install beta -q

LABEL Name="nscalc" Author="Sam Wachspress"

# Copy local code to the container image.
COPY --from=builder /app/server /nemascan/server

ENV PATH="/nemascan:${PATH}"

# Set start process
CMD ["/nemascan/server"]
