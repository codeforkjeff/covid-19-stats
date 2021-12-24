#!/bin/bash

# docker pull meltano/meltano:v1.90.1

docker run \
    -v $(pwd):/project \
    -v "$HOME/bigquery/service-account.json":"/service-account.json" \
    -e "GOOGLE_APPLICATION_CREDENTIALS=/service-account.json" \
    -w /project \
    meltano/meltano:v1.90.1 \
    $@
