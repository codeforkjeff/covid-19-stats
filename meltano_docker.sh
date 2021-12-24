#!/bin/bash

# docker pull meltano/meltano:v1.90.1

docker run \
    --rm \
    -v $(pwd):/project \
    -v covid-19-stats-meltano-data:/project/.meltano \
    -v covid-19-stats-stage:"/project/stage" \
    -v "$(pwd)/service-account.json":"/service-account.json" \
    -e "GOOGLE_APPLICATION_CREDENTIALS=/service-account.json" \
    -w /project \
    meltano/meltano:v1.90.1 \
    $@
