#!/bin/bash

# use the ssh keys on the host system
# -i -t --entrypoint /bin/bash \

docker run \
     --rm \
     --name covid-19-stats-download-container \
     -v "$HOME/.ssh":"/root/.ssh" \
     -v "$(pwd)/service-account.json":"/root/service-account.json" \
     -v "$(pwd)":"/root/covid-19-stats" \
     -v covid-19-stats-stage:"/root/covid-19-stats/stage" \
     covid-19-stats-download \
     $@
