#!/bin/bash

# use the ssh keys on the host system

# to debug, add this line above the image name:
# -i -t --entrypoint /bin/bash

exec docker run \
     --rm \
     --name covid-19-stats-container \
     -v "$(pwd)":"/root/covid-19-stats" \
     -v covid-19-stats-volume:"/root/covid-19-stats/data" \
     -v "$HOME/.ssh":"/root/.ssh" \
     -v "$(pwd)/service-account.json":"/root/service-account.json" \
     -v "$(pwd)/profiles.yml":"/root/.dbt/profiles.yml" \
     covid-19-stats-image
