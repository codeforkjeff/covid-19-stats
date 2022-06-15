#!/bin/bash

# use the ssh keys on the host system

# to debug, add this line above the image name:
# -i -t --entrypoint /bin/bash

exec docker run \
     --rm \
     --name covid-19-stats-container \
     -e "BUCKET_SOURCES=codeforkjeff-covid-19-sources" \
     -e "BUCKET_PUBLIC=codeforkjeff-covid-19-public" \
     -v covid-19-stats-volume:"/root/covid-19-stats/data" \
     -v "$(pwd)/service-account.json":"/root/service-account.json" \
     -v "$(pwd)/profiles.yml":"/root/.dbt/profiles.yml" \
     covid-19-stats-image
