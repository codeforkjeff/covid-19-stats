# docker build . -t covid-19-stats-image

FROM ubuntu:20.04

ENV TZ=America/Los_Angeles
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apt-get update && apt-get -y install git cmake make python3 python3-pip python3-venv sqlite3 vim virtualenvwrapper

WORKDIR /root

RUN mkdir .dbt

RUN mkdir covid-19-stats

WORKDIR covid-19-stats

COPY requirements-dbt.txt .
COPY requirements-load.txt .
COPY create_envs.sh .

RUN /bin/bash ./create_envs.sh

COPY . .

WORKDIR /root/covid-19-stats

VOLUME /root/covid-19-stats/data

CMD /root/covid-19-stats/elt
