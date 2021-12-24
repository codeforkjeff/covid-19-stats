# Build an image for running pythons to download and preprocess flat files

# docker build . -t covid-19-stats-download

FROM ubuntu:20.04

RUN apt update && apt-get -y install git make python3 python3-pip python3-venv sqlite3 vim virtualenvwrapper

RUN git config --global user.email "jeff@codefork.com"
RUN git config --global user.name "codeforkjeff"

RUN mkdir /root/.dbt

RUN mkdir /root/covid-19-stats

# paths must exist as files otherwise -v option in "docker run" will create dirs
RUN touch /root/service-account.json

ADD requirements-load.txt /tmp

RUN pip3 install -r /tmp/requirements-load.txt

WORKDIR /root/covid-19-stats
