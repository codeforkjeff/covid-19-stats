# docker build . -t covid-19-stats-image

FROM ubuntu:20.04

ENV TZ=America/Los_Angeles
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apt-get update && apt-get -y install git cmake make python3 python3-pip python3-venv sqlite3 vim virtualenvwrapper

RUN git config --global user.email "jeff@codefork.com"
RUN git config --global user.name "codeforkjeff"

ENV WORKON_HOME /root/.virtualenvs

WORKDIR /root

RUN mkdir /root/.dbt

VOLUME /root/covid-19-stats

# paths must exist as files otherwise -v option in "docker run" will create dirs
RUN touch /root/.dbt/profiles.yml
RUN touch /root/service-account.json

ENTRYPOINT main.sh
