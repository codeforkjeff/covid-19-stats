#!/bin/bash

# main script to run in container

# this is supposed to happen automatically in ubuntu 20.04
# if bash-completion is installed, but it desn't, so we source it here.
source /usr/share/virtualenvwrapper/virtualenvwrapper.sh

echo "==== Running at `date`"

if [ -z "$PROJECT_DIR" ]; then
    PROJECT_DIR=~
fi

echo Using project directory: $PROJECT_DIR

## clone or update

cd $PROJECT_DIR

if [ ! -d "covid-19-stats" ]; then
    git clone git@github.com:codeforkjeff/covid-19-stats.git
fi

## clone or update

cd $PROJECT_DIR

if [ ! -d "COVID-19" ]; then
    git clone git@github.com:CSSEGISandData/COVID-19.git
fi

## make sure repo is up to date

cd $PROJECT_DIR/covid-19-stats
git checkout master
git pull

# setup environments / make sure they're up to date

echo "==== Running setup_envs.sh"

. ./setup_envs.sh

# do the ELT

echo "==== Running elt.sh"

. ./elt.sh

echo "==== Finished at `date`"
