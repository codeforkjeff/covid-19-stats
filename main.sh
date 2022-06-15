#!/bin/bash

# main script to run in container

# this is supposed to happen automatically in ubuntu 20.04
# if bash-completion is installed, but it desn't, so we source it here.
source /usr/share/virtualenvwrapper/virtualenvwrapper.sh

echo "==== Running at `date`"

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";

echo Using project directory: $SCRIPT_DIR

# do the ELT

echo "==== Running elt.sh"

. ./elt.sh

echo "==== Finished at `date`"
