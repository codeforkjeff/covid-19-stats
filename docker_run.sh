#!/bin/bash

docker run -it --rm --name my-running-script -v "$PWD":/usr/src/myapp -v "$HOME"/COVID-19:/root/COVID-19 -w /usr/src/myapp python:3.7.7-buster python load_csse_data.py
