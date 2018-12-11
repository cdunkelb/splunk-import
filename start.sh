#!/bin/bash
docker run -dt --rm --name splunk -e "SPLUNK_START_ARGS=--accept-license --seed-passwd password" -e "SPLUNK_USER=root" -p "8000:8000" splunk/splunk
