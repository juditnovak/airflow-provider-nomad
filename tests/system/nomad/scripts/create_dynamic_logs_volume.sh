#!/bin/bash

INFILE=$1

if [[ -z "$INFILE" ]]; then
    echo "Usage: $0 <json-file>"
fi

PROTOCOL=http
HOST=localhost
PORT=4646
URL=${PROTOCOL}://${HOST}:${PORT}


echo "Creating dynamic host volume on ${HOST}"
ID=$(curl -s --request PUT --data @${INFILE} ${URL}/v1/volume/host/create \
	| jq \
	| grep '"ID":' \
	| cut -d':' -f2 \
	| sed -e 's/[," ]*//g')

if [[ -z "$ID" ]]; then
    echo "Failed to create dynamic host volume"
    exit 1
fi

echo "Created dynamic host volume ${ID}"

# The volume has to be rw for runner
HOSTPATH=$(curl -s --request GET ${URL}/v1/volume/host/$ID \
	| jq \
	| grep HostPath \
	| cut -d':' -f2 \
	| sed -e 's/[," ]*//g')


if [[ -z "$ID" ]]; then
    echo "Failed to change host volume permissions"
    exit 1
fi

echo "Chaning hostpath ${HOSTPATH} to RW"
sudo chmod 777 ${HOSTPATH}
