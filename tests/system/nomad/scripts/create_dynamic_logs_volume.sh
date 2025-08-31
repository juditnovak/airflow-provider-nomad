#!/bin/bash

INFILE=$1

if [[ -z "$INFILE" ]]; then
    echo "Usage: $0 <json-file>"
    exit 1
fi

PROTOCOL=http
HOST=localhost
PORT=4646
URL=${PROTOCOL}://${HOST}:${PORT}


printf "Creating dynamic host volume on ${HOST}\n"
ID=$(curl -s --request PUT --data @${INFILE} ${URL}/v1/volume/host/create \
	| jq  2> /dev/null \
	| grep '"ID":' \
	| cut -d':' -f2 \
	| sed -e 's/[," ]*//g')

if [[ -z "$ID" ]]; then
    echo "Failed to create dynamic host volume"
    exit 1
fi

printf "Created dynamic host volume ${ID}\n"

# The volume has to be rw for runner
HOSTPATH=$(curl -s --request GET ${URL}/v1/volume/host/$ID \
	| jq  2> /dev/null \
	| grep HostPath \
	| cut -d':' -f2 \
	| sed -e 's/[," ]*//g')


if [[ -z "$ID" ]]; then
    echo "Failed to change host volume permissions"
    exit 1
fi

printf "Chaning hostpath ${HOSTPATH} to RW\n"
sudo chmod 777 ${HOSTPATH}
