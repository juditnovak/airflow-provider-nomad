#!/bin/bash


help () {
    echo "Usage: $0 [-h] [-s] <json-file>"
    echo
    echo" -h    Display this message"
    echo" -s    Secure (use test certificates)"
}


# A POSIX variable
OPTIND=1                    # Reset in case getopts has been used previously in the shell.

# Initializing variables
PROTOCOL=http
HOST=localhost
PORT=4646

while getopts "sh:" opt; do
  case "$opt" in
    h)
      help
      exit 0
      ;;
    s)  PROTOCOL='https'
      ;;
  esac
done

shift $((OPTIND-1))
[ "${1:-}" = "--" ] && shift


INFILE=$1
if [[ -z "$INFILE" ]]; then
    help
    exit 1
fi


CURL_PARAM=""
if [ $PROTOCOL == "https" ]
then
    SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
    CERTDIR="${SCRIPT_DIR%/*}/certs"
    CURL_PARAM="--cert $CERTDIR/global-cli-nomad.pem --key $CERTDIR/global-cli-nomad-key.pem --cacert $CERTDIR/nomad-agent-ca.pem"
fi


URL=${PROTOCOL}://${HOST}:${PORT}

printf "Creating dynamic host volume on ${HOST}\n"
ID=$(curl $CURL_PARAM -s --request PUT --data @${INFILE} ${URL}/v1/volume/host/create \
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
HOSTPATH=$(curl $CURL_PARAM -s --request GET ${URL}/v1/volume/host/$ID \
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
