#! /bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
CERTDIR="${SCRIPT_DIR%/*}/certs"

echo "Generating CA certficates..."


CMD1=1
CMD2=1

openssl genrsa 2048 > $CERTDIR/nomad-agent-ca-key.pem
CMD1=$?
openssl req -new -x509 -config $CERTDIR/openssl.cnf -days 7300   -out $CERTDIR/nomad-agent-ca.pem -key $CERTDIR/nomad-agent-ca-key.pem -subj '/O=My Company Name LTD./C=EU'
CMD2=$?

if [ $CMD1 -eq 0 ] && [ $CMD2 -eq 0 ]
then
  echo "CA certificates generated successfully"
else
  echo "Failed to generate CA cerificates. Exiting..."
  exit 1
fi

echo "Generating Nomad client certficates..."

CURDIR=$PWD

cd $CERTDIR
nomad tls cert create -client
CMD1=$?
nomad tls cert create -cli
CMD2=$?
cd $CURDIR

if [ $CMD1 -eq 0 ] && [ $CMD2 -eq 0 ]
then
  echo "CA certificates generated successfully"
else
  echo "Failed to generate CA cerificates. Exiting..."
  exit 1
fi
