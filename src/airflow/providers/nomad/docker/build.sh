#! /bin/bash


help () {
    echo "Usage: $0 <docker_image> [<tag>]"
    echo "You must be authenticated with Docker Services"
    echo "(as we are about to push a Docker image to DockerHub)"


}


if [ -z $1 ]
then
    help
fi

IMAGE=$1
TAG=${$2:~latest}

echo "Bulding $IMAGE:$TAG"

# Preparing pachage
# uv build

if ! docker login
then
    help
    echo "You must be authenticated with Docker Services"
    echo "(as we are about to push a Docker image to DockerHub)"
fi

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

build=$(docker build -f $SCRIPT_DIR/Dockerfile.runner -t novakjudit/af_nomad_test:latest . )
if ! $build
then
    echo "Build failed"
    exit 1

fi


push=$(docker push novakjudit/af_nomad_test:latest)
if ! $push
then
    echo "Couldn't push image $IMAGE:$TAG to DockerHub"
    echo "(HINT: Soure you have access to the namespace?)"
    exit 1
fi
