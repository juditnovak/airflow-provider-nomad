#! /bin/bash
#
# This file is part of apache-airflow-providers-nomad which is
# released under Apache License 2.0. See file LICENSE or go to
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# for full license details.
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
FILE=$SCRIPT_DIR/Dockerfile.runner
TAG=latest


help () {
    echo "Usage: $0 [-t <tag>] [-f <dockerfile>] <docker_image_name>"
    echo
    echo "NOTE: You must be authenticated with Docker Services"
    echo "(as we are about to push a Docker image to DockerHub)"
    echo
    echo "    -t    tag (default: $TAG)"
    echo "    -f    Dockerfile (default: $FILE)"
    echo

}

# Processing options

if [ -z $1 ] || [ "$1" = "-h" ] || [[ "$1" =~ "help" ]]
then
    help
    exit 1
fi

while getopts ":t:f:" o; do
    case "${o}" in
        t)
            TAG=${OPTARG}
            echo "-t"
            # shift
            ;;
        f)
            FILE=${OPTARG}
            echo "-f"
            # shift
            ;;
        *)
            echo "Unknown argument $OPTARG"
            usage
            exit 1
            ;;
    esac
done
shift $((OPTIND-1))

# Processing arguments

IMAGE=$1
shift

if [ $# -gt 0 ]
then
    help
    exit 1
fi


echo "Bulding $IMAGE:$TAG"


if ! docker login
then
    help
    echo "You must be authenticated with Docker Services"
    echo "(as we are about to push a Docker image to DockerHub)"
fi


build=$(docker build -f $FILE -t $IMAGE:$TAG . )
if ! $build
then
    echo "Build failed"
    exit 1

fi


# push=$(docker push $IMAGE:$TAG)
docker push $IMAGE:$TAG
if [ ! $? ]
then
    echo "Couldn't push image $IMAGE:$TAG to DockerHub"
    echo "(HINT: Soure you have access to the namespace?)"
    exit 1
fi
