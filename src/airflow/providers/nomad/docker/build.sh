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


help () {
    echo "Usage: $0 <docker_image_name> [<tag>]"
    echo
    echo "NOTE: You must be authenticated with Docker Services"
    echo "(as we are about to push a Docker image to DockerHub)"


}


if [ -z $1 ]
then
    help
    exit 1
fi

IMAGE=$1

TAG=latest
if [ $2 ]
then
    TAG=$2
fi

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

build=$(docker build -f $SCRIPT_DIR/Dockerfile.runner -t $IMAGE:$TAG . )
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
