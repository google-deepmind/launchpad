#!/bin/bash
# Copyright 2020 DeepMind Technologies Limited. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

# Flags
DEBUG_DOCKER=true
CLEAN=false
RELEASE=false
PYTHON=3.9
TF_PACKAGE=tf-nightly
REVERB_PACKAGE=dm-reverb-nightly

while [[ $# -gt -0 ]]; do
  key="$1"
  case $key in
      --debug_docker)
      DEBUG_DOCKER="$2"
      shift
      ;;
      --python)
      PYTHON="$2"
      shift
      ;;
      --clean)
      CLEAN="$2"
      shift
      ;;
      --release)
      RELEASE="$2"
      shift
      ;;
      --tf_package)
      TF_PACKAGE="$2"
      shift
      ;;
      --reverb_package)
      REVERB_PACKAGE="$2"
      shift
      ;;
    *)
      echo "Unknown flag: $key"
      echo "Usage:"
      echo "--debug_docker [Enter the Docker image upon failure for debugging.]"
      echo "--python  [3.7|3.8|3.9(default)]"
      echo "--clean   [true to run bazel clean]"
      echo "--release [true to build a release binary]"
      exit 1
      ;;
  esac
  shift # past argument or value
done

MOUNT_CMD="--mount type=bind,src=${PWD},dst=/tmp/launchpad"

run_docker() {
  set +e
  set -x
  "$@"
  if [[ $? != 0 ]]; then
    if [[ $DEBUG_DOCKER ]]; then
      IMAGE=`docker container ls --last 1 | tail -n 1 | sed -r 's/.* ([a-z0-9]{12}).*/\1/'`
      echo "Launchpad build failed, entering Docker image $IMAGE for debugging."
      docker run --rm ${MOUNT_CMD} \
        -it $IMAGE bash
    fi
    exit 1
  fi
  set +x
  set -e
}

RELEASE_FLAG=''
if [[ $RELEASE == 'true' ]]; then
  RELEASE_FLAG='--release'
fi

run_docker docker build --tag launchpad:build \
  --build-arg python_version="${PYTHON}" \
  --build-arg tensorflow_pip="${TF_PACKAGE}" \
  -f "docker/build.dockerfile" .

run_docker docker run --rm ${MOUNT_CMD} \
  launchpad:build /tmp/launchpad/oss_build.sh --python "${PYTHON}" \
  --clean ${CLEAN} --install false --tf_package $TF_PACKAGE \
  --reverb_package $REVERB_PACKAGE $RELEASE_FLAG

for python_version in $PYTHON; do

  # Install Launchpad in a fresh Docker image for testing.
  abi=$(echo cp${python_version} | sed 's/\.//')
  base_image="python:$python_version"
  run_docker docker build --tag launchpad:test \
    --build-arg python_version=$python_version \
    --build-arg abi=$abi \
    --build-arg base_image=$base_image \
    -f "docker/test.dockerfile" .

  # Run tests.
  run_docker docker run -e DISPLAY=$DISPLAY ${MOUNT_CMD} -v /tmp/.X11-unix:/tmp/.X11-unix:rw \
    --rm launchpad:test bash /tmp/launchpad/run_python_tests.sh --python ${python_version}

done

echo "All tests passed! Built wheel(s):"
ls -la dist/
echo "To enter docker image used for building, execute:"
echo "> docker run ${MOUNT_CMD} --entrypoint bash --rm -it launchpad:build"
echo "From there you can for instance 'cd /tmp/launchpad && bazel build ...'"

echo "To enter docker image used for testing, execute:"
echo "> docker run ${MOUNT_CMD} --entrypoint bash --rm -it launchpad:test"
echo "From there you can for instance 'python3 -m launchpad.examples.consumer_producers.launch --lp_launch_type=local_mt'"
