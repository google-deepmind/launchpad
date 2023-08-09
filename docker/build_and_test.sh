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
CLEAN=false
CLEAR_CACHE=false
RELEASE=false
PYTHON=3.10

while [[ $# -gt -0 ]]; do
  key="$1"
  case $key in
      --python)
      PYTHON="$2"
      shift
      ;;
      --clean)
      CLEAN="$2"
      shift
      ;;
      --clear_bazel_cache)
      CLEAR_CACHE="$2"
      shift
      ;;
      --release)
      RELEASE="$2"
      shift
      ;;
    *)
      echo "Unknown flag: $key"
      echo "Usage:"
      echo "--python  [3.7|3.8|3.9|3.10(default)]"
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
    exit 1
  fi
  set +x
  set -e
}

RELEASE_FLAG=''
pushd launchpad/pip_package
if [[ $RELEASE == 'true' ]]; then
  RELEASE_FLAG='--release'
  TF_PACKAGE=`python3 -c 'import launchpad_version as v; print(v.__tensorflow_version__)'`
else
  TF_PACKAGE=`python3 -c 'import launchpad_version as v; print(v.__nightly_tensorflow_version__)'`
fi
popd

PYTHON_CMDS=$(echo ${PYTHON} | sed 's/[^ ]* */python&/g')

run_docker docker build --tag launchpad:build \
  --build-arg python_version="${PYTHON_CMDS}" \
  --build-arg tensorflow_pip="${TF_PACKAGE}" \
  -f "docker/build.dockerfile" .

run_docker docker run --rm ${MOUNT_CMD} \
  launchpad:build /tmp/launchpad/oss_build.sh --python "${PYTHON}" \
  --clean ${CLEAN} --clear_bazel_cache ${CLEAR_CACHE} --install false $RELEASE_FLAG

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
