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

function build_wheel() {
  TMPDIR="$1"
  DESTDIR="$2"
  RELEASE_FLAG="$3"

  # Before we leave the top-level directory, make sure we know how to
  # call python.
  if [[ -e python_bin_path.sh ]]; then
    echo $(date)  "Setting PYTHON_BIN_PATH equal to what was set with configure.py."
    source python_bin_path.sh
  fi
  PYTHON_BIN_PATH=${PYTHON_BIN_PATH:-$(which python3)}

  pushd ${TMPDIR} > /dev/null

  echo $(date) : "=== Building wheel"
  # "${PYTHON_BIN_PATH}" setup.py bdist_wheel ${PKG_NAME_FLAG} ${RELEASE_FLAG} --plat manylinux2014_x86_64 > /dev/null
  "${PYTHON_BIN_PATH}" setup.py bdist_wheel ${PKG_NAME_FLAG} ${RELEASE_FLAG} > /dev/null
  DEST=${TMPDIR}/dist/
  if [[ ! "$TMPDIR" -ef "$DESTDIR" ]]; then
    mkdir -p ${DESTDIR}
    cp dist/* ${DESTDIR}
    DEST=${DESTDIR}
  fi
  popd > /dev/null
  echo $(date) : "=== Output wheel file is in: ${DEST}"
}

function prepare_src() {
  TMPDIR="${1%/}"
  mkdir -p "$TMPDIR"

  echo $(date) : "=== Preparing sources in dir: ${TMPDIR}"

  if [ ! -d bazel-bin/courier ]; then
    echo "Could not find bazel-bin.  Did you run from the root of the build tree?"
    exit 1
  fi

  cp LICENSE ${TMPDIR}

  # Copy all Python files, requirements and so libraries.
  cd bazel-bin && find . -name "*.py*" -exec rsync -R {} ${TMPDIR} \; && cd ..
  cd launchpad && find . -name "*.py*" -exec rsync -R {} ${TMPDIR}/launchpad \; && cd ..
  cd courier && find . -name "*.py*" -exec rsync -R {} ${TMPDIR}/courier \; && cd ..
  cd reverb && find . -name "*.py*" -exec rsync -R {} ${TMPDIR}/reverb \; && cd ..
  find . -name requirements.txt -exec cp -f {} ${TMPDIR} \;
  cp launchpad/launch/run_locally/decorate_output ${TMPDIR}/launchpad/launch/run_locally/decorate_output

  cd bazel-bin/courier && find . -name "*.so" -exec rsync -R {} ${TMPDIR}/courier \; && cd ../..
  cd bazel-bin/courier && find . -name "*.dylib" -exec rsync -R {} ${TMPDIR}/courier \; && cd ../..
  mv ${TMPDIR}/courier/serialization/*.so ${TMPDIR}/courier/python/

  mv ${TMPDIR}/launchpad/pip_package/setup.py ${TMPDIR}
  cp launchpad/pip_package/MANIFEST.in ${TMPDIR}
  mkdir ${TMPDIR}/version/
  mv ${TMPDIR}/launchpad/pip_package/launchpad_version.py ${TMPDIR}/version/


  # Copies README.md to temp dir so setup.py can use it as the long description.
  cp README.md ${TMPDIR}
}

function usage() {
  echo "Usage:"
  echo "$0 [options]"
  echo "  Options:"
  echo "    --release         build a release version"
  echo "    --dst             path to copy the .whl into."
  echo "    --tf_package      tensorflow version dependency passed to setup.py."
  echo "    --reverb_package  reverb version dependency passed to setup.py."
  echo ""
  exit 1
}

function main() {
  RELEASE_FLAG=""
  # This is where the source code is copied and where the whl will be built.
  DST_DIR=""

  while true; do
    if [[ "$1" == "--help" ]]; then
      usage
      exit 1
    elif [[ "$1" == "--release" ]]; then
      RELEASE_FLAG="--release"
    elif [[ "$1" == "--dst" ]]; then
      shift
      DST_DIR=$1
    fi

    if [[ -z "$1" ]]; then
      break
    fi
    shift
  done

  TMPDIR="$(mktemp -d -t tmp.XXXXXXXXXX)"
  if [[ -z "$DST_DIR" ]]; then
    DST_DIR=${TMPDIR}
  fi

  prepare_src "$TMPDIR"
  build_wheel "$TMPDIR" "$DST_DIR" "$RELEASE_FLAG"
}

main "$@"
