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

# Designed to work with ./docker/build.dockerfile to build Launchpad.

# Exit if any process returns non-zero status.
set -e
set -o pipefail
cd "$(dirname "$0")"

# Flags
PYTHON_VERSIONS=3.8 # Options 3.7, 3.8, 3.9, 3.10
CLEAN=false # Set to true to run bazel clean.
# Also see https://github.com/deepmind/reverb/commit/4da8a918794b65aff4afd539e89e0ef6c35c3f35
CLEAR_CACHE=false # Set to true to delete Bazel cache folder. b/279235134
OUTPUT_DIR=/tmp/launchpad/dist/
INSTALL=true # Should the built package be installed.

PIP_PKG_EXTRA_ARGS="" # Extra args passed to `build_pip_package`.

while [[ $# -gt -0 ]]; do
  key="$1"
  case $key in
      --release)
      PIP_PKG_EXTRA_ARGS="${PIP_PKG_EXTRA_ARGS} --release" # Indicates this is a release build.
      ;;
      --python)
      PYTHON_VERSIONS="$2" # Python versions to build against.
      shift
      ;;
      --clean)
      CLEAN="$2" # `true` to run bazel clean. False otherwise.
      shift
      ;;
      --clear_bazel_cache)
      CLEAR_CACHE="$2"
      shift
      ;;
      --install)
      INSTALL="$2" # `true` to install built package. False otherwise.
      shift
      ;;
      --output_dir)
      OUTPUT_DIR="$2"
      shift
      ;;
    *)
      echo "Unknown flag: $key"
      echo "Usage:"
      echo "--release [Indicates this is a release build. Otherwise nightly.]"
      echo "--python  [3.7|3.8(default)|3.9]"
      echo "--clean   [true to run bazel clean]"
      echo "--clear_bazel_cache [true to delete Bazel cache folder]"
      echo "--install [true to install built package]"
      echo "--output_dir  [location to copy .whl file.]"
      exit 1
      ;;
  esac
  shift # past argument or value
done

for python_version in $PYTHON_VERSIONS; do

  # Cleans the environment.
  if [ "$CLEAN" = "true" ]; then
    if [ "$CLEAR_CACHE" = "true" ]; then
      rm -rf $HOME/.cache/bazel
    fi
    bazel clean
  fi

  if [ "$python_version" = "3.7" ]; then
    export PYTHON_BIN_PATH=/usr/bin/python3.7 && export PYTHON_LIB_PATH=/usr/local/lib/python3.7/dist-packages
    ABI=cp37
  elif [ "$python_version" = "3.8" ]; then
    export PYTHON_BIN_PATH=/usr/bin/python3.8 && export PYTHON_LIB_PATH=/usr/local/lib/python3.8/dist-packages
    ABI=cp38
  elif [ "$python_version" = "3.9" ]; then
    export PYTHON_BIN_PATH=/usr/bin/python3.9 && export PYTHON_LIB_PATH=/usr/local/lib/python3.9/dist-packages
    ABI=cp39
  elif [ "$python_version" = "3.10" ]; then
    export PYTHON_BIN_PATH=/usr/bin/python3.10 && export PYTHON_LIB_PATH=/usr/local/lib/python3.10/dist-packages
    ABI=cp310
  else
    echo "Error unknown --python. Only [3.7|3.8|3.9|3.10]"
    exit 1
  fi

  # Configures Bazel environment for selected Python version.
  $PYTHON_BIN_PATH configure.py

  # Build Launchpad and run all bazel Python tests. All other tests are executed
  # later.
  bazel build -c opt --copt=-mavx --config=manylinux2014 --test_output=errors //...

  # Builds Launchpad and creates the wheel package.
  /tmp/launchpad/launchpad/pip_package/build_pip_package.sh --dst $OUTPUT_DIR/fresh $PIP_PKG_EXTRA_ARGS

  # Install built package.
  if [ "$INSTALL" = "true" ]; then
    $PYTHON_BIN_PATH -mpip install --upgrade $OUTPUT_DIR/fresh/*
  fi

  chmod 666 $OUTPUT_DIR/fresh/*
  mv $OUTPUT_DIR/fresh/* $OUTPUT_DIR/
  rm -r $OUTPUT_DIR/fresh

done
