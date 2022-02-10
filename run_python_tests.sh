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

# Executes Launchpad Python tests.
set +x
set -e

# Flags
PYTHON=3.8

while [[ $# -gt -0 ]]; do
  key="$1"
  case $key in
      --python)
      PYTHON="$2"
      shift
      ;;
    *)
      echo "Unknown flag: $key"
      echo "Usage:"
      echo "--python  [3.7|3.8(default)|3.9|3.10]"
      exit 1
      ;;
  esac
  shift # past argument or value
done


LAUNCHPAD=`python${PYTHON} -c 'import launchpad as lp; print(lp.__path__[0])'`
COURIER=`python${PYTHON} -c 'import courier; print(courier.__path__[0])'`

py_test() {
  echo "===========Running Python tests============"

  for test_file in `find $LAUNCHPAD $COURIER -name '*_test.py' -print`
  do
    echo "####=======Testing ${test_file}=======####"
    date
    time python${PYTHON} "${test_file}"
  done
}
test_terminal () {
  date
  LAUNCHPAD_LAUNCH_LOCAL_TERMINAL=$@ python${PYTHON} -m launchpad.examples.consumer_producers.launch --lp_launch_type=local_mp
}

N_CPU=$(grep -c ^processor /proc/cpuinfo)

# Run static type-checking.
# TODO(b/205923232): enable PyType once they address the typed_ast problem.
if [[ ${PYTHON} == "3.8" ]]
then
  date
  pytype -k -x /tmp/launchpad/launchpad/pip_package/ \
/tmp/launchpad/launchpad/examples/consumer_producers/launch_test.py \
/tmp/launchpad/configure.py -j "${N_CPU}" /tmp/launchpad/
  date
fi

# Run all tests.
py_test

# Test different terminals.
test_terminal tmux_session
test_terminal byobu_session
test_terminal current_terminal
test_terminal output_to_files
date
python${PYTHON} -m launchpad.examples.consumer_producers.launch --lp_launch_type=local_mt
date
