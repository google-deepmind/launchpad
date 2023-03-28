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

"""Entry of a PythonNode worker."""


import contextlib
import functools
import json
import os
import sys

from absl import app
from absl import flags
from absl import logging
import cloudpickle
from launchpad import flags as lp_flags
from launchpad.launch import worker_manager
from launchpad.launch import worker_manager_v2
import six


FLAGS = flags.FLAGS

flags.DEFINE_integer(
    'lp_task_id', None, 'a list index deciding which '
    'worker to run. given a list of workers (obtained from the'
    ' data_file)')
flags.DEFINE_string('data_file', '',
                    'Pickle file location with entry points for all nodes')
# An index of an entry point in a pickle file. Will not match work unit id if
# entry points are saved to separate pickle files.
flags.DEFINE_integer('lp_unit_id', None,
                     'Which work unit within pickle file to run.')
flags.DEFINE_string(
    'lp_job_name', '',
    'The name of the job, used to access the correct pickle file resource when '
    'using the new launch API')
flags.DEFINE_string(
    'init_file', '', 'Pickle file location containing initialization module '
    'executed for each node prior to an entry point')
flags.DEFINE_string('flags_to_populate', '{}', 'obsolete')

_FLAG_TYPE_MAPPING = {
    str: flags.DEFINE_string,
    six.text_type: flags.DEFINE_string,
    float: flags.DEFINE_float,
    int: flags.DEFINE_integer,
    bool: flags.DEFINE_boolean,
    list: flags.DEFINE_list,
}


def _parse_process_entry_flags(all_argv: list[str]) -> list[str]:
  """Parse and consume all flags for the entry script; return the rest."""
  # unconsumed_argv will still include all_argv[0], which is expected to be
  # the program name and is ignored by flag parsing.
  unconsumed_argv = FLAGS(all_argv, known_only=True)


  # JAX doesn't use absl flags and so we need to forward absl flags to JAX
  # explicitly. Here's a heuristic to detect JAX flags and forward them.
  if any(arg.startswith('--jax_') for arg in sys.argv):
    try:
      # pytype:disable=import-error
      import jax  
      # pytype:enable=import-error
      jax.config.parse_flags_with_absl()
    except ImportError:
      pass

  return unconsumed_argv


def _get_task_id():
  """Returns current task's id."""
  if FLAGS.lp_task_id is None:
    # Running under Vertex AI...
    cluster_spec = os.environ.get('CLUSTER_SPEC', None)
    return json.loads(cluster_spec).get('task').get('index')

  return FLAGS.lp_task_id


def main(argv: list[str], process_argv: list[str]):
  # See `parse_flags_and_run()` for why arguments are passed in `process_argv`
  # instead.
  assert len(argv) == 1
  del argv

  # Allow for importing modules from the current directory.
  sys.path.append(os.getcwd())
  data_file = FLAGS.data_file
  init_file = FLAGS.init_file

  if os.environ.get('TF_CONFIG', None):
    # For GCP runtime log to STDOUT so that logs are not reported as errors.
    logging.get_absl_handler().python_handler.stream = sys.stdout

  if init_file:
    init_function = cloudpickle.load(open(init_file, 'rb'))
    init_function()
  functions = cloudpickle.load(open(data_file, 'rb'))

  # Now that the code that we intend to run has been unpickled, that should
  # have caused the registration of any remaining flags that the program needs.
  [unused_program_name, *unconsumed_argv] = FLAGS(process_argv, known_only=True)
  if unconsumed_argv:
    logging.warning('The following command-line arguments were passed to the '
                    'program but are not used by anything that it imports: %s',
                    unconsumed_argv)

  task_id = _get_task_id()

  if lp_flags.LP_WORKER_MANAGER_V2.value:
    worker_manager_v2.WorkerManager(
        handle_sigterm=True,
        register_in_thread=True)
  else:
    # Worker manager is used here to handle termination signals and provide
    # preemption support.
    worker_manager.WorkerManager(
        register_in_thread=True)

  with contextlib.suppress():  # no-op context manager
    functions[task_id]()


def parse_flags_and_run():
  # Parse flags for this module and the things it has already imported.
  # Pass whatever flags are left over to main() through a side channel, so that
  # app.run() doesn't try to parse them before we have set the scene.
  [program_name, *process_argv] = _parse_process_entry_flags(sys.argv)
  app.run(functools.partial(main, process_argv=[program_name, *process_argv]),
          argv=[program_name])

if __name__ == '__main__':
  parse_flags_and_run()
