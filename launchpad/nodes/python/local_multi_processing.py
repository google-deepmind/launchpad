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

"""Utilities to run PyNodes as multiple processes."""

import atexit
import dataclasses
import json
import os
import shutil
import sys
import tempfile
from typing import Any, List, Mapping, Optional, Sequence, Union

from absl import flags
from absl import logging
from launchpad import flags as lp_flags  
from launchpad.launch import serialization
from launchpad.launch.local_multi_processing import commands as mp_commands
import portpicker

_INTERPRETER = sys.executable

StrOrFloat = Union[str, float]


def _to_cmd_arg(key: str, value: Any) -> str:
  """Converts key value pair to "--key=value"."""
  if isinstance(value, bool):
    return f'--{key}' if value else f'--no{key}'
  return f'--{key}={value}'




@dataclasses.dataclass
class PythonProcess:
  """Local multiprocessing launch configuration for a PyNode.

  Attributes:
    args: Arguments to pass to the user script.
    env: Additional environment variables to set.
  """

  args: Mapping[str, StrOrFloat] = dataclasses.field(default_factory=dict)
  env: Mapping[str, StrOrFloat] = dataclasses.field(default_factory=dict)

  _absolute_interpreter_path: str = ''

  def _get_absolute_interpreter_path(self):
    """Resolve self.interpreter to an absolute path."""
    return _INTERPRETER

  @property
  def absolute_interpreter_path(self) -> str:
    """Returns the absolute path to the interpreter binary."""
    if not self._absolute_interpreter_path:
      self._absolute_interpreter_path = self._get_absolute_interpreter_path()
    return self._absolute_interpreter_path


_DATA_FILE_NAME = 'job.pkl'


def to_multiprocessing_executables(
    nodes: Sequence[Any],
    label: str,
    launch_config: PythonProcess,
    pdb_post_mortem: bool,
    ) -> List[mp_commands.Command]:
  """Returns a list of `Command`s objects for the given `PyNode`s."""
  launch_config = launch_config or PythonProcess()
  if not isinstance(launch_config, PythonProcess):
    raise ValueError(
        'Launch config for {} must be a PythonProcess.'.format(label))


  entry_script_path = os.path.join(os.path.dirname(__file__),
                                   'process_entry.py')

  tmp_dir = tempfile.mkdtemp()
  atexit.register(shutil.rmtree, tmp_dir, ignore_errors=True)
  data_file_path = os.path.join(tmp_dir, _DATA_FILE_NAME)
  serialization.serialize_functions(data_file_path, label,
                                    [n.function for n in nodes])


  args = dict(launch_config.args)
  per_task_args = [{} for _ in nodes]
  per_task_interpreter_args = [{} for _ in nodes]
  per_task_env = [{} for _ in nodes]

  commands = []
  for task_id, (_, task_args) in enumerate(zip(nodes, per_task_args)):
    command_as_list = [
        launch_config.absolute_interpreter_path, entry_script_path
    ]


    all_args = {**args, **task_args}
    # Arguments to pass to the script
    for key, value in all_args.items():
      command_as_list.append(_to_cmd_arg(key, value))

    command_as_list.extend([
        '--data_file', data_file_path,
        '--lp_task_id', str(task_id),
    ])
    if pdb_post_mortem:
      command_as_list.append('--pdb_post_mortem')
    env = {**launch_config.env}
    env.update(per_task_env[task_id])
    command = mp_commands.Command(command_as_list, env,
                                  label + '/' + str(task_id))
    commands.append(command)
  return commands
