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

"""Run commands to launch Launchpad workers in current terminal."""

import atexit
import os
import subprocess

from launchpad import flags as lp_flags

from launchpad.launch import worker_manager
from launchpad.launch import worker_manager_v2

_COLOUR_PALETTE = [
    36,  # Blue
    33,  # Yellow
    32,  # Green
    34,  # Purple
    35,  # Red
]


def launch_in_current_terminal(commands_to_launch):
  """Launch commands given as CommandToLaunch all in the same terminal.

  Args:
    commands_to_launch: An iterable of `CommandToLaunch` namedtuples.

  Returns:
    Worker manager that can be used to wait for a program execution to finish.
  """
  if lp_flags.LP_WORKER_MANAGER_V2.value:
    manager = worker_manager_v2.WorkerManager(
        handle_sigterm=True, kill_all_upon_sigint=True)
  else:
    manager = worker_manager.WorkerManager()
  atexit.register(manager.wait)
  decorate_output = os.path.dirname(__file__) + '/decorate_output'

  for i, command_to_launch in enumerate(commands_to_launch):
    colour = _COLOUR_PALETTE[i % len(_COLOUR_PALETTE)]

    env = {}
    env.update(os.environ)
    env.update(command_to_launch.env_overrides)
    process = subprocess.Popen(
        ([decorate_output, str(colour), command_to_launch.title] +
         command_to_launch.command_as_list),
        env=env)
    manager.register_existing_process(command_to_launch.title, process.pid)
  return manager
