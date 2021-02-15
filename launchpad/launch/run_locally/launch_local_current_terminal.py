# Lint as: python3
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

import psutil

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
  """
  processes = []
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
    processes.append(process)

  def kill_processes():
    for p in processes:
      parent = psutil.Process(p.pid)
      children = parent.children(recursive=True)
      for child in children:
        try:
          child.send_signal(9)
        except psutil.NoSuchProcess:
          pass
      p.send_signal(9)

  atexit.register(kill_processes)
