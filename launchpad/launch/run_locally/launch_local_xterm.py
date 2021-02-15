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

"""Run commands to launch Launchpad workers in xterm."""

import atexit
import os
import subprocess

from launchpad.launch.run_locally import feature_testing


def launch_with_xterm(commands_to_launch):
  """Launch multiple commands given as CommandToLaunch tuples through xterm.

  Args:
    commands_to_launch: An iterable of `CommandToLaunch` namedtuples.
  """
  if not feature_testing.has_xterm():
    raise ValueError(
        'xterm is not available, please choose another way to launch.')
  processes = []
  for window_index, command_to_launch in enumerate(commands_to_launch):
    inner_cmd = '{}; exec $SHELL'.format(
        subprocess.list2cmdline(command_to_launch.command_as_list))

    xterm_command_list = [
        'xterm',
        '-title',
        command_to_launch.title,
        '-sl',
        '2000',
        '-geometry',
        '80x60+{}+{}'.format(window_index * 40, window_index * 40),
        '-e',
        inner_cmd,
    ]
    env = {}
    env.update(os.environ)
    env.update(command_to_launch.env_overrides)
    processes.append(subprocess.Popen(xterm_command_list, env=env))

  def kill_processes():
    for p in processes:
      p.kill()

  atexit.register(kill_processes)
