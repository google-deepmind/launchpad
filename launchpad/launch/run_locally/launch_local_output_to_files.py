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

"""Run commands to launch Launchpad workers output their logging to files."""

import os
import subprocess


_LOGGING_DIR = '/tmp/launchpad_out/'


def launch_and_output_to_files(commands_to_launch):
  """Launch commands given as CommandToLaunch and log the outputs to files.

  Args:
    commands_to_launch: An iterable of `CommandToLaunch` namedtuples.
  """
  titles = []
  for command_to_launch in commands_to_launch:
    env = {}
    env.update(os.environ)
    env.update(command_to_launch.env_overrides)
    title = command_to_launch.title
    count = 0
    while title in titles:
      count += 1
      title = command_to_launch.title + '_' + str(count)
    titles.append(title)
    filename = os.path.join(_LOGGING_DIR, title)
    directory = os.path.dirname(filename)
    if not os.path.exists(directory):
      os.makedirs(directory)
    print('Logging to: {}'.format(filename))
    with open(filename, 'w') as outfile:
      subprocess.Popen(command_to_launch.command_as_list,
                       env=env,
                       stdout=outfile,
                       stderr=outfile)
