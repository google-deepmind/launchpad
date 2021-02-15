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

"""Feature testing utilities."""
import distutils
import distutils.spawn
import os
from typing import Text


def _has_command(command: Text) -> bool:
  """Determines whether a command is available in the user's shell.

  Args:
    command: The name of the command.

  Returns:
    Whether the command exists.
  """
  return distutils.spawn.find_executable(command) is not None


def has_x() -> bool:
  """Determines whether X is running."""
  return True if os.environ.get('DISPLAY', '') else False


def has_xterm() -> bool:
  """Determines whether xterm can run."""
  return has_x() and _has_command('xterm')


def has_tmux() -> bool:
  """Determines whether tmux can run."""
  return _has_command('tmux')


def has_byobu() -> bool:
  """Determines whether byobu can run."""
  return _has_command('byobu')


def has_gnome_terminal() -> bool:
  """Determines whether gnome-terminal can run."""
  return has_x() and _has_command('gnome-terminal')
