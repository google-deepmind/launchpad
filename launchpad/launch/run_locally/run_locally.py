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

"""Provides functionality for running commands locally."""

import collections
import os
from typing import Optional, Sequence, Text

from absl import logging

from launchpad.launch.run_locally import feature_testing
from launchpad.launch.run_locally import launch_local_current_terminal
from launchpad.launch.run_locally import launch_local_gnome
from launchpad.launch.run_locally import launch_local_output_to_files
from launchpad.launch.run_locally import launch_local_tmux
from launchpad.launch.run_locally import launch_local_xterm

SEPARATE_TERMINAL_XTERM = 'xterm'
SEPARATE_TERMINAL_GNOME_TERMINAL_WINDOWS = 'gnome-terminal'
SEPARATE_TERMINAL_GNOME_TERMINAL_TABS = 'gnome-terminal-tabs'
SEPARATE_TERMINAL_TMUX_SESSION = 'tmux_session'
SEPARATE_TERMINAL_BYOBU_SESSION = 'byobu_session'
SEPARATE_TERMINAL_CURRENT_TERMINAL = 'current_terminal'
SEPARATE_TERMINAL_OUTPUT_TO_FILES = 'output_to_files'

SEPARATE_TERMINAL_MODES = (
    SEPARATE_TERMINAL_XTERM,
    SEPARATE_TERMINAL_GNOME_TERMINAL_WINDOWS,
    SEPARATE_TERMINAL_GNOME_TERMINAL_TABS,
    SEPARATE_TERMINAL_TMUX_SESSION,
    SEPARATE_TERMINAL_BYOBU_SESSION,
    SEPARATE_TERMINAL_CURRENT_TERMINAL,
    SEPARATE_TERMINAL_OUTPUT_TO_FILES,
)

TERMINALS_FOR_X = (
    SEPARATE_TERMINAL_XTERM,
    SEPARATE_TERMINAL_GNOME_TERMINAL_WINDOWS,
    SEPARATE_TERMINAL_GNOME_TERMINAL_TABS,
)

# Map terminal name to the corresponding launch function
_LOCAL_LAUNCHER_MAP = {
    SEPARATE_TERMINAL_XTERM:
        launch_local_xterm.launch_with_xterm,
    SEPARATE_TERMINAL_GNOME_TERMINAL_WINDOWS:
        launch_local_gnome.launch_with_gnome_terminal_windows,
    SEPARATE_TERMINAL_GNOME_TERMINAL_TABS:
        launch_local_gnome.launch_with_gnome_terminal_tabs,
    SEPARATE_TERMINAL_TMUX_SESSION:
        launch_local_tmux.launch_with_tmux_session,
    SEPARATE_TERMINAL_BYOBU_SESSION:
        launch_local_tmux.launch_with_byobu_session,
    SEPARATE_TERMINAL_CURRENT_TERMINAL:
        launch_local_current_terminal.launch_in_current_terminal,
    SEPARATE_TERMINAL_OUTPUT_TO_FILES:
        launch_local_output_to_files.launch_and_output_to_files,
}


class CommandToLaunch(
    collections.namedtuple(
        'command_to_launch',
        ['command_as_list', 'env_overrides', 'resource_name', 'worker_name'])):

  @property
  def title(self):
    return '{}_{}'.format(self.resource_name, self.worker_name)


def _get_terminal(given_terminal: Optional[Text]):
  """Returns the terminal for local launch based on X & command availability.

  By order of priority it will:
  - use the provided `given_terminal`
  - default to the shell environment variable `LAUNCHPAD_LAUNCH_LOCAL_TERMINAL`
    if set
  - or select the first supported option in: Gnome, Tmux, Xterm and current
    terminal.

  Args:
    given_terminal: The terminal identifier to use or `None`.

  Returns:
    One of the legal terminal modes (a string in SEPARATE_TERMINAL_MODES) based
    on the priority described above.
  """
  if (given_terminal is not None and
      given_terminal not in SEPARATE_TERMINAL_MODES):
    raise ValueError('`terminal` got a mode that it does not '
                     'understand %r. Please choose from %r.' %
                     (given_terminal, SEPARATE_TERMINAL_MODES))
  terminal = given_terminal or os.environ.get('LAUNCHPAD_LAUNCH_LOCAL_TERMINAL',
                                              None)
  # Set terminal to None, if the chosen terminal cannot be used because we are
  # running without X.
  if not feature_testing.has_x() and terminal in TERMINALS_FOR_X:
    logging.info('Not using %s to launch, since DISPLAY is not set.', terminal)
    terminal = None

  if terminal is None:
    if feature_testing.has_gnome_terminal():
      terminal = SEPARATE_TERMINAL_GNOME_TERMINAL_WINDOWS
    elif feature_testing.has_tmux():
      terminal = SEPARATE_TERMINAL_TMUX_SESSION
    elif feature_testing.has_xterm():
      terminal = SEPARATE_TERMINAL_XTERM

    # Examine the type of terminal and explain why it is chosen.
    if terminal is None:
      logging.info('Launching in the same console since we cannot find '
                   'gnome-terminal, tmux, or xterm.')
      terminal = SEPARATE_TERMINAL_CURRENT_TERMINAL
    else:
      logging.info(
          'Launching with %s because the `terminal` launch option '
          'is not explicitly specified. To remember your preference '
          '(assuming tmux_session is the preferred option), either: \n'
          '1. Pass the `terminal` launch option (e.g., '
          '`lp.launch(program, terminal="tmux_session")`).\n'
          '2. Set the following in your bashrc to remember your '
          'preference:\n'
          '    export LAUNCHPAD_LAUNCH_LOCAL_TERMINAL=tmux_session', terminal)
  return terminal


def run_commands_locally(commands: Sequence[CommandToLaunch], terminal=None):
  # Minimally validate all the commands before executing any of them. This also
  # gives better errors in the case that a terminal implementation executes
  # the commands via a wrapper.
  for command in commands:
    if not os.access(command.command_as_list[0], os.X_OK):
      raise ValueError("Unable to execute '%s'" % command.command_as_list[0])
  _LOCAL_LAUNCHER_MAP[_get_terminal(terminal)](commands)
