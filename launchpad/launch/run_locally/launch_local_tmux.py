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

"""Run commands to launch Launchpad workers in tmux."""

import atexit
import os
import subprocess

from absl import flags
from absl import logging
from launchpad import flags as lp_flags  
from launchpad.launch import worker_manager
from launchpad.launch.run_locally import feature_testing


tty_write = print


def launch_with_tmux_session(commands_to_launch,
                             session_name_prefix=None):
  """Launch multiple CommandToLaunch tuples in a new tmux session."""

  if not feature_testing.has_tmux():
    raise ValueError(
        'tmux is not available, please choose another way to launch '
        'or install it.')

  session_name_prefix = session_name_prefix or flags.FLAGS.tmux_session_name

  return _launch_with_multiplex_session(commands_to_launch,
                                        session_name_prefix,
                                        'tmux')


def launch_with_byobu_session(commands_to_launch,
                              session_name_prefix='launchpad'):
  """Launch multiple CommandToLaunch tuples in a new byobu session."""

  if not feature_testing.has_byobu():
    raise ValueError(
        'byobu is not available, please choose another way to launch '
        'or install it.')

  return _launch_with_multiplex_session(commands_to_launch,
                                        session_name_prefix,
                                        'byobu')


def _launch_with_multiplex_session(commands_to_launch, session_name_prefix,
                                   multiplexer):
  """Launch multiple CommandToLaunch tuples in a new multiplex session.

  Args:
    commands_to_launch: An iterable of `CommandToLaunch` namedtuples.
    session_name_prefix: Leading part of the name given to the new tmux session.
      If there is no existing session with this name, it will be used as-is,
      however if another session exists the name will be uniquified by appending
      an incrementing counter.
    multiplexer : tmux or byobu
  Returns:
    Worker manager that can be used to wait for a program execution to finish.
  """

  # Make a new session with the unmodified name, if this fails add a suffix to
  # the name and retry.
  session_name = session_name_prefix
  suffix_index = 0
  while True:
    try:
      subprocess.check_output(
          [multiplexer, 'new-session', '-d', '-s', session_name],
          stderr=subprocess.STDOUT)

    except subprocess.CalledProcessError as e:
      if 'duplicate session' in e.output.decode():
        logging.info('%r session %r already exists, trying to uniquify...',
                     multiplexer, session_name)
        session_name = '{}_{}'.format(session_name_prefix, suffix_index)
        suffix_index += 1
      else:
        raise e  # If `tmux new-session` failed for some other reason.
    else:
      break

  def get_session_processes():
    p = subprocess.run([
        multiplexer, 'list-panes', '-t', session_name, '-s', '-F',
        '"#{pane_pid}"'], stdout=subprocess.PIPE, check=True)

    # Kill all subprocesses in the tmux session
    return [int(pid) for pid in p.stdout.replace(b'"', b'').strip().split()]

  # Copy over the environment of the current process to the new session.
  for key, value in os.environ.items():
    subprocess.check_call(
        [multiplexer, 'set-environment', '-t', session_name, key, value])

  # For each node to run, create the corresponding launch command
  # and run it with subprocess.Popen.
  for command_to_launch in commands_to_launch:

    # Apply command-specific overrides to environment variables.
    env_as_list = [
        f'{k}={v}' for k, v in command_to_launch.env_overrides.items()]

    # When the program is done, echo the command so it can be copy-pasted, and
    # then drop into a shell.

    command_str = subprocess.list2cmdline(env_as_list +
                                          command_to_launch.command_as_list)
    inner_command = f'{command_str}; echo "{command_str}"; exec $SHELL'

    window_name = command_to_launch.title
    command = [
        multiplexer,
        'new-window',
        '-t',
        session_name,
        '-n',
        window_name,
        inner_command,
    ]
    # Make the process block until it has completed.
    subprocess.Popen(command)

  tty_write(
      f'Opened new {multiplexer} session called `{session_name}`. '
      f'If you are already in a tmux session, use `Ctrl+B W` as a '
      f'convenient way to switch to the new session. '
      f'Otherwise run \n\n  {multiplexer} a -t "{session_name}"\n\nTo change '
      f'the name of the tmux sessions use the `--tmux_session_name` flag. You '
      f'can terminate all the processes and the {multiplexer} session by '
      f'pressing Ctrl-C here.\n')
  if flags.FLAGS.tmux_open_window is not None:
    command = [
        multiplexer, 'switch-client', '-t',
        f'{session_name}:{flags.FLAGS.tmux_open_window}'
    ]
    subprocess.run(command, check=True)

  manager = worker_manager.WorkerManager()
  atexit.register(manager.wait)
  for pid in get_session_processes():
    manager.register_existing_process('tmux', pid)
  return manager
