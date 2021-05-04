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

"""Run commands to launch Launchpad workers in gnome-terminal."""

import atexit
import datetime
import os
import shlex
import signal
import stat
import subprocess
import tempfile
import time

from launchpad.launch.run_locally import feature_testing
import psutil

GNOME_TERMINAL_SERVER_PATHS = [
    '/usr/libexec/gnome-terminal-server',
    '/usr/lib/gnome-terminal/gnome-terminal-server',
]


def find_gnome_terminal_server():
  """Probe multiple locations for gnome-terminal-server, as this is distro specific."""
  for path in GNOME_TERMINAL_SERVER_PATHS:
    if os.path.isfile(path):
      return path
  return None


def _run_gnome_command(command, env):
  """Launches gnome command, retrying until gnome server is ready."""
  retry_backoff = 0.1
  while True:
    process = subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    stdout, stderr = process.communicate()
    process.wait()
    if ('# Error creating terminal' not in stderr.decode('UTF-8') or
        retry_backoff > 10):
      if stdout:
        print(stdout)
      if stderr:
        print(stderr)
      break
    time.sleep(retry_backoff)
    retry_backoff *= 2


def _launch_in_windows(commands_to_launch, app_id):
  """Launches commands in gnome windows."""
  for window_index, command_to_launch in enumerate(commands_to_launch):
    set_title = (r'PS1=$; PROMPT_COMMAND=; '
                 'echo -en "\\033]0;{}\\a"'.format(command_to_launch.title))
    inner_cmd = '{}; {}; exec $SHELL'.format(
        set_title, subprocess.list2cmdline(command_to_launch.command_as_list))
    terminal_command_list = [
        'gnome-terminal',
        '--app-id',
        app_id,  # Connects to the recently opened terminal server.
        '--geometry',
        '80x60+{}+{}'.format(window_index * 40, window_index * 40),
        '--',
        'bash',
        '-c',
        inner_cmd,
    ]
    env = {}
    env.update(os.environ)
    env.update(command_to_launch.env_overrides)
    _run_gnome_command(terminal_command_list, env)


def _launch_in_tabs(commands_to_launch, app_id):
  """Launches commands in gnome tabs."""
  file_handle, command_file_path = tempfile.mkstemp('.sh')
  os.close(file_handle)
  atexit.register(os.remove, command_file_path)
  with open(command_file_path, 'w') as command_file:
    # The command file starts with setting up environment.
    for key, value in os.environ.items():
      # Remove these two keys so that new processes are created by the newly
      # started gnome-terminal-server.
      if key in ['GNOME_TERMINAL_SERVICE', 'GNOME_TERMINAL_SCREEN']:
        continue
      command_file.write(f'export {shlex.quote(key)}={shlex.quote(value)}\n')
    for command_to_launch in commands_to_launch:
      inner_cmd = '; '.join([
          # Set the title (see https://superuser.com/a/1330292/156433).
          'PS1=$',
          'PROMPT_COMMAND=',
          f'echo -en "\\033]0;{command_to_launch.title}\\a"',
          # Run the actual command.
          subprocess.list2cmdline(command_to_launch.command_as_list),
          # Start a shell so that the tab doesn't close instantly when the
          # command finishes.
          'exec $SHELL',
      ])
      terminal_command_list = [
          'gnome-terminal',
          '--tab',
          '--',
          'bash',
          '-c',
          inner_cmd,
      ]
      env_overrides = []
      for key, value in command_to_launch.env_overrides.items():
        env_overrides.append(f'{shlex.quote(key)}={shlex.quote(value)}')
      command_file.write(
          subprocess.list2cmdline(env_overrides + terminal_command_list) + '\n')

  os.chmod(command_file_path, os.stat(command_file_path).st_mode | stat.S_IEXEC)
  _run_gnome_command(
      ['gnome-terminal', '--app-id', app_id, '--', command_file_path],
      os.environ)


def launch_with_gnome_terminal(commands_to_launch, use_tabs=False):
  """Launch commands given as CommandToLaunch tuples with gnome-terminal.

  Args:
    commands_to_launch: An iterable of `CommandToLaunch` namedtuples.
    use_tabs: Whether or not to run each command in a gnome tab (instead of a
      window)
  """
  # The new server-client architecture of gnome-terminal removes several
  # extremely useful features. Relevant ones here are
  #   * The ability to launch commands as new tabs.
  #   * The ability to destroy gnome terminal windows from the command line.
  # While there is nothing we can do about the former, there's a workaround
  # for the latter. We start a new gnome-terminal-server and launch all
  # windows belonging to this launchpad session in this server. To clean up,
  # we can just kill the server and all windows get closed. This has the
  # additional benefit that windows are grouped together when switching between
  # applications with alt-tab.
  if not feature_testing.has_gnome_terminal():
    raise ValueError('`gnome-terminal` is not available, '
                     'please choose another way to launch.')
  # Check if we can find the gnome-terminal-server.
  gnome_terminal_server_path = find_gnome_terminal_server()
  if not gnome_terminal_server_path:
    raise ValueError(
        'gnome-terminal-server is not present on your system but it is required '
        'to launch locally with gnome-terminal. Searched: {}'.format(
            GNOME_TERMINAL_SERVER_PATHS))
  # Start session.
  timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
  # app-ids must be character only. Turn numbers in timestep to characters.
  timestamp = ''.join(['abcdefghij'[int(c)] for c in timestamp])
  app_id = 'launchpad.locallaunch.{}'.format(timestamp)
  # Gnome terminal server waits for 10 seconds. If no terminal has connected
  # to it by then it will terminate. Wait half that time after starting up the
  # server to make sure the server is running before starting up terminals.
  def preexec_fn():
    # Prevents SIGINT from killing gnome-terminal-server too early
    signal.signal(signal.SIGINT, signal.SIG_IGN)

  
  server_process = subprocess.Popen([
      gnome_terminal_server_path, '--app-id', app_id, '--name', app_id,
      '--class', app_id
  ],
                                    env=os.environ,
                                    preexec_fn=preexec_fn)
  if use_tabs:
    _launch_in_tabs(commands_to_launch, app_id)
  else:
    _launch_in_windows(commands_to_launch, app_id)

  def kill_processes():
    parent = psutil.Process(server_process.pid)
    children = parent.children(recursive=True)
    for process in children:
      try:
        process.send_signal(9)
      except psutil.NoSuchProcess:
        pass
    server_process.kill()

  atexit.register(kill_processes)


def launch_with_gnome_terminal_windows(commands_to_launch):
  launch_with_gnome_terminal(commands_to_launch, use_tabs=False)


def launch_with_gnome_terminal_tabs(commands_to_launch):
  launch_with_gnome_terminal(commands_to_launch, use_tabs=True)
