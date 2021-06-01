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

"""Launches a Launchpad program as a test, using multiple processes.

Usage:

    class LaunchTest(absltest.TestCase):

      def test_run_program(self):
        program = ...  # Create a program

        # Configure process resources
        local_resources = dict(
            foo=lp.PythonProcess(
                interpreter=...
            )
        )

        lp.launch(program, launch_type='test_mp',
                  local_resources=local_resources, test_case=self)
"""

from concurrent import futures
import os
import signal
import subprocess
from typing import Any, Mapping, Optional, Sequence, Text

from absl import logging
from absl.testing import absltest
from launchpad import context
from launchpad import program as lp_program
import psutil


class _Processes:
  """Encapsulates the running processes of a launched Program."""

  def __init__(self):
    """Initializes a _Processes."""
    self.process_dict: Mapping[Text, Sequence[subprocess.Popen]] = {}
    self._stop_requested = False
    signal.signal(signal.SIGUSR1,
                  lambda signum, unused_frame: self._stop())

  def set_processes(self,
                    process_dict: Mapping[Text, Sequence[subprocess.Popen]]):
    """Set collection of processes to handle.

    Args:
      process_dict: Mapping from node group label to list of running processes
        for that group.
    """
    self.process_dict = process_dict
    if self._stop_requested:
      self.kill_processes()

  def _stop(self):
    self._stop_requested = True
    self.kill_processes()

  def kill_processes(self):
    for processes in self.process_dict.values():
      for process in processes:
        process.kill()

  def wait(self, labels_to_wait_for: Optional[Sequence[Text]] = None):
    """Wait for processes to finish.

    Args:
      labels_to_wait_for: If supplied, only wait for these groups' processes to
        finish (but still raise an exception if any process from any group
        fails).

    Raises:
      RuntimeError: if any process raises an exception.
    """
    processes_to_wait_for = set()
    for label in (labels_to_wait_for
                  if labels_to_wait_for is not None else self.process_dict):
      processes_to_wait_for.update(self.process_dict[label])

    all_processes = set()
    for processes in self.process_dict.values():
      all_processes.update(processes)
    executor = futures.ThreadPoolExecutor(len(all_processes))

    def waiter(p):
      return lambda: (p, p.wait())

    process_futures = [executor.submit(waiter(p)) for p in all_processes]
    while processes_to_wait_for:
      done, process_futures = futures.wait(
          process_futures, return_when=futures.FIRST_COMPLETED)
      for future in done:
        unused_p, returncode = future.result()
        if returncode != 0 and not self._stop_requested:
          raise RuntimeError('One of the processes has failed!')
      processes_to_wait_for.difference_update(f.result()[0] for f in done)

  def ensure_healthy(self):
    for label, procs_per_label in self.process_dict.items():
      for process in procs_per_label:
        if not psutil.pid_exists(process.pid) and process.returncode:
          logging.error('Process %d of %s has crashed.', process.pid, label)
          healthy = False
    if not healthy:
      raise RuntimeError(
          'The processes are not all healthy (see logs above for details)!')


def launch(program: lp_program.Program,
           test_case: absltest.TestCase,
           local_resources: Optional[Mapping[str, Any]] = None):
  """Launches a program using multiple processes as a test."""
  # Set up the launch context (launch type & launch config) for all nodes
  local_resources = local_resources or {}
  for label, nodes in program.groups.items():
    launch_config = local_resources.get(label, None)
    for node in nodes:
      node._initialize_context(  
          context.LaunchType.TEST_MULTI_PROCESSING,
          launch_config=launch_config)

  # Notify the input handles
  for label, nodes in program.groups.items():
    for node in nodes:
      for handle in node._input_handles:  
        handle.connect(node, label)

  # Bind addresses
  for node in program.get_all_nodes():
    node.bind_addresses()

  label_to_commands = {}
  process_handles = {}
  for label, nodes in program.groups.items():
    # to_executables() is a static method, so we can call it from any of the
    # nodes in this group.
    # Somehow pytype thinks to_executables() gets wrong arg count.
    
    # pytype: disable=wrong-arg-count
    commands = nodes[0].to_executables(nodes, label, nodes[0]._launch_context)
    
    # pytype: disable=wrong-arg-count
    if commands:
      label_to_commands[label] = commands
      # Not to create to actual processes, in case of failures in this loop.
      process_handles[label] = []

  processes = _Processes()
  for label, commands in label_to_commands.items():
    for command in commands:
      env = {}
      env.update(os.environ)
      env.update(command.env_overrides)
      process_handles[label].append(
          subprocess.Popen(command.command_as_list, env=env))
  processes.set_processes(process_handles)
  test_case.addCleanup(processes.kill_processes)
  return processes
