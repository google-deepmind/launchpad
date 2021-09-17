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

import os
from typing import Any, Mapping, Optional

from absl.testing import absltest
from launchpad import context
from launchpad import program as lp_program
from launchpad.launch import worker_manager


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
    
    # pytype: enable=wrong-arg-count
    if commands:
      label_to_commands[label] = commands
      # Not to create to actual processes, in case of failures in this loop.
      process_handles[label] = []

  manager = worker_manager.WorkerManager(kill_main_thread=False)
  for label, commands in label_to_commands.items():
    for command in commands:
      env = {}
      env.update(os.environ)
      env.update(command.env_overrides)
      manager.process_worker(label, command.command_as_list, env=env)
  test_case.addCleanup(manager.cleanup_after_test, test_case=test_case)
  return manager
