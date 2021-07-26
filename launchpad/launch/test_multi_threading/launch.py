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

"""Launches a Launchpad program as a multithreaded integration test.

This is very similar to local_multi_threading/launch.py but terminates the
process upon exception (instead of entering pdb).
"""

from typing import Optional
from absl.testing import absltest

from launchpad import context
from launchpad.launch import worker_manager



def launch(program, test_case: Optional[absltest.TestCase] = None):
  """Launches the program as a multi-threaded integration test."""
  for node in program.get_all_nodes():
    node._initialize_context(  
        context.LaunchType.TEST_MULTI_THREADING,
        launch_config=None)

  # Notify the input handles
  for label, nodes in program.groups.items():
    for node in nodes:
      for handle in node._input_handles:  
        handle.connect(node, label)

  # Bind addresses
  for node in program.get_all_nodes():
    node.bind_addresses()



  manager = worker_manager.WorkerManager()
  if test_case is not None:
    test_case.addCleanup(manager.cleanup_after_test, test_case)

  for label, nodes in program.groups.items():
    # to_executables() is a static method, so we can call it from any of the
    # nodes in this group.
    # Somehow pytype thinks to_executables() gets wrong arg count.
    
    # pytype: disable=wrong-arg-count
    executables = nodes[0].to_executables(nodes, label,
                                          nodes[0]._launch_context)
    
    # pytype: enable=wrong-arg-count
    for executable in executables:
      manager.thread_worker(label, executable)

  return manager
