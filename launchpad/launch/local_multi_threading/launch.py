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

"""Local Multithreading Launcher implementation."""


import collections
import threading

from absl import flags
from launchpad import context
from launchpad import program as lp_program
from launchpad.launch import signal_handling
from launchpad.launch.local_multi_threading import thread_waiter



def launch(program: lp_program.Program):
  """Launches a program using multiple threads."""
  # Set up the launch context (launch type & launch config) for all nodes
  for label, nodes in program.groups.items():
    for node in nodes:
      node._launch_context.initialize(  
          context.LaunchType.LOCAL_MULTI_THREADING,
          launch_config=None)

  # Notify the input handles
  for label, nodes in program.groups.items():
    for node in nodes:
      for handle in node._input_handles:  
        handle.connect(node, label)

  # Setup addressing
  for node in program.get_all_nodes():
    node.bind_addresses()

  thread_handler = vanilla_thread_handler

  signal_handling.exit_gracefully_on_sigquit()
  return thread_handler(program)


def vanilla_thread_handler(program):
  """Runs the threads directly."""

  thread_dict = collections.defaultdict(list)
  for label, nodes in program.groups.items():
    # to_executables() is a static method, so we can call it from any of the
    # nodes in this group.
    
    # pytype: disable=wrong-arg-count
    executables = nodes[0].to_executables(nodes, label,
                                          nodes[0]._launch_context)
    
    # pytype: enable=wrong-arg-count
    for executable in executables:
      thread = threading.Thread(target=executable, daemon=True)
      thread.start()
      thread_dict[label].append(thread)

  return thread_waiter.ThreadWaiter(thread_dict)


