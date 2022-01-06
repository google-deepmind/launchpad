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

import atexit
from concurrent import futures
import sys
import threading
from typing import Optional

from absl import flags
from launchpad import context
from launchpad import flags as lp_flags  
from launchpad import program as lp_program
from launchpad.launch import serialization
from launchpad.launch import worker_manager

FLAGS = flags.FLAGS


def launch(program: lp_program.Program,
           *,
           serialize_py_nodes: Optional[bool] = None):
  """Launches a program using multiple threads."""
  if serialize_py_nodes is None:
    serialize_py_nodes = False

  # Set up the launch context (launch type & launch config) for all nodes
  for label, nodes in program.groups.items():
    if serialize_py_nodes:
      serialization.check_nodes_are_serializable(label, nodes)

    for node in nodes:
      node._initialize_context(  
          context.LaunchType.LOCAL_MULTI_THREADING,
          launch_config=None)

  # Notify the input handles
  for label, nodes in program.groups.items():
    for node in nodes:
      for handle in node.input_handles:
        handle.connect(node, label)

  # Setup addressing
  for node in program.get_all_nodes():
    node.bind_addresses()

  return thread_handler(program)


def thread_handler(program):
  """Runs the threads and wraps them in Worker Manager."""

  manager = worker_manager.WorkerManager(
  )
  for label, nodes in program.groups.items():
    # to_executables() is a static method, so we can call it from any of the
    # nodes in this group.
    # Somehow pytype thinks to_executables() gets wrong arg count.
    # pytype: disable=wrong-arg-count
    executables = nodes[0].to_executables(nodes, label,
                                          nodes[0].launch_context)
    # pytype: enable=wrong-arg-count
    for executable in executables:
      manager.thread_worker(label, executable)

  if sys.version_info[:2] >= (3, 9):
    # Make sure `manager.wait` will be called before ThreadPoolExecutor atexit
    # method. Otherwise running program will not be able to start new threads.
    futures.ThreadPoolExecutor  
    threading._register_atexit(manager.wait)  
  else:
    atexit.register(manager.wait)

  return manager
