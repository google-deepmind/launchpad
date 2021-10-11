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
import os
import signal
import threading
import time
from typing import Optional

from absl import logging
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

  # Run a background thread to detect and handle node failures.
  stop_node_monitor = threading.Event()
  def _node_monitor():
    while not stop_node_monitor.is_set():
      time.sleep(.5)
      try:
        manager.check_for_thread_worker_exception()
      except Exception:  
        logging.exception('One of the workers has FAILED!')
        # Wait for 3s, in case the exception is caught timely.
        time.sleep(3)
        if not stop_node_monitor.is_set():
          # The exception isn't caught in time and we have to kill the test to
          # avoid a timeout. This happens when, for example, a client running in
          # the main thread trying (forever) to talk to a failed server.
          logging.info('Killing the test due to an uncaught exception. See the '
                       'above for stack traces.')
          os.kill(os.getpid(), signal.SIGQUIT)
          return
  node_monitor_thread = threading.Thread(target=_node_monitor, daemon=True)
  node_monitor_thread.start()

  def _cleanup():
    stop_node_monitor.set()
    node_monitor_thread.join()
    manager.cleanup_after_test(test_case)

  if test_case is not None:
    test_case.addCleanup(_cleanup)

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
