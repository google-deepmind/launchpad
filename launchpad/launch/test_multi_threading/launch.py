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

import collections
from concurrent import futures
import os
import sys
import threading
import traceback

from typing import Mapping, Optional, Sequence, Text

from absl import logging

from launchpad import context


def _run_worker(worker_func):
  """Runs a worker (as a callable) but terminates the process upon exception."""
  try:
    worker_func()
  except Exception:  
    logging.warning(
        'A worker has died with error type: %s, '
        'error value:\n %s. \n\nShutting down test...',
        *sys.exc_info()[:2])
    # Somehow pylint thinks some arguments are missing
    
    logging.error('Traceback:\n%s',
                  ''.join(traceback.format_exception(*sys.exc_info())))
    # Abort the main process.
    os.kill(os.getpid(), 9)
    


class _Threads:
  """Encapsulates the running threads of a launched Program."""

  def __init__(self, thread_dict: Mapping[Text, Sequence[threading.Thread]]):
    """Initializes a _Threads.

    Args:
      thread_dict: Mapping from node group label to list of running threads
          for that group.
    """
    self.thread_dict = thread_dict

  def wait(self, labels_to_wait_for: Optional[Sequence[Text]] = None):
    """Wait for threads to finish.

    Args:
      labels_to_wait_for: If supplied, only wait for these groups' threads to
          finish (but still raise an exception if any thread from any group
          fails).

    Raises:
      RuntimeError: if any thread raises an exception.
    """
    threads_to_wait_for = set()
    for label in (labels_to_wait_for if labels_to_wait_for is not None
                  else self.thread_dict):
      threads_to_wait_for.update(self.thread_dict[label])

    all_threads = set()
    for threads in self.thread_dict.values():
      all_threads.update(threads)
    should_exit = threading.Event()
    executor = futures.ThreadPoolExecutor(len(all_threads))
    def waiter(thread):
      while thread.is_alive() and not should_exit.is_set():
        thread.join(0.01)
      return thread
    thread_futures = [executor.submit(waiter, t) for t in all_threads]
    while threads_to_wait_for:
      done, thread_futures = futures.wait(
          thread_futures, return_when=futures.FIRST_COMPLETED)
      for future in done:
        future.result()
      threads_to_wait_for.difference_update(f.result() for f in done)
    should_exit.set()

  def join(self):

    self.wait()



def launch(program):
  """Launches the program as a multi-threaded integration test."""
  for node in program.get_all_nodes():
    node._launch_context.initialize(  
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



  thread_dict = collections.defaultdict(list)
  for label, nodes in program.groups.items():
    # to_executables() is a static method, so we can call it from any of the
    # nodes in this group.
    # Somehow pytype thinks to_executables() gets wrong arg count.
    
    
    executables = nodes[0].to_executables(nodes, label,
                                          nodes[0]._launch_context)
    
    
    for executable in executables:
      thread = threading.Thread(target=_run_worker, args=(executable,))
      thread.daemon = True
      thread.start()
      thread_dict[label].append(thread)

  return _Threads(thread_dict)
