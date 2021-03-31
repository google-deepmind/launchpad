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

"""Utilities for waiting threads, useful in multithreading launch."""

from concurrent import futures
import threading
from typing import Mapping, Optional, Sequence, Text


class ThreadWaiter:
  """Encapsulates the running threads of a launched Program."""

  def __init__(self, thread_dict: Mapping[Text, Sequence[threading.Thread]]):
    """Initializes a ThreadWaiter.

    Args:
      thread_dict: Mapping from node group label to list of running threads for
        that group.
    """
    self.thread_dict = thread_dict

  def wait(self, labels_to_wait_for: Optional[Sequence[Text]] = None):
    """Waits for threads to finish.

    Args:
      labels_to_wait_for: If supplied, only wait for these groups' threads to
        finish (but still raise an exception if any thread from any group
        fails).

    Raises:
      RuntimeError: if any thread raises an exception.
    """
    threads_to_wait_for = set()
    for label in (labels_to_wait_for
                  if labels_to_wait_for is not None else self.thread_dict):
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
