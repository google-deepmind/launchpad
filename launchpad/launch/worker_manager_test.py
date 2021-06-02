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

"""Tests for launchpad.launch.worker_manager."""

import os
import signal
import time

from absl.testing import absltest
from launchpad.launch import worker_manager


class WorkerManagerTest(absltest.TestCase):

  def setUp(self):
    super().setUp()
    self._manager = worker_manager.WorkerManager()
    self.addCleanup(self._manager.cleanup_after_test, self)

  def test_wait_for_stop(self):

    def waiter():
      self._manager.wait_for_stop()

    self._manager.thread_worker('worker', waiter)
    os.kill(os.getpid(), signal.SIGTERM)

  def test_slow_termination(self):
    def waiter():
      self._manager.wait_for_stop()
      time.sleep(1)

    self._manager.thread_worker('worker', waiter)
    os.kill(os.getpid(), signal.SIGTERM)

  def test_system_exit(self):
    def waiter():
      try:
        while True:
          time.sleep(0.1)
      except SystemExit:
        pass

    self._manager.thread_worker('worker', waiter)
    os.kill(os.getpid(), signal.SIGTERM)

  def test_stop_and_wait(self):
    def waiter():
      self._manager.wait_for_stop()

    self._manager.thread_worker('worker1', waiter)
    self._manager.thread_worker('worker2', waiter)
    self._manager.thread_worker('worker3', waiter)
    self._manager.stop_and_wait()
    self._manager.cleanup_after_test(self)

  def test_failure_wait(self):
    def waiter():
      self._manager.wait_for_stop()

    def failure():
      raise Exception('Error')

    self._manager.thread_worker('waiter', waiter)
    self._manager.thread_worker('failure', failure)
    with self.assertRaisesRegexp(  
        Exception, 'Error'):
      self._manager.wait(['waiter'])
    self._manager.wait()

  def test_return_on_first_completed(self):
    def waiter():
      self._manager.wait_for_stop()

    def worker():
      pass

    self._manager.thread_worker('waiter', waiter)
    self._manager.thread_worker('worker', worker)
    self._manager.wait(return_on_first_completed=True)

  def test_dont_raise_error(self):
    def failure():
      raise Exception('Error')

    self._manager.thread_worker('failure', failure)
    self._manager.wait(raise_error=False)
    with self.assertRaisesRegexp(  
        Exception, 'Error'):
      self._manager.wait()

  def test_process_worker_stop(self):
    self._manager.process_worker('sleep', ['sleep', '3600'])
    self._manager.stop_and_wait()

  def test_process_worker_failure(self):
    self._manager.process_worker('failure', ['cat', 'missing_file'])
    with self.assertRaisesRegexp(  
        RuntimeError, 'One of the workers failed.'):
      self._manager.wait()


if __name__ == '__main__':
  absltest.main()
