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
import threading
import time

from absl.testing import absltest

from launchpad.launch import worker_manager
import mock



class WorkerManagerTest(absltest.TestCase):

  def setUp(self):
    super().setUp()
    self._sigterm_patcher = mock.patch.object(
        signal, 'SIGTERM', new=signal.SIGUSR1)
    self._sigterm_patcher.start()
    self._sigint_patcher = mock.patch.object(
        signal, 'SIGINT', new=signal.SIGUSR2)
    self._sigint_patcher.start()
    self._manager = worker_manager.WorkerManager()
    self.addCleanup(self._manager.cleanup_after_test, self)

  def tearDown(self):
    self._sigterm_patcher.stop()
    self._sigint_patcher.stop()
    super().tearDown()

  def test_wait_for_stop(self):

    def waiter():
      self.assertTrue(self._manager.wait_for_stop())

    self._manager.thread_worker('worker', waiter)
    os.kill(os.getpid(), signal.SIGTERM)
    self.assertTrue(self._manager.wait_for_stop())

  def test_stop_event(self):

    def waiter():
      self.assertTrue(self._manager.stop_event().wait())

    self._manager.thread_worker('worker', waiter)
    os.kill(os.getpid(), signal.SIGTERM)
    self.assertTrue(self._manager.wait_for_stop())

  def test_wait_for_stop_timeout(self):
    checks_done = threading.Event()

    def waiter():
      self.assertFalse(self._manager.wait_for_stop(0))
      self.assertFalse(self._manager.wait_for_stop(0.1))
      checks_done.set()
      self.assertTrue(self._manager.wait_for_stop(10))

    self._manager.thread_worker('worker', waiter)
    self.assertTrue(checks_done.wait())
    os.kill(os.getpid(), signal.SIGTERM)
    self.assertTrue(self._manager.wait_for_stop())

  def test_slow_termination(self):
    def waiter():
      self.assertTrue(self._manager.wait_for_stop())
      time.sleep(1)

    self._manager.thread_worker('worker', waiter)
    os.kill(os.getpid(), signal.SIGTERM)
    self.assertTrue(self._manager.wait_for_stop())

  def test_system_exit(self):
    def waiter():
      self.assertTrue(self._manager.wait_for_stop(100.0))

    self._manager.thread_worker('worker', waiter)
    os.kill(os.getpid(), signal.SIGTERM)
    self.assertTrue(self._manager.wait_for_stop())


  def test_stop_and_wait(self):
    def waiter():
      self.assertTrue(self._manager.wait_for_stop())

    self._manager.thread_worker('worker1', waiter)
    self._manager.thread_worker('worker2', waiter)
    self._manager.thread_worker('worker3', waiter)
    self._manager.stop_and_wait()

  def test_failure_wait(self):
    def waiter():
      self.assertTrue(self._manager.wait_for_stop())

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
      self.assertTrue(self._manager.wait_for_stop())

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
