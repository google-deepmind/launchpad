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

"""Tests for launchpad.nodes.multi_threading_colocation.node."""

import threading

from absl.testing import absltest
import courier
import launchpad as lp
from launchpad.launch import worker_manager


class NodeTest(absltest.TestCase):

  def test_ping(self):
    program = lp.Program('test')
    server_address = lp.Address()

    def run_client():
      client = courier.Client(server_address.resolve())
      client.ping()

    has_ping = threading.Event()

    def run_server():
      server = courier.Server(
          port=lp.get_port_from_address(server_address.resolve()))
      server.Bind('ping', has_ping.set)
      server.Start()
      lp.wait_for_stop()
      server.Stop()

    client_node = lp.PyNode(run_client)
    server_node = lp.PyNode(run_server)
    server_node.allocate_address(server_address)
    program.add_node(
        lp.MultiThreadingColocation([client_node, server_node]),
        label='client_server')
    lp.launch(
        program,
        launch_type='test_mt',
        test_case=self,
        serialize_py_nodes=False)  # Disable serialization due to `Lock` objects
    has_ping.wait()

  def test_preemption(self):
    program = lp.Program('test')
    ready_to_preempt = threading.Event()
    preemption_ok = threading.Event()

    def node():
      ready_to_preempt.set()
      lp.wait_for_stop()
      preemption_ok.set()

    def stopper():
      ready_to_preempt.wait()
      lp.stop()

    program.add_node(
        lp.MultiThreadingColocation([lp.PyNode(node), lp.PyNode(stopper)]),
        label='coloc')
     # Disable serialization due to `Lock` objects
    lp.launch(program, launch_type='test_mt', test_case=self,
              serialize_py_nodes=False)
    preemption_ok.wait()

  def test_exception_propagation(self):

    def raise_error():
      raise RuntimeError('Foo')

    def wait_test_end():
      lp.wait_for_stop()

    error_node = lp.PyNode(raise_error)
    waiter_node = lp.PyNode(wait_test_end)
    colo_node = lp.MultiThreadingColocation([error_node, waiter_node])
    parent_manager = worker_manager.WorkerManager(register_in_thread=True)
    with self.assertRaisesRegex(RuntimeError, 'Foo'):
      manager = colo_node.run()
      self.addCleanup(manager.cleanup_after_test, self)  # pytype: disable=attribute-error
    del parent_manager

  def test_first_completed(self):
    manager = worker_manager.WorkerManager(register_in_thread=True)
    self.addCleanup(manager.cleanup_after_test, self)
    quick_done = threading.Event()
    slow_done = threading.Event()
    slow_can_start = threading.Event()

    def quick():
      quick_done.set()

    def slow():
      slow_can_start.wait()
      slow_done.set()

    colo_node = lp.MultiThreadingColocation(
        [lp.PyNode(quick), lp.PyNode(slow)],
        return_on_first_completed=True)
    colo_node.run()  # Returns immediately without waiting for the slow node.
    self.assertTrue(quick_done.is_set())
    self.assertFalse(slow_done.is_set())

    # Let the slow one finish.
    slow_can_start.set()
    slow_done.wait()

  def test_all_completed(self):
    manager = worker_manager.WorkerManager(register_in_thread=True)
    self.addCleanup(manager.cleanup_after_test, self)
    f1_done = threading.Event()
    f2_done = threading.Event()

    colo_node = lp.MultiThreadingColocation(
        [lp.PyNode(f1_done.set), lp.PyNode(f2_done.set)],
        return_on_first_completed=False)
    colo_node.run()  # Returns after both f1 and f2 finish.
    self.assertTrue(f1_done.is_set())
    self.assertTrue(f2_done.is_set())

  def test_stop(self):
    def _sleep():
      lp.wait_for_stop()

    def _stop():
      lp.stop()

    program = lp.Program('stop')
    program.add_node(
        lp.MultiThreadingColocation([lp.PyNode(_sleep),
                                     lp.PyNode(_stop)]), label='node')
    waiter = lp.launch(program, launch_type='test_mt', test_case=self)
    waiter.wait()

  def test_nested_stop(self):
    def _sleep():
      lp.wait_for_stop()

    def _stop():
      lp.stop()

    program = lp.Program('stop')
    program.add_node(
        lp.MultiThreadingColocation([
            lp.PyNode(_sleep),
            lp.MultiThreadingColocation([lp.PyNode(_sleep),
                                         lp.PyNode(_stop)])
        ]), label='node')
    waiter = lp.launch(program, launch_type='test_mt', test_case=self)
    waiter.wait()


if __name__ == '__main__':
  absltest.main()
