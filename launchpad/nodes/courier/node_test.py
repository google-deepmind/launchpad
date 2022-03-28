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

"""Tests for launchpad.nodes.courier.node."""

import datetime
import sys
import threading
from unittest import mock

from absl.testing import absltest
import courier
from launchpad.launch.test_multi_threading import address_builder
from launchpad.nodes.courier import node as lp_courier


class Server(object):
  """Terminates once it receives the first ping() call."""

  def __init__(self):
    self._server = None  # type: courier.Server
    self._has_ping = threading.Event()
    self._has_call = threading.Event()

  def ping(self):
    self._has_ping.set()
    return 'pong'

  def __call__(self):
    self._has_call.set()
    return 'called'

  def set_courier_server(self, server: courier.Server):
    self._server = server

  def run(self):
    self._server.Start()
    self._has_ping.wait()
    self._has_call.wait()
    self._server.Stop()


class CourierNodeTest(absltest.TestCase):

  def test_ping_pong(self):
    node = lp_courier.CourierNode(Server)
    handle = node.create_handle()

    # Bind all addresses
    address_builder.bind_addresses([node])

    threading.Thread(target=node.run).start()
    client = handle.dereference()
    self.assertEqual(client.ping(), 'pong')
    self.assertEqual(client(), 'called')
    # Make sure Tensorflow is not imported.
    self.assertNotIn('tensorflow', sys.modules)

  def test_future_ping_pong(self):
    node = lp_courier.CourierNode(Server)
    handle = node.create_handle()

    # Bind all addresses
    address_builder.bind_addresses([node])

    threading.Thread(target=node.run).start()
    client = handle.dereference()
    self.assertEqual(client.futures.ping().result(), 'pong')
    self.assertEqual(client.futures().result(), 'called')
    # Make sure Tensorflow is not imported.
    self.assertNotIn('tensorflow', sys.modules)

  def test_cyclic_reference(self):

    def _foo(bar):
      del bar  # unused

    def _bar(foo):
      del foo  # unused

    foo_node = lp_courier.CourierNode(_foo)
    foo_handle = foo_node.create_handle()
    bar_node = lp_courier.CourierNode(_bar)
    bar_handle = bar_node.create_handle()

    self.assertNotIn(foo_handle, bar_node._input_handles)
    self.assertNotIn(bar_handle, foo_node._input_handles)
    foo_node.configure(bar=bar_handle)
    bar_node.configure(foo=foo_handle)
    self.assertIn(foo_handle, bar_node._input_handles)
    self.assertIn(bar_handle, foo_node._input_handles)




if __name__ == '__main__':
  absltest.main()
