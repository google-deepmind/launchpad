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

  def ping(self):
    self._has_ping.set()
    return 'pong'

  def set_courier_server(self, server: courier.Server):
    self._server = server

  def run(self):
    self._server.Start()
    self._has_ping.wait()
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


class CacherNodeTest(absltest.TestCase):

  def test_bind_cacher(self):
    # Mock a CourierHandle
    mock_handle = mock.create_autospec(lp_courier.CourierHandle, instance=True)
    mock_client = mock.create_autospec(courier.Client, instance=True)
    mock_handle.dereference.return_value = mock_client
    mock_client.address = 'localhost:12345'

    with mock.patch.object(courier, 'Server') as mock_server_cls:
      # Mock courier server
      mock_server = mock.Mock()
      mock_server_cls.return_value = mock_server

      node = lp_courier.CacherNode(mock_handle, 100, 200)

      # Bind all addresses
      address_builder.bind_addresses([node])

      node.run()

      # Verify methods are called with right arguments
      mock_server.BindCacher.assert_called_once_with(
          target_server_address='localhost:12345',
          poll_interval=datetime.timedelta(milliseconds=100),
          stale_after=datetime.timedelta(milliseconds=200))
      mock_server.Start.assert_called_once_with()
      mock_server.Join.assert_called_once_with()


if __name__ == '__main__':
  absltest.main()
