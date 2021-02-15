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

"""Tests for launchpad.nodes.reverb.node."""

import threading
from unittest import mock

from absl.testing import absltest
from absl.testing import parameterized
from launchpad import context
from launchpad.launch.test_multi_threading import address_builder as test_address_builder
from launchpad.nodes.reverb import node as reverb_node
import numpy as np
import reverb
from reverb import rate_limiters

_TABLE_NAME = 'dist'


def priority_tables_fn():
  return [
      reverb.Table(
          name=_TABLE_NAME,
          sampler=reverb.selectors.Uniform(),
          remover=reverb.selectors.Fifo(),
          max_size=100,
          rate_limiter=rate_limiters.MinSize(100))
  ]


class ReverbNodeTest(absltest.TestCase):

  def test_insert(self):

    node = reverb_node.ReverbNode(priority_tables_fn=priority_tables_fn)
    test_address_builder.bind_addresses([node])
    threading.Thread(target=node.run).start()

    client = node.create_handle().dereference()
    client.insert([np.zeros((81, 81))], {_TABLE_NAME: 1})

    node._server.stop()



if __name__ == '__main__':
  absltest.main()
