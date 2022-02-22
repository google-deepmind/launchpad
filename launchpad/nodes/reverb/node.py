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

"""Reverb replay buffers."""


from typing import Any, Callable, Optional, Sequence

from absl import logging
from launchpad import address as lp_address
from launchpad import context
from launchpad.launch import worker_manager
from launchpad.nodes import base
from launchpad.nodes.python import node as python
import reverb


PriorityTablesFactory = Callable[[], Sequence[reverb.Table]]
CheckpointerFactory = Callable[[], reverb.checkpointers.CheckpointerBase]

REVERB_PORT_NAME = 'reverb'


class ReverbHandle(base.Handle):
  """Handle of the ReverbNode.

  When dereferenced a reverb-Client (see https://github.com/deepmind/reverb/client.py) is
  returned. This client should primarily be used for insert operations on the
  actors. For sampling and updates TFClient (see
  https://github.com/deepmind/reverb/tf_client.py) should be used.

  To construct a TFClient:

  ```python
  from reverb import tf_client as reverb_tf_client

  client = ...  # reverb.Client
  tf_client = reverb_tf_client.TFClient(client.server_address)
  ```

  The TF-client is not made directly available through LP as it would require
  a dependency on TF even when TF is not used (e.g many actors).
  """

  def __init__(self, address: lp_address.Address):
    self._address = address

  def dereference(self):
    address = self._address.resolve()
    logging.info('Reverb client connecting to: %s', address)
    return reverb.Client(address)



class ReverbNode(python.PyNode):
  """Represents a Reverb replay buffer in a Launchpad program."""

  def __init__(self,
               priority_tables_fn: PriorityTablesFactory,
               checkpoint_ctor: Optional[CheckpointerFactory] = None,
               checkpoint_time_delta_minutes: Optional[int] = None):
    """Initialize a ReverbNode.

    Args:
      priority_tables_fn: A mapping from table name to function used to
        compute priorities for said table.
      checkpoint_ctor: Constructor for the checkpointer to be used. Passing None
        uses Reverb's default checkpointer.
      checkpoint_time_delta_minutes: Time between async (non-blocking)
        checkpointing calls.
    """
    super().__init__(self.run)
    self._priority_tables_fn = priority_tables_fn
    self._checkpoint_ctor = checkpoint_ctor
    self._checkpoint_time_delta_minutes = checkpoint_time_delta_minutes
    self._address = lp_address.Address(REVERB_PORT_NAME)
    self.allocate_address(self._address)

    if (self._checkpoint_time_delta_minutes is not None and
        self._checkpoint_time_delta_minutes <= 0):
      raise ValueError(
          'Replay checkpoint time delta must be positive when specified.')

  def create_handle(self):
    return self._track_handle(ReverbHandle(self._address))

  def run(self):
    priority_tables = self._priority_tables_fn()
    if self._checkpoint_ctor is None:
      checkpointer = None
    else:
      checkpointer = self._checkpoint_ctor()

    self._server = reverb.Server(
        tables=priority_tables,
        port=lp_address.get_port_from_address(self._address.resolve()),
        checkpointer=checkpointer)

    if self._checkpoint_time_delta_minutes is not None:
      while not worker_manager.wait_for_stop(
          self._checkpoint_time_delta_minutes * 60):
        self._server.localhost_client().checkpoint()
    else:
      worker_manager.wait_for_stop()

  @staticmethod
  def to_executables(nodes: Sequence['ReverbNode'], label: str,
                     launch_context: context.LaunchContext):
    return python.PyNode.to_executables(nodes, label, launch_context)

  @property
  def reverb_address(self) -> lp_address.Address:
    return self._address



