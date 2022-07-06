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

R"""This example shows how to introduce batched handlers.

It is useful for centralised inference handled in Python.
"""


import time

from absl import app
from absl import flags
from absl import logging
import launchpad as lp
import numpy as np


_NUM_CLIENTS = flags.DEFINE_integer('num_clients', 2, 'The number of clients.')


class Client:
  """A simple client that calls batched server method."""

  def __init__(self, client_id: int, server: lp.CourierClient) -> None:
    """Initializes a Client.

    Args:
      client_id: Id of the client.
      server: Server's handler.
    """
    self._client_id = client_id
    self._server = server

  def run(self) -> None:
    """Entry point of the client."""
    for x in range(self._client_id, self._client_id + 10):
      result = self._server.compute([x, x + 1])
      logging.info('Result: %s', result)
    time.sleep(5)
    lp.stop()


class Server:
  """A simple server which sums ."""

  def __init__(self, batch_size) -> None:
    self._sum = None
    self.compute = lp.batched_handler(batch_size=batch_size)(self.compute)

  def compute(self, values):
    result = np.sum(values, axis=0)
    if self._sum is None:
      self._sum = result
    else:
      self._sum += result
    return np.tile(self._sum, (len(values), 1))


def make_program() -> lp.Program:
  """Define the program topology."""
  program = lp.Program('batching')
  batch_size = _NUM_CLIENTS.value
  # In case of big batch size it is important to set thread_pool_size to
  # prevent deadlocks (Courier uses synchronous GRPC server currently...).
  server = program.add_node(
      lp.CourierNode(
          Server, batch_size, courier_kwargs={'thread_pool_size': batch_size}),
      label='server')
  with program.group('clients'):
    for client_id in range(_NUM_CLIENTS.value):
      program.add_node(lp.CourierNode(Client, client_id, server))
  return program


def main(argv):
  if len(argv) > 1:
    raise app.UsageError('Too many command-line arguments.')
  program = make_program()
  lp.launch(program)


if __name__ == '__main__':
  app.run(main)
