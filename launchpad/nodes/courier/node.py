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

"""A PyClassNode subclass that also exposes the instance as a Courier server."""

import datetime
from typing import Callable, Generic, TypeVar

from absl import logging
import courier
from launchpad import address as lp_address
from launchpad.nodes import base
from launchpad.nodes.courier import courier_utils
from launchpad.nodes.python import node as python

WorkerType = TypeVar('WorkerType')
CourierClient = courier.Client

COURIER_PORT_NAME = 'courier'


class CourierHandle(base.Handle[CourierClient]):
  """Handle of a CourierNode."""

  def __init__(self, address: lp_address.Address, **kwargs):
    self._address = address
    self._kwargs = kwargs

  def set_client_kwargs(self, **kwargs):
    self._kwargs = kwargs

  def dereference(self) -> CourierClient:
    return CourierClient(self._address.resolve(), **self._kwargs)


class CourierNode(python.PyClassNode[CourierHandle, WorkerType],
                  Generic[WorkerType]):
  """Exposes a Python instance as a Courier server.

  This will initialize the object and expose all its public methods as Courier
  RPC methods. Attributes and method names starting with underscore will not be
  exposed. After that, run() will be called if it's provided.

  When run() is provided, the server will terminate at the end of run().
  Otherwise, it will serve indefinitely (until the job/experiment terminates).


  Advanced usage: if the object has a set_courier_server() method, it will be
  called with the courier server object passed in as the only argument. The
  courier server will then be managed by the user (e.g., need to manually call
  Start() of the courier server).
  """

  def __init__(self,
               constructor: Callable[..., WorkerType],
               *args,
               courier_kwargs=None,
               **kwargs):
    super().__init__(constructor, *args, **kwargs)  # pytype:disable=wrong-arg-types
    self._address = lp_address.Address(COURIER_PORT_NAME)
    self.allocate_address(self._address)
    if courier_kwargs is None:
      courier_kwargs = dict()
    self._courier_kwargs = courier_kwargs
    # Set in `run()` method.
    self._server = None  # type: courier.Server

  def configure(self, *args, **kwargs):
    """Sets the args and kwargs being passed to the constructor.

    This is useful for achieving cyclic referencing. E.g.:

        foo_node = CourierNode(_foo)
        foo_handle = foo_node.create_handle()
        bar_node = CourierNode(_bar)
        bar_handle = bar_node.create_handle()
        foo_node.configure(bar=bar_handle)
        bar_node.configure(foo=foo_handle)
        p.add_node(foo_node)
        p.add_node(bar_node)

    Args:
      *args: non-keyword arguments to pass to the constructor.
      **kwargs: keyword arguments to pass to the constructor.
    """
    self._args = args
    self._kwargs = kwargs
    # Somehow pytype doesn't recognize CourierNode as the subclass.
    self._collect_input_handles()  # pytype:disable=wrong-arg-types

  def create_handle(self) -> CourierHandle:
    return self._track_handle(CourierHandle(self._address))

  def run(self) -> None:
    instance = self._construct_instance()  # pytype:disable=wrong-arg-types
    self._server = courier_utils.make_courier_server(
        instance,
        port=lp_address.get_port_from_address(self._address.resolve()),
        **self._courier_kwargs)
    if hasattr(instance, 'set_courier_server'):
      # Transfer the ownership of the server to the instance, so that the user
      # can decide when to start and stop the courier server.
      instance.set_courier_server(self._server)
      if hasattr(instance, 'run') and self._should_run:
        instance.run()
    else:
      # Start the server after instantiation and serve forever
      self._server.Start()
      if hasattr(instance, 'run') and self._should_run:
        # If a run() method is provided, stop the server at the end of run().
        instance.run()
        self._server.Stop()
      self._server.Join()

  @property
  def courier_address(self) -> lp_address.Address:
    return self._address



