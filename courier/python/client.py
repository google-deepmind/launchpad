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

"""Python client bindings for Courier RPCs.

Example usage:
server = courier.Server('my_server')
server.Bind('my_function', lambda a, b: a + b)
server.Start()

client = courier.Client('my_server')
result = client.my_function(4, 7)  # 11, evaluated on the server.
"""

from concurrent import futures
import datetime
from typing import List, Optional, Union

from courier.python import py_client


class _AsyncClient:
  """Asynchronous client."""

  def __init__(
      self,
      client: 'Client',
      wait_for_ready: bool,
      call_timeout: datetime.timedelta,
      compress: bool,
  ):
    self._client = client
    self._wait_for_ready = wait_for_ready
    self._call_timeout = call_timeout
    self._compress = compress

  def __getattr__(self, method):
    """Gets a callable function for the method that returns a future.

    Args:
      method: Name of the method.

    Returns:
      Callable function for the method that returns a future.
    """

    def call(*args, **kwargs):  
      f = futures.Future()

      def set_exception(s):
        f.set_exception(translate_status(s))

      canceller = self._client.AsyncPyCall(method, list(args), kwargs,
                                           f.set_result, set_exception,
                                           self._wait_for_ready,
                                           self._call_timeout, self._compress)

      def done_callback(f):
        if f.cancelled():
          canceller.Cancel()

      f.add_done_callback(done_callback)
      return f

    return call


class Client:
  """Client class for using Courier RPCs.

  This provides a convenience wrapper around the CLIF bindings which allows
  calling server methods as if they were class methods.
  """

  def __init__(
      self,
      server_address: str,
      compress: bool = False,
      call_timeout: Optional[Union[int, float, datetime.timedelta]] = None,
      wait_for_ready: bool = True,
  ):
    """Initiates a new client that will connect to a server.

    Args:
      server_address: Address of the server. If the string does not start
        with "/" or "localhost" then it will be interpreted as a custom BNS
        registered server_name (constructor passed to Server).
      compress: Whether to use compression.
      call_timeout: If set, uses a timeout for all calls.
      wait_for_ready: Sets `wait_for_ready` on the gRPC::ClientContext.
        This specifies whether to wait for a server to come online.
    """
    self._init_args = (server_address, compress)
    self._address = str(server_address)
    self._compress = compress
    self._client = py_client.PyClient(self._address)
    self._call_timeout = call_timeout if call_timeout else datetime.timedelta(0)
    if not isinstance(self._call_timeout, datetime.timedelta):
      self._call_timeout = datetime.timedelta(seconds=self._call_timeout)
    self._wait_for_ready = wait_for_ready
    self._async_client = _AsyncClient(self._client, self._wait_for_ready,
                                      self._call_timeout, self._compress)

  def __reduce__(self):
    return self.__class__, self._init_args

  @property
  def address(self) -> str:
    return self._address

  @property
  def futures(self) -> _AsyncClient:
    """Gets an asynchronous client on which a method call returns a future."""
    return self._async_client

  def __getattr__(self, method: str):
    """Gets a callable function for the method and sets it as an attribute.

    Args:
      method: Name of the method.

    Returns:
      Callable function for the method.
    """
    def func(*args, **kwargs):
      return self._client.PyCall(method, list(args), kwargs,
                                 self._wait_for_ready, self._call_timeout,
                                 self._compress)

    setattr(self, method, func)
    return func


def list_methods(client: Client) -> List[str]:
  """Lists the methods which are available on the server.

  Args:
    client: A client instance.

  Returns:
    List of method names.
  """
  return client._client.ListMethods()  
