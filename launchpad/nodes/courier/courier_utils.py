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

"""Courier utilities."""

import datetime
import functools
import inspect
from typing import Any, Callable, Text

from absl import logging

import courier
from courier.handlers.python import pybind
def batched_handler(batch_size, max_parallelism=1,
                    timeout=datetime.timedelta(milliseconds=200),
                    pad_batch=False):
  """A decorator to enable batching of the CourierNode method.

  `batch_size` calls to the method will be batched and executed in one go.
  Tensors and Numpy arrays are batched by adding an additional batching
  dimension. Each value inside of the dictionary as well as elements of the
  tupples are batched independently. Primitive types are batched by being
  wrapped in a list. Batched method is supposed to return a batched result, so
  that it can be unbatch and corresponding results send back to each caller.
  Class member functions should be wrapped in the __init__ function instead
  of decorated (as self parameter needs to be bound at the time of wrapping),
  for example:

  class Server:

    def __init__(self, batch_size):
      self.compute = lp.batched_handler(batch_size=batch_size)(self.compute)

    def compute(self, values):
      ...

  Args:
    batch_size: How many calls to batch together (at most).
    max_parallelism: How many parallel calls of the function being batched to
        allow.
    timeout: Timeout after which batched handler will be called even if
        `batch_size` calls has not been collected.
    pad_batch: Should the timed-out batch be padded to the batch_size.
        It guarantees that all executed batches are of the same size.

  Returns:
    The decorated function.
  """
  def decorator_batch(func):
    args = inspect.getfullargspec(func).args
    # Make sure function being wrapped doesn't expect `self` as the first
    # argument. Otherwise it means it is a non-bound class member.
    assert not args or args[0] != 'self' or hasattr(func, '__self__'), (
        'Do not decorate class methods with @lp.batched_handler. '
        'Wrap them in the object constructor instead.')
    handler = pybind.BuildPyCallHandler(func)
    batcher = pybind.BuildBatchedHandlerWrapper(func.__name__, handler,
                                                batch_size, max_parallelism,
                                                timeout, pad_batch)

    @functools.wraps(func)
    def wrapper_batch(*args, **kwargs):
      return pybind.CallHandler(batcher, func.__name__, list(args), kwargs)

    
    wrapper_batch._batched_handler = True
    wrapper_batch._batch_size = batch_size
    wrapper_batch._max_parallelism = max_parallelism
    wrapper_batch._timeout = timeout
    wrapper_batch._pad_batch = pad_batch
    wrapper_batch._func = func
    

    return wrapper_batch
  return decorator_batch


def _should_expose_method(func: Callable[..., Any], method_name: Text) -> bool:
  return (callable(func) and method_name != 'set_courier_server' and
          (not method_name.startswith('_') or method_name == '__call__'))


def make_courier_server(instance: Any, *courier_args,
                        **courier_kwargs) -> courier.Server:
  """Builds a courier.Server for an instance.


  Args:
    instance: The instance that the courier server associates with.
    *courier_args: positional arguments to pass to courier.Server().
    **courier_kwargs: keyword arguments to pass to courier.Server().

  Returns:
    A courier.Server object.
  """
  server = courier.Server(*courier_args, **courier_kwargs)

  # Bind all non-private user-defined local methods.
  for method_name in dir(instance):
    func = getattr(instance, method_name)
    if _should_expose_method(func, method_name):
      logging.info('Binding: %s', method_name)
      handler = None
      if func.__dict__.get('_batched_handler', False):
        if handler is None:
          handler = pybind.BuildPyCallHandler(func.__dict__['_func'])
        batch_size = func.__dict__['_batch_size']
        max_parallelism = func.__dict__['_max_parallelism']
        timeout = func.__dict__['_timeout']
        pad_batch = func.__dict__['_pad_batch']
        handler = pybind.BuildBatchedHandlerWrapper(method_name, handler,
                                                    batch_size, max_parallelism,
                                                    timeout, pad_batch)
      if handler is None:
        handler = pybind.BuildPyCallHandler(func)
      server.BindHandler(method_name, handler)

  return server
