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

"""Nodes that run user-defined Python code.

PyNode runs a user-defined function. PyClassNode constructs a Python object and
calls its run() method (if provided).

"""

import functools
import itertools
from typing import Any, Callable, Generic, TypeVar

from absl import logging
from launchpad import context
from launchpad import lazy_loader
from launchpad.nodes import base
from launchpad.nodes import dereference
from launchpad.nodes.python import addressing

from launchpad.nodes.python import local_multi_processing
import tree


T = TypeVar('T')
HandleType = TypeVar('HandleType', bound=base.Handle)
ReturnType = TypeVar('ReturnType')
WorkerType = TypeVar('WorkerType')


class _DummyHandle(base.Handle[Any]):

  def dereference(self) -> None:
    raise NotImplementedError('_DummyHandle cannot be dereferenced.')


class PyNode(base.Node[HandleType], Generic[HandleType, ReturnType]):
  """Runs a user-defined Python function."""

  # Used only for no-serialization launch
  NEXT_PY_NODE_ID = itertools.count()

  def __init__(self, function: Callable[..., ReturnType], *args, **kwargs):
    super().__init__()
    self._func_args = args
    self._func_kwargs = kwargs
    self._function = function
    self._partial_function = self._construct_function
    self.py_node_id = next(PyNode.NEXT_PY_NODE_ID)

    # Find input handles and put them in self._input_handles.
    tree.map_structure(
        functools.partial(base.extract_handles, handles=self._input_handles),
        (self._func_args, self._func_kwargs))

  def _construct_function(self):
    context.set_context(self._launch_context)  
    args, kwargs = tree.map_structure(dereference.maybe_dereference,
                                      (self._func_args, self._func_kwargs))
    return functools.partial(self._function, *args, **kwargs)()

  def create_handle(self) -> HandleType:
    """Doesn't expose an interface for others to interact with it."""
    return _DummyHandle()

  @property
  def function(self) -> Callable[..., ReturnType]:
    return self._partial_function

  @staticmethod
  def to_executables(nodes, label, launch_context):
    """Creates Executables."""
    if (launch_context.launch_type in [
        context.LaunchType.LOCAL_MULTI_THREADING,
        context.LaunchType.TEST_MULTI_THREADING
    ]):
      return [node.function for node in nodes]
    elif (launch_context.launch_type is
          context.LaunchType.LOCAL_MULTI_PROCESSING):
      return local_multi_processing.to_multiprocessing_executables(
          nodes, label, launch_context.launch_config, pdb_post_mortem=True)
    elif (
        launch_context.launch_type is context.LaunchType.TEST_MULTI_PROCESSING):
      return local_multi_processing.to_multiprocessing_executables(
          nodes, label, launch_context.launch_config, pdb_post_mortem=False)
    raise NotImplementedError('Unsupported launch type: {}'.format(
        launch_context.launch_type))


  def bind_addresses(self, **kwargs):
    if self._launch_context.launch_type in [
        context.LaunchType.LOCAL_MULTI_THREADING,
        context.LaunchType.LOCAL_MULTI_PROCESSING,
        context.LaunchType.TEST_MULTI_PROCESSING,
        context.LaunchType.TEST_MULTI_THREADING
    ]:
      addressing.bind_addresses_local(self.addresses)
    else:
      raise NotImplementedError('Unsupported launch type: {}'.format(
          self._launch_context.launch_type))

  @classmethod
  def default_launch_config(cls, launch_type: context.LaunchType):
    if launch_type in [
        context.LaunchType.LOCAL_MULTI_THREADING,
        context.LaunchType.TEST_MULTI_THREADING
    ]:
      return None
    return super().default_launch_config(launch_type)


class PyClassNode(PyNode[HandleType, type(None)],
                  Generic[HandleType, WorkerType]):
  """Instantiates a Python object and runs its run() method (if provided).

  If disable_run() is called before launch, instance.run() method won't be
  called. This is useful in TAP-based integration tests, where users might need
  to step each worker synchronously.
  """

  def __init__(self, constructor: Callable[..., WorkerType], *args, **kwargs):
    """Initializes a new instance of the `PyClassNode` class.

    Args:
      constructor: A function that when called returns a Python object with a
        run method.
      *args: Arguments passed to the constructor.
      **kwargs: Key word arguments passed to the constructor.
    """
    super().__init__(self.run)
    self._constructor = constructor
    self._args = args
    self._kwargs = kwargs
    self._should_run = True
    self._collect_input_handles()

  def _collect_input_handles(self):
    self._input_handles.clear()
    try:
      # Find input handles and put them in self._input_handles.
      tree.map_structure(
          functools.partial(base.extract_handles, handles=self._input_handles),
          (self._args, self._kwargs))
    except TypeError as e:
      raise ValueError(
          f'Failed to construct the {self.__class__.__name__} with\n'
          f'- constructor: {self._constructor}\n'
          f'- args: {self._args}\n- kwargs: {self._kwargs}') from e

  def _construct_instance(self) -> WorkerType:
    args, kwargs = tree.map_structure(dereference.maybe_dereference,
                                      (self._args, self._kwargs))
    return self._constructor(*args, **kwargs)

  def disable_run(self) -> None:
    """Prevents the node from calling `run` on the Python object.

    Note that the Python object is still constructed even if `disable_run` has
    been called.
    """
    self._should_run = False

  def enable_run(self) -> None:
    """Ensures `run` is called on the Python object.

    This is the default state and callers don't need to call `enable_run` unless
    `disable_run` has been called.
    """
    self._should_run = True

  def run(self) -> None:
    """Constructs Python object and (maybe) calls its `run` method.

    The `run` method is not called if `disable_run` has ben called previously or
    if the constructed Python object does not have a `run` method.
    """
    instance = self._construct_instance()
    if hasattr(instance, 'run') and self._should_run:
      instance.run()
    else:
      logging.warning(
          'run() not defined on the instance (or disable_run() was called.).'
          'Exiting...')
