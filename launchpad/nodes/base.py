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

"""A Launchpad Node.

Node represents a service. It may return a handle for others to interact with
it.
"""

import abc
import functools
from typing import Any, Generic, List, Optional, Sequence, Set, TypeVar

from launchpad import address as lp_address
from launchpad import context as lp_context
from launchpad.nodes import dereference
from launchpad.program_stopper import program_stopper

ClientType = TypeVar('ClientType')
HandleType = TypeVar('HandleType', bound='Handle')


class Handle(dereference.Dereferenceable[ClientType], Generic[ClientType]):
  """Represents an interface of the service (the node).

  Call `.dereference()` to get the actual client object of this service (to be
  implemented in subclasses).
  """

  def connect(self, node: 'Node[Handle[ClientType]]', label: str) -> None:
    """Called to let this handle know about it's connecting to a node.

    This is supposed to be called:

      1. Before creating any executables
      2. Before any address binding happens

    The motivation is we want to give the handle a chance to configure itself
    for the node, before it's turned into executables and addresses are
    finalized.

    Args:
      node: The node that the handle connects to.
      label: Label of the node.
    """
    pass

  def transform(self, executables: Sequence[Any]) -> Sequence[Any]:
    """Transforms the executables that make use of this handle."""
    return executables


class Node(Generic[HandleType], metaclass=abc.ABCMeta):
  """Represents a service, and may return a Handle for interaction with it."""

  def __init__(self) -> None:
    # This is a low-level API to allow Node/Handle to access launch config
    # during run time. It's only available after launch, and it's set by the
    # launcher.
    self._launch_context = lp_context.LaunchContext()
    # Handles used by this node (to interact with other nodes)
    self._input_handles = []  # type: List[Handle[Any]]
    # Handles created by this node
    # Note: `type: List[HandleType]` is not supported yet.
    self._created_handles = []  # type: List[Handle]
    # Addresses known to the node. This exists so that launchpad can, from
    # the program (which contains the nodes), list all the addresses that need
    # to be bind before launch.
    # `addresses` usually contains the address(es) owned by the node. However,
    # in case of nodes containing other nodes (e.g. multi-threading nodes), it
    # will also contain addresses owned by sub-nodes.
    # Thus, use `address.assign` to give ownership of an address to a node,
    # and `addresses.append` to only expose the address to launchpad launch
    # mechanism.
    self.addresses = []  # type: List[lp_address.Address]

  @property
  def launch_context(self):
    return self._launch_context

  def _initialize_context(self, launch_type: lp_context.LaunchType,
                          launch_config: Any):
    self._launch_context.initialize(
        launch_type, launch_config,
        program_stopper.make_program_stopper(launch_type))

  def _track_handle(self, handle: HandleType) -> HandleType:
    """Keeps track of created handles.

    MUST be called in create_handle().

    This is called so that the node knows about the handle it creates. The
    reason we don't automate this is because we'll lose return annotation if
    we wrap create_handle() using a base class method (i.e., the base class
    wrapper method doesn't know about the subclass return type).

    Args:
      handle: The handle (MUST be created by this node) to track.

    Returns:
      The same handle that was passed in, for the nicer syntax on call site.
    """
    self._created_handles.append(handle)
    return handle

  @abc.abstractmethod
  def create_handle(self) -> HandleType:
    """Creates a handle to interact with this node.

    MUST call _track_handle() after creating a handle.
    """
    raise NotImplementedError()

  @abc.abstractstaticmethod
  def to_executables(nodes, label, context):
    """Creates executables for a specific launch type."""
    raise NotImplementedError()


  def bind_addresses(self, **kwargs) -> None:
    """Binds addresses of the node."""
    del kwargs  # Unused.

  @property
  def input_handles(self) -> List[Handle[Any]]:
    return list(self._input_handles)

  def allocate_address(self, address: lp_address.Address) -> None:
    """Low-level API to add an address to listen to.

    Prefer `address.assign(node)`.

    This is a low level API and users shouldn't need to use it most of the time.

    Args:
      address: Address to listen to (i.e., to create a server).
    """
    address.assign(self)

  @classmethod
  def default_launch_config(cls, launch_type: lp_context.LaunchType):
    """Defines the default launch config of this node type.

    This is optional. The returned config is conditional on the launch type.

    Args:
      launch_type: Return the launch_config for this launch_type.
    """
    raise NotImplementedError(
        f'Launch config has to be explicitly specified for {cls.__name__}')


def extract_handles(
    obj: Any,
    handles: List[Handle],
    visited: Optional[Set[int]] = None,
) -> None:
  """Extract the handles of `obj` to and add them into `handles`."""
  visited = visited or set()
  # Transitive input_handles from Deferred objects are included.
  if isinstance(obj, dereference.Deferred):
    if id(obj) not in visited:
      visited.add(id(obj))
      obj._apply_to_args(  
          functools.partial(extract_handles, handles=handles, visited=visited))
  elif isinstance(obj, Handle):
    handles.append(obj)
