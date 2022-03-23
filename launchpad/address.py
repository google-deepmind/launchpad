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

"""Placeholders of network addresses to be evaluated at runtime."""

import abc
import os
import re
from typing import Optional
from absl import logging
import portpicker

_ADDRESS_NAME_VALID_PATTERN = re.compile('[a-z][a-z0-9]*')


class AbstractAddressBuilder(metaclass=abc.ABCMeta):
  """Base class for creating a platform-specific address at runtime."""

  @abc.abstractmethod
  def build(self) -> str:
    """Builds an address."""


class SimpleLocalAddressBuilder(AbstractAddressBuilder):
  """Creates a locahost:port address, with port decided by portpicker."""

  def __init__(self):
    # This automatically makes use of PORTSERVER_ADDRESS (usually set by test)
    self._address = 'localhost:{}'.format(portpicker.pick_unused_port())

  def build(self) -> str:
    return self._address


class Address(object):
  """A network address to be evaluated.

  1. An unbound address is created using `address = Address()`.
  2. An address should be assigned to exactly one node e.g. using
     `address.assign(node)`.
  2. Upon launching, Launchpad will call bind() for each address to assign the
     address builder, which constructs the actual string format address at
     runtime. Launchpad has access to the addressed used in a program because
     each node maintains an `addresses` list of the nodes he knows about (it can
     be the address(es) the node own, or addresses that sub-nodes own.
  3. At runtime, use `address.resolve()` to finally resolve it.

  Using an unbound address will trigger an error.

  Attributes:
    name: Name of this address.
  """

  def __init__(self, name: Optional[str] = None):
    """Initializes an address object.

    Args:
      name: (Optional) Name of the address.
    """
    if name is not None and not _ADDRESS_NAME_VALID_PATTERN.fullmatch(name):
      raise ValueError(f'Wrong address name: {name} does not match '
                       f'{_ADDRESS_NAME_VALID_PATTERN.pattern}.')
    self.name = name
    self._address_builder = None  # type: AbstractAddressBuilder
    self._owning_node = None

  def bind(self, address_builder: AbstractAddressBuilder) -> None:
    """Sets a function that creates the platform-specific address at runtime."""
    # The address cannot be evaluated before we launch, because we might not
    # have all the necessary info for evaluation
    self._address_builder = address_builder

  def resolve(self) -> str:
    """Returns the address as a string."""
    if not self._address_builder:
      if self._owning_node is None:
        raise RuntimeError(
            "The lp.Address hasn't been assigned to any node, "
            'thus it cannot be resolved. Use '
            '`address.assign(node)` to assign an address to a node')
      raise RuntimeError(
          f'The lp.Address associated to the node {self._owning_node} has not '
          'been bound to any address builder. Launchpad is responsible for '
          'doing that at launch time. If you are in tests, '
          'you can use:\n\n'
          'from launchpad.launch.test_multi_threading import address_builder as test_address_builder\n'
          '...\n'
          'test_address_builder.bind_addresses([node])')

    return self._address_builder.build()

  def assign(self, node) -> None:
    """Assigns the Address to the specified node (must be done exactly once)."""
    if self._owning_node is not None:
      if self._owning_node is node:
        logging.warning(
            'You are binding an lp.Address twice on the same node '
            "of type %s it's probably a mistake.", node)
        return
      raise ValueError('You are associating a node to this lp.Address which '
                       'is already assigned to another node. The previous node '
                       'and the new node are:\n'
                       f'{node}\n{self._owning_node}')
    self._owning_node = node
    node.addresses.append(self)

  def __getstate__(self):
    state = self.__dict__.copy()
    # Don't pickle `_owning_node`, as it's here only for launch-time checks.
    # Instead, store a description of the node. If a restored instance is
    # re-pickled, leave the existing node description unchanged.
    if not isinstance(self._owning_node, str):
      state['_owning_node'] = ('Restored from pickle node named: ' +
                               repr(self._owning_node))
    return state


def get_port_from_address(address: str) -> int:
  """Returns the port from a given address.

  Note that Launchpad uses a convention where named ports are passed as
  environment variables. For example, for the named port 'baz', the actual port
  value will be stored in LP_PORT_baz environment variable.

  Args:
    address: address with a named port or a host:port address.

  Returns:
    The port number as an integer.
  """
  port_name = address.split(':')[-1]
  if port_name.isdigit():
    return int(port_name)
  else:
    return int(os.environ['LP_PORT_' + port_name])
