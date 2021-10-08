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

"""Placeholders of network addresses to be evaluated at runtime.

1. An unbound address is created using `address = Address()`.
2. Upon launching, Launchpad will call bind() for each address to assign the
   address builder, which constructs the actual string format address at
   runtime.
3. At runtime, use `address.resolve()` to finally resolve it.

Using an unbound address will trigger an error.
"""

import abc
import os
import re
from typing import Optional

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

  Launchpad will call bind() on each address upon launching. Once the program
  is running, call resolve() to get the actual network address as a string.

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

  def bind(self, address_builder: AbstractAddressBuilder):
    """Sets a function that creates the platform-specific address at runtime."""
    # The address cannot be evaluated before we launch, because we might not
    # have all the necessary info for evaluation
    self._address_builder = address_builder

  def resolve(self):
    """Returns the address as a string."""
    if not self._address_builder:
      raise RuntimeError('Unbound address cannot be resolved.')
    return self._address_builder.build()


def get_port_from_address(address: str) -> int:
  """Utility function to extract a port from a given address.

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
