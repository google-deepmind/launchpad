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

"""Utilities to resolve addresses in multithreaded tests."""

from typing import Sequence

from launchpad import address as lp_address
from launchpad.nodes import base



def bind_addresses(nodes: Sequence[base.Node]):
  for node in nodes:
    for address in node.addresses:
      address.bind(lp_address.SimpleLocalAddressBuilder())
