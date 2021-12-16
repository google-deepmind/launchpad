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

"""Addressing for PyNodes."""

import collections
import itertools
import json
import os
import typing
from typing import Any, List, Optional

from absl import flags
from absl import logging

from launchpad import address as lp_address
from launchpad import flags as lp_flags  


FLAGS = flags.FLAGS






def bind_addresses_local(addresses: List[lp_address.Address]):
  """Binds addresses for the local launch."""

  for address in addresses:
    address.bind(lp_address.SimpleLocalAddressBuilder())


class VertextAiAddressBuilder(lp_address.AbstractAddressBuilder):
  """Builds an address for Vertex AI."""

  def __init__(self, cluster: str, instance: int):
    self._cluster = cluster
    self._instance = instance

  def build(self) -> str:
    cluster_spec = os.environ.get('CLUSTER_SPEC', None)
    return json.loads(cluster_spec).get('cluster').get(
        self._cluster)[self._instance]


def bind_addresses_vertex_ai(addresses: List[lp_address.Address], cluster: str,
                             instance: int):
  """Binds addresses for the execution using Vertex AI."""
  if len(addresses) > 1:
    raise RuntimeError(
        f'Vertex AI supports only one port per node. {len(addresses)} requested.'
    )

  if len(addresses) == 1:
    addresses[0].bind(VertextAiAddressBuilder(cluster, instance))
