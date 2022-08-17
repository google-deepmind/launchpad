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

"""A special node type for colocating multiple PyNodes."""

import collections
import itertools
from typing import Any, Sequence

from launchpad.launch import worker_manager_migration
from launchpad.nodes.python import node as python

HandleType = Any


class MultiThreadingColocation(python.PyNode):
  """A special node type for colocating multiple PyNodes.

  Please don't add inner nodes to the program, as they will become part of this
  colocation node.

  Example:

      learner_node = lp.CacherNode(...)
      replay_node = lp.CourierNode(...)
      program.add_node(lp.MultiThreadingColocation([learner_node, replay_node]))

  In `__init__()`, `return_on_first_completed` defaults to False, meaning
  it will return from `run()` when 1) any of the colocated PyNodes throws an
  exception, or 2) all of them finish. This could be set to True so as to wait
  until any of the nodes finishes (or throws an exception).
  """

  def __init__(self,
               nodes: Sequence[python.PyNode],
               return_on_first_completed=False):
    super().__init__(self.run)
    self._nodes = []
    self._name_uniquifier = collections.defaultdict(itertools.count)
    self._return_on_first_completed = return_on_first_completed
    for node in nodes:
      self.add_node(node)

  def add_node(self, node: python.PyNode) -> HandleType:
    if not isinstance(node, python.PyNode):
      raise ValueError('MultiThreadingColocation only works with PyNodes.')
    self._nodes.append(node)
    # Reference all the children addresses (not owned by this node, but only
    # referenced).
    for address in node.addresses:
      self.addresses.append(address)
      # Ensure unique address names (avoid name clash when creating named ports)
      address.name = address.name or 'lp'  # Name might not be specified
      unique_id = str(next(self._name_uniquifier[address.name]))
      address.name = address.name + unique_id
    return node.create_handle()

  @property
  def nodes(self):
    return self._nodes

  def run(self):
    if not self._nodes:
      raise ValueError('MultiThreadingColocation requires at least one node.')
    group_name = f'coloc_{id(self)}'
    manager = worker_manager_migration.get_worker_manager()

    for n in self._nodes:
      n._launch_context = self._launch_context  
      manager.thread_worker(group_name, n.function)
    manager.wait(
        [group_name],
        return_on_first_completed=self._return_on_first_completed,
        raise_error=True,  # Any error from the inner threads will surface.
    )
    return manager
