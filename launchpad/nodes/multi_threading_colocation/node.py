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

"""A special node type for colocating multiple PyNodes."""

import collections
from concurrent import futures
import itertools
import threading
from typing import Any, Sequence

from launchpad.nodes.python import node as python

HandleType = Any


def run_and_return_future(f):
  future = futures.Future()
  def run_inner(f=f, future=future):
    try:
      future.set_result(f())
    except Exception as e:  
      future.set_exception(e)
  threading.Thread(target=run_inner, daemon=True).start()
  return future


class MultiThreadingColocation(python.PyNode):
  """A special node type for colocating multiple PyNodes.

  Please don't add inner nodes to the program, as they will become part of this
  colocation node.

  Example:

      learner_node = lp.CacherNode(...)
      replay_node = lp.CourierNode(...)
      program.add_node(lp.MultiThreadingColocation([learner_node, replay_node]))

  In `__init__()`, `return_when` defaults to `futures.FIRST_EXCEPTION`, meaning
  it will return from `run()` when 1) any of the colocated PyNodes throws an
  exception, or 2) all of them finish. This could be set to
  `futures.FIRST_COMPLETED` so as to wait until any of the nodes finishes (or
  throws an exception).
  """

  def __init__(self,
               nodes: Sequence[python.PyNode],
               return_when=futures.FIRST_EXCEPTION):
    super().__init__(self.run)
    self._nodes = []
    self._name_uniquifier = collections.defaultdict(itertools.count)
    self._return_when = return_when
    for node in nodes:
      self.add_node(node)

  def add_node(self, node: python.PyNode) -> HandleType:
    if not isinstance(node, python.PyNode):
      raise ValueError('MultiThreadingColocation only works with PyNodes.')
    self._nodes.append(node)
    # Take over the addresses of the node.
    for address in node.addresses:
      self.allocate_address(address)
      # Ensure unique address names (avoid name clash when creating named ports)
      address.name = address.name or 'lp'  # Name might not be specified
      unique_id = str(next(self._name_uniquifier[address.name]))
      address.name = address.name + unique_id
    return node.create_handle()

  @property
  def nodes(self):
    return self._nodes

  def run(self):
    for node in self._nodes:
      node._launch_context = self._launch_context  
    if not self._nodes:
      raise ValueError('MultiThreadingColocation requires at least one node.')

    not_done = [run_and_return_future(n.function) for n in self._nodes]
    done = []
    while not_done:
      # Wait with a timeout so that this thread will catch exceptions. In
      # particular, SystemExit injected by lp.stop().
      done, not_done = futures.wait(
          not_done, return_when=self._return_when, timeout=1)
      if done and self._return_when in [
          futures.FIRST_EXCEPTION, futures.FIRST_COMPLETED
      ]:
        break

    # Any exception will be raised in the main thread.
    for f in done:
      f.result()
