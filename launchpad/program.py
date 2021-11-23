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

"""A Launchpad program."""

import contextlib
import dataclasses
import itertools

from typing import Any, Dict, List, Optional

from launchpad.nodes import base


HandleType = Any




class Program(object):
  """A Launchpad program, representing a distributed program and its topology.

  A Launchpad program contains nodes, each node could be just a process, or
  provide a service, etc (please refer to Launchpad documentation for types of
  nodes). Homogenous nodes are organized as groups. Here's an example of
  adding nodes to a group:

      with program.group('actor'):
        program.add_node(lp.CourierNode(MyActor, ...))

  `add_node()` returns a handle, which can be passed to another node for
  communication purpose, and is the way to set up distributed communication
  topology. For example:

      with program.group('learner'):
        learner = program.add_node(lp.CourierNode(MyLearner, ...))

      with program.group('actor'):
        program.add_node(lp.CourierNode(MyActor, learner=leaner))
  """

  def __init__(self, name: str):
    self._name = name
    self._groups = {}  # type: Dict[str, List[base.Node]]
    # Group to add nodes to. Used by group()
    self._current_group = None  # type: str

  def add_node(self,
               node: base.Node,
               label: Optional[str] = None) -> HandleType:
    """Adds node to the program and returns the node handle."""

    if self._current_group:
      if label and label != self._current_group:
        raise ValueError('The given label does not match the current group: '
                         f'{label} vs {self._current_group}.')
      label = self._current_group
    else:
      if not label:
        raise ValueError('Label should not be empty.')
    if label not in self._groups:
      self._groups[label] = [node]
    else:
      self._groups[label].append(node)
    return node.create_handle()

  @contextlib.contextmanager
  def group(self, label: str):
    """Creates a group for a collection of homogeneous nodes."""
    if not label:
      raise ValueError('Label should not be empty.')
    if self._current_group:
      raise ValueError('group() cannot be nested.')
    try:
      self._current_group = label
      yield
    finally:
      # Try/finally is to make sure that the current_group is correctly
      # reset even if an exception occurs.
      self._current_group = None


  def get_all_nodes(self) -> List[base.Node]:
    return list(itertools.chain(*self._groups.values()))

  @property
  def name(self) -> str:
    return self._name

  @property
  def groups(self) -> Dict[str, List[base.Node]]:
    return self._groups



def make_program(*nodes: base.Node, name: str = 'launchpad'):
  """A shortcut to create a program from a list of nodes.

  This simplifies the syntax. For example you can do a one-liner launch:

      lp.launch(lp.make_program(lp.PyNode(lambda: ...)))

  Args:
    *nodes: Nodes to run.
    name: An optional name of the program.

  Returns:
    A lp.Program object
  """
  program = Program(name)
  for node in nodes:
    program.add_node(node)
  return program
