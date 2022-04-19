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

"""Tests for launchpad.program."""

from unittest import mock

from absl.testing import absltest

from launchpad import program as lp_program
from launchpad.nodes import base


class ProgramTest(absltest.TestCase):

  def test_add_node(self):
    program = lp_program.Program('foo')
    node_foo = mock.Mock(autospec=base.Node)
    program.add_node(node_foo, 'foo')
    node_bar = mock.Mock(autospec=base.Node)
    program.add_node(node_bar, 'bar')
    self.assertEqual(program.groups['foo'], [node_foo])
    self.assertEqual(program.groups['bar'], [node_bar])

  def test_add_nodes_to_group(self):
    program = lp_program.Program('foo')
    nodes_foo = [mock.Mock(autospec=base.Node), mock.Mock(autospec=base.Node)]
    with program.group('foo'):
      for node in nodes_foo:
        program.add_node(node)
    nodes_bar = [mock.Mock(autospec=base.Node), mock.Mock(autospec=base.Node)]
    with program.group('bar'):
      for node in nodes_bar:
        program.add_node(node)
    self.assertEqual(program.groups['foo'], nodes_foo)
    self.assertEqual(program.groups['bar'], nodes_bar)

  def test_add_node_without_label(self):
    program = lp_program.Program('foo')
    with self.assertRaisesRegexp(ValueError, 'Label should not be empty.'):  
      program.add_node(mock.Mock(autospec=base.Node))

  def test_add_node_with_empty_label(self):
    program = lp_program.Program('foo')
    with self.assertRaisesRegexp(ValueError, 'Label should not be empty.'):  
      program.add_node(mock.Mock(autospec=base.Node), label='')

  def test_add_group_with_empty_label(self):
    program = lp_program.Program('foo')
    with self.assertRaisesRegexp(ValueError, 'Label should not be empty.'):  
      with program.group(''):
        program.add_node(mock.Mock(autospec=base.Node))

  def test_use_label_in_node_and_group(self):
    program = lp_program.Program('foo')
    with self.assertRaisesRegexp(  
        ValueError, 'label does not match the current group'):
      with program.group('foo'):
        program.add_node(mock.Mock(autospec=base.Node), label='bar')

    # If the label matches the current group, then it's ok.
    with program.group('foo'):
      program.add_node(mock.Mock(autospec=base.Node), label='foo')

  def test_nested_groups(self):
    program = lp_program.Program('foo')
    with self.assertRaisesRegexp(ValueError, r'group\(\) cannot be nested.'):  
      with program.group('foo'):
        with program.group('bar'):
          program.add_node(mock.Mock(autospec=base.Node))

  def test_duplicated_label(self):
    program = lp_program.Program('foo')
    node1 = mock.Mock(autospec=base.Node)
    node2 = mock.Mock(autospec=base.Node)
    program.add_node(node1, 'foo')
    program.add_node(node2, 'foo')
    with program.group('bar'):
      program.add_node(node1)
    with program.group('bar'):
      program.add_node(node2)
    self.assertEqual(program.groups['foo'], [node1, node2])
    self.assertEqual(program.groups['bar'], [node1, node2])

  def test_get_all_nodes(self):
    nodes = []
    program = lp_program.Program('test')
    node_foo = mock.Mock(autospec=base.Node)
    nodes.append(node_foo)
    program.add_node(node_foo, 'foo')

    node_bar = mock.Mock(autospec=base.Node)
    nodes.append(node_bar)
    program.add_node(node_bar, 'bar')

    with program.group('baz'):
      node_baz = mock.Mock(autospec=base.Node)
      nodes.append(node_baz)
      program.add_node(node_baz)
    self.assertCountEqual(program.get_all_nodes(), nodes)



if __name__ == '__main__':
  absltest.main()
