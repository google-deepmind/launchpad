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

"""Tests for launchpad.launch.test_multi_processing.launch."""


from absl.testing import absltest
import launchpad as lp
from launchpad import program
from launchpad.launch.test_multi_processing import launch
from launchpad.nodes.python import local_multi_processing
from launchpad.nodes.python import node as python
from launchpad.program_stopper import program_stopper


def _get_default_py_node_config():
  return local_multi_processing.PythonProcess(
  )


def _noop():
  pass


def _fail():
  raise RuntimeError('Some error.')


def _block():
  while not lp.wait_for_stop(2):
    pass


def _stop(stopper):
  stopper()


class LaunchTest(absltest.TestCase):

  def test_wait_for_one(self):
    p = program.Program('test')

    p.add_node(python.PyNode(_noop), 'noop')
    resources = dict(noop=_get_default_py_node_config())
    processes = launch.launch(p, test_case=self, local_resources=resources)

    p_dict = processes._active_workers
    self.assertEqual(list(p_dict.keys()), ['noop'])
    self.assertLen(p_dict['noop'], 1)
    p_dict['noop'][0].wait()  # Wait until termination
    self.assertEqual(p_dict['noop'][0].returncode, 0)

  def test_wait_for_all(self):
    p = program.Program('test')

    p.add_node(python.PyNode(_noop), 'noop')
    resources = dict(noop=_get_default_py_node_config())
    processes = launch.launch(p, test_case=self, local_resources=resources)

    processes.wait()

  def test_wait_for_some_only_waits_for_specified_node_groups(self):
    p = program.Program('test')

    with p.group('main'):
      p.add_node(python.PyNode(_noop))
    with p.group('daemon'):
      p.add_node(python.PyNode(_block))
    resources = dict(
        main=_get_default_py_node_config(),
        daemon=_get_default_py_node_config())
    processes = launch.launch(p, test_case=self, local_resources=resources)

    processes.wait(['main'])

  def test_wait_for_some_detects_exception_in_any_node_group(self):
    p = program.Program('test')

    with p.group('main'):
      p.add_node(python.PyNode(_block))
    with p.group('daemon'):
      p.add_node(python.PyNode(_fail))
    resources = dict(
        main=_get_default_py_node_config(),
        daemon=_get_default_py_node_config())
    processes = launch.launch(p, test_case=self, local_resources=resources)

    with self.assertRaisesRegexp(  
        RuntimeError, 'One of the workers failed.'):
      processes.wait(['main'])


  def test_grouping(self):
    # This verifies the process handles are grouped correctly
    p = program.Program('test')

    with p.group('foo'):
      for _ in range(1):
        p.add_node(python.PyNode(_noop))

    with p.group('bar'):
      for _ in range(2):
        p.add_node(python.PyNode(_noop))

    resources = dict(
        foo=_get_default_py_node_config(), bar=_get_default_py_node_config())
    processes = launch.launch(p, test_case=self, local_resources=resources)
    p_dict = processes._active_workers
    self.assertCountEqual(list(p_dict.keys()), ['foo', 'bar'])
    self.assertLen(p_dict['foo'], 1)
    self.assertLen(p_dict['bar'], 2)

  def test_program_stopper(self):
    # This verifies the program stopper works for test_multi_processing
    p = program.Program('test')

    with p.group('block'):
      p.add_node(python.PyNode(_block))

    with p.group('stop'):
      p.add_node(python.PyNode(_stop, program_stopper.make_program_stopper(
          lp.context.LaunchType.TEST_MULTI_PROCESSING)))

    resources = dict(
        block=_get_default_py_node_config(), stop=_get_default_py_node_config())
    processes = launch.launch(p, test_case=self, local_resources=resources)
    processes.wait()

  def test_cleanup(self):
    # Test verifies that test cleanup works.
    p = program.Program('test')

    with p.group('block'):
      p.add_node(python.PyNode(_block))

    resources = dict(block=_get_default_py_node_config())
    launch.launch(p, test_case=self, local_resources=resources)


if __name__ == '__main__':
  absltest.main()
