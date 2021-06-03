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

"""Tests for launchpad.launch.test_multi_threading.launch."""

import threading
import time

from absl.testing import absltest
from launchpad import context
from launchpad import program as lp_program
from launchpad.launch.test_multi_threading import launch
from launchpad.nodes.python import node as python
from launchpad.program_stopper import program_stopper


def _block():
  while True:
    time.sleep(1)


def _stop(stopper):
  stopper()


class LaunchTest(absltest.TestCase):

  def test_one_py_node_program(self):
    has_run = threading.Event()

    def run():
      has_run.set()

    program = lp_program.Program('test')
    program.add_node(python.PyNode(run), label='run')
    launch.launch(program, test_case=self)
    has_run.wait()

  def test_handle_exception(self):
    def run():
      raise RuntimeError('Launchpad has stopped working')

    program = lp_program.Program('test')
    program.add_node(python.PyNode(run), label='run')

    with self.assertRaisesRegex(RuntimeError, 'Launchpad has stopped working'):
      waiter = launch.launch(program, test_case=self)
      waiter.wait()

  def test_program_stopper(self):
    # This verifies the program stopper works for test_multi_threading
    p = lp_program.Program('test')

    with p.group('block'):
      p.add_node(python.PyNode(_block))

    with p.group('stop'):
      p.add_node(python.PyNode(_stop, program_stopper.make_program_stopper(
          context.LaunchType.TEST_MULTI_THREADING)))

    threads = launch.launch(p, test_case=self)
    threads.wait()

  def test_cleanup(self):
    # Test verifies that test cleanup works.
    p = lp_program.Program('test')

    with p.group('block'):
      p.add_node(python.PyNode(_block))

    launch.launch(p, test_case=self)


if __name__ == '__main__':
  absltest.main()
