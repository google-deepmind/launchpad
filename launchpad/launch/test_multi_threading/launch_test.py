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

import os
import threading
from unittest import mock

from absl.testing import absltest
from launchpad import program as lp_program
from launchpad.launch.test_multi_threading import launch
from launchpad.nodes.python import node as python


class LaunchTest(absltest.TestCase):

  def test_one_py_node_program(self):
    has_run = threading.Event()

    def run():
      has_run.set()

    program = lp_program.Program('test')
    program.add_node(python.PyNode(run), label='run')
    launch.launch(program)
    has_run.wait()

  def test_handle_exception(self):
    def run():
      raise RuntimeError('Launchpad has stopped working')

    program = lp_program.Program('test')
    program.add_node(python.PyNode(run), label='run')

    # Mock os.kill() so as to make sure it is called.
    has_run = threading.Event()
    def mock_kill(pid, unused_signal):
      self.assertEqual(pid, os.getpid())
      has_run.set()
    with mock.patch.object(os, 'kill', mock_kill):
      launch.launch(program)
      has_run.wait()


if __name__ == '__main__':
  absltest.main()
