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

"""Tests for launchpad.nodes.multi_processing_colocation.node."""

import shutil
import subprocess
import tempfile
from unittest import mock

from absl.testing import absltest
from launchpad.nodes.multi_processing_colocation import node as multi_processing_colocation
from launchpad.nodes.python import node as python


def _run_no_op():
  pass


class NodeTest(absltest.TestCase):

  @mock.patch.object(tempfile, 'mkdtemp')
  @mock.patch.object(subprocess, 'Popen')
  @mock.patch.object(shutil, 'rmtree')
  def test_one_subprocess(self, mock_rmtree, mock_popen, mock_mkdtemp):
    # Verify cleanups are done correctly
    mock_mkdtemp.return_value = 'temp_dir'
    mock_process = mock.Mock()
    mock_popen.return_value = mock_process
    mock_process.wait.return_value = 0  # return normally
    colocation = multi_processing_colocation.MultiProcessingColocation(
        [python.PyNode(_run_no_op)])
    colocation.run()
    mock_mkdtemp.called_once_with()
    mock_popen.called_once()
    mock_rmtree.called_once_with('temp_dir')

  @mock.patch.object(tempfile, 'mkdtemp')
  @mock.patch.object(subprocess, 'Popen')
  @mock.patch.object(shutil, 'rmtree')
  def test_two_subprocesses(self, mock_rmtree, mock_popen, mock_mkdtemp):
    # Verify it works for more than one PyNode
    mock_mkdtemp.return_value = 'temp_dir'
    mock_process = mock.Mock()
    mock_popen.return_value = mock_process
    mock_process.wait.return_value = 0  # return normally
    colocation = multi_processing_colocation.MultiProcessingColocation(
        [python.PyNode(_run_no_op),
         python.PyNode(_run_no_op)])
    colocation.run()
    self.assertEqual(mock_mkdtemp.call_count, 2)
    self.assertEqual(mock_popen.call_count, 2)
    self.assertEqual(mock_rmtree.call_count, 2)

  @mock.patch.object(tempfile, 'mkdtemp')
  @mock.patch.object(subprocess, 'Popen')
  @mock.patch.object(shutil, 'rmtree')
  def test_retry_on_failure(self, mock_rmtree, mock_popen, mock_mkdtemp):
    # Verify it retries after subprocess failure
    mock_mkdtemp.return_value = 'temp_dir'
    mock_process = mock.Mock()
    mock_popen.return_value = mock_process
    mock_process.pid = 42

    def mock_wait():
      # First time fail, then succeed
      if mock_popen.call_count == 1:
        return 1
      return 0

    mock_process.wait = mock_wait
    colocation = multi_processing_colocation.MultiProcessingColocation(
        [python.PyNode(_run_no_op)], num_retries_on_failure=1)
    colocation.run()
    self.assertEqual(mock_mkdtemp.call_count, 2)
    self.assertEqual(mock_popen.call_count, 2)
    self.assertEqual(mock_rmtree.call_count, 2)

  @mock.patch.object(tempfile, 'mkdtemp')
  @mock.patch.object(subprocess, 'Popen')
  @mock.patch.object(shutil, 'rmtree')
  def test_fail_without_retry(self, mock_rmtree, mock_popen, mock_mkdtemp):
    mock_mkdtemp.return_value = 'temp_dir'
    mock_process = mock.Mock()
    mock_popen.return_value = mock_process
    mock_process.wait.return_value = 1  # return abnormally
    colocation = multi_processing_colocation.MultiProcessingColocation(
        [python.PyNode(_run_no_op)], num_retries_on_failure=0)
    with self.assertRaisesRegexp(  
        RuntimeError, r'num_retries_on_failure \(=0\) is reached.'):
      colocation.run()
    mock_mkdtemp.called_once_with()
    mock_popen.called_once()
    mock_rmtree.called_once_with('temp_dir')


if __name__ == '__main__':
  absltest.main()
