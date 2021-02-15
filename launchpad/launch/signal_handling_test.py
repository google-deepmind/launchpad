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

"""Tests for launchpad.launch.signal_handling."""

import os
import signal

from absl.testing import absltest
from launchpad.launch import signal_handling
import mock
from six.moves import range


class SignalHandlingTest(absltest.TestCase):

  @mock.patch('sys.exit')
  def test_can_intercept_sigint(self, exit_mock):
    signal_handling.exit_gracefully_on_sigint()
    os.kill(os.getpid(), signal.SIGINT)
    exit_mock.assert_called_once_with(0)

  @mock.patch('sys.exit')
  def test_can_intercept_sigquit(self, exit_mock):
    signal_handling.exit_gracefully_on_sigquit()
    os.kill(os.getpid(), signal.SIGQUIT)
    exit_mock.assert_called_once_with(0)

  @mock.patch('sys.exit')
  def test_ignore_multiple_sigints(self, exit_mock):
    signal_handling.exit_gracefully_on_sigint()
    for _ in range(100):
      os.kill(os.getpid(), signal.SIGINT)
    exit_mock.assert_called_once_with(0)


if __name__ == '__main__':
  absltest.main()
