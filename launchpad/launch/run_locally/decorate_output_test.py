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

"""Tests for the `decorate_output` wrapper script."""

import os
import subprocess

from absl.testing import absltest


class DecorateOutputTest(absltest.TestCase):

  def test_annotates_each_line(self):
    decorate_output = os.path.dirname(__file__) + '/decorate_output'

    decorated_output = subprocess.check_output(
        [decorate_output, '33', 'my title', 'seq', '10', '10', '30'])

    self.assertListEqual(
        decorated_output.split(b'\n'),
        [
            b'\x1b[1;33m[my title] 10\x1b[0;0m',
            b'\x1b[1;33m[my title] 20\x1b[0;0m',
            b'\x1b[1;33m[my title] 30\x1b[0;0m',
            b'',
        ])


if __name__ == '__main__':
  absltest.main()
