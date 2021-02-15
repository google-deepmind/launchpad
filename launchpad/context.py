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

"""Platform-specific configuration on the node."""

import enum

from typing import Any


class LaunchType(enum.Enum):
  """The different launch types supported by Launchpad."""
  LOCAL_MULTI_THREADING = 'local_mt'
  # Launch locally using multiple processes. Can display logs from different
  # nodes in separate windows. The behavior can be controlled using the
  # `terminal` argument in `launch.launch`.
  LOCAL_MULTI_PROCESSING = 'local_mp'
  # Launch using multiple processes, as a test.
  TEST_MULTI_PROCESSING = 'test_mp'
  # Launch as a test using multiple threads (same as LOCAL_MULTI_THREADING but
  # terminates the process instead of dropping into PDB).
  TEST_MULTI_THREADING = 'test_mt'


class LaunchContext(object):
  """Stores platform-specific launch config of a node.

  This is created and set on the node only at launch time.
  """

  def __init__(self):
    self._launch_type = None
    self._launch_config = None
    self._is_initialized = False

  @property
  def launch_type(self) -> LaunchType:
    self._check_inititialized()
    return self._launch_type

  @property
  def launch_config(self) -> Any:
    self._check_inititialized()
    return self._launch_config

  def _check_inititialized(self):
    if not self._is_initialized:
      raise RuntimeError(
          'Launch context is not yet initialized. It should be initialized by '
          'calling initialize() at launch time.')

  def initialize(self, launch_type: LaunchType, launch_config: Any):
    self._launch_config = launch_config
    self._launch_type = launch_type
    self._is_initialized = True
