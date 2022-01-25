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
import threading

from typing import Any, Callable, Optional, Union


class LaunchType(enum.Enum):
  """The different launch types supported by Launchpad.

  Launch type can be specified through `lp_launch_type` command line flag or
  by passing `launch_type` parameter to lp.launch() call.
  """
  # Launch locally using multiple threads logging on the same terminal with
  # different colors. Upon crash it drops into PDB for the
  # thread that crashed, and locks output from all other threads.
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
  # Launch locally using docker containers, similar to local multi processing.
  # NOTE: Experimental, do not use.
  LOCAL_DOCKER = 'local_docker'
  # Launch on Google Cloud using Vertex AI (https://cloud.google.com/vertex-ai)
  # throught xmanager. For an example on how to use VERTEX_AI launch, please
  # refer to Launchpad's example:
  # https://github.com/deepmind/launchpad/tree/master/launchpad/examples/consumer_producers/launch_vertex_ai.py
  # It is also worth looking at RL agents examples from Acme, for instance:
  # https://github.com/deepmind/acme/tree/master/examples/gym/lp_d4pg.py
  # NOTE: Using this runtime involves prior GCP project configuration.
  # Please follow the steps described at
  # https://github.com/deepmind/xmanager#create-a-gcp-project.
  VERTEX_AI = 'vertex_ai'


class LaunchContext(object):
  """Stores platform-specific launch config of a node.

  This is created and set on the node only at launch time.
  """

  def __init__(self):
    self._launch_type = None
    self._launch_config = None
    self._program_stopper = None
    self._is_initialized = False

  @property
  def launch_type(self) -> LaunchType:
    self._check_inititialized()
    return self._launch_type

  @property
  def launch_config(self) -> Any:
    self._check_inititialized()
    return self._launch_config

  @property
  def program_stopper(self) -> Callable[[], None]:
    self._check_inititialized()
    return self._program_stopper

  def _check_inititialized(self):
    if not self._is_initialized:
      raise RuntimeError(
          'Launch context is not yet initialized. It should be initialized by '
          'calling initialize() at launch time.')

  def initialize(self, launch_type: LaunchType, launch_config: Any,
                 program_stopper: Optional[Callable[[], None]] = None):
    self._launch_config = launch_config
    self._launch_type = launch_type
    self._program_stopper = program_stopper
    self._is_initialized = True


_LAUNCH_CONTEXT = threading.local()


def get_context():
  context = getattr(_LAUNCH_CONTEXT, 'lp_context', None)
  assert context, ("Launchpad context was not instantiated. Do you try to "
                   "access it outside of the main node's thread?")
  return context


def set_context(context: LaunchContext):
  _LAUNCH_CONTEXT.lp_context = context


def is_local_launch(launch_type: Union[LaunchType, str]) -> bool:
  """Returns true if launch type is local multithreading/multiprocessing.

  If you use `--lp_launch_type=...`, please call it with
  `is_local_launch(lp.LAUNCH_TYPE.value)`, where lp.LAUNCH_TYPE.value gives the
  value of `--lp_launch_type`.

  Args:
    launch_type: A string (e.g., 'local_mp') or a LaunchType object.

  Returns:
    True if launch_type is a local one, otherwise False.
  """
  if isinstance(launch_type, str):
    launch_type = LaunchType(launch_type)
  return launch_type in [
      LaunchType.LOCAL_MULTI_THREADING, LaunchType.LOCAL_MULTI_PROCESSING
  ]


def is_local_launch_or_test(launch_type: Union[LaunchType, str]) -> bool:
  """Returns true if launch type is local/test multithreading/multiprocessing.

  If you use `--lp_launch_type=...`, please call it with
  `is_local_launch_or_test(lp.LAUNCH_TYPE.value)`, where lp.LAUNCH_TYPE.value
  gives the value of `--lp_launch_type`.

  Args:
    launch_type: A string (e.g., 'local_mp') or a LaunchType object.

  Returns:
    True if launch_type is local or unit test, otherwise False.
  """
  if isinstance(launch_type, str):
    launch_type = LaunchType(launch_type)
  return launch_type in [
      LaunchType.LOCAL_MULTI_THREADING, LaunchType.LOCAL_MULTI_PROCESSING,
      LaunchType.TEST_MULTI_PROCESSING, LaunchType.TEST_MULTI_THREADING
  ]
