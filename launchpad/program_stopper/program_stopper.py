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

"""Stops a Launchpad program."""

import os
import signal
from typing import Union

from absl import logging

from launchpad import context


def make_program_stopper(launch_type: Union[str, context.LaunchType]):
  """Returns a callable that stops the Launchpad program.

  Args:
    launch_type: launch_type with which the program stopper is used.

  Returns:
    A callable. When called, it stops the running program.
  """
  launch_type = context.LaunchType(launch_type)

  if launch_type in [
      context.LaunchType.LOCAL_MULTI_PROCESSING,
      context.LaunchType.LOCAL_MULTI_THREADING,
      context.LaunchType.TEST_MULTI_PROCESSING,
      context.LaunchType.TEST_MULTI_THREADING
  ]:
    launcher_process_id = os.getpid()

    def ask_launcher_for_termination(mark_as_completed=False):
      del mark_as_completed
      os.kill(launcher_process_id, signal.SIGTERM)

    return ask_launcher_for_termination

  raise NotImplementedError(f'{launch_type} is not yet supported.')
