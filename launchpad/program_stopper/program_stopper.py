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
      context.LaunchType.TEST_MULTI_THREADING,
      context.LaunchType.TEST_MULTI_PROCESSING
  ]:

    raise NotImplementedError(
        'Program stoppers for test launch types are not yet supported! '
        'Please mock lp.make_program_stopper().')

  if launch_type is context.LaunchType.LOCAL_MULTI_PROCESSING:
    launcher_process_id = os.getpid()

    def shut_down_local_process_launcher(unused_mark_as_completed=False):
      os.kill(launcher_process_id, signal.SIGINT)

    return shut_down_local_process_launcher

  if launch_type is context.LaunchType.LOCAL_MULTI_THREADING:

    def shut_down_process(unused_mark_as_completed=False):
      os.kill(os.getpid(), signal.SIGQUIT)

    return shut_down_process

  raise NotImplementedError(f'{launch_type} is not yet supported.')
