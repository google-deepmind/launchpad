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

import functools
import os
import signal
import sys
from typing import Union

from absl import logging
from launchpad import context
from launchpad import flags as lp_flags
from launchpad.launch import worker_manager
from launchpad.launch import worker_manager_v2




def _stop_vertex_ai(mark_as_completed=False):
  del mark_as_completed
  from google.cloud import aiplatform  
  from google.api_core import exceptions  
  aiplatform.init(project=os.environ['CLOUD_ML_PROJECT_ID'])
  try:
    aiplatform.CustomJob.get(os.environ['CLOUD_ML_JOB_ID']).cancel()
  except exceptions.FailedPrecondition:
    # Experiment could have been already cancelled.
    pass


def _ask_launcher_for_termination(launcher_process_id, mark_as_completed=False):
  del mark_as_completed
  os.kill(launcher_process_id, signal.SIGTERM)


def make_program_stopper(launch_type: Union[str, context.LaunchType]):
  """Returns a callable that stops the Launchpad program.

  Args:
    launch_type: launch_type with which the program stopper is used.

  Returns:
    A callable. When called, it stops the running program.
  """
  launch_type = context.LaunchType(launch_type)

  def _stop_mt():
    worker_manager_v2.get_worker_manager().stop_event.set()

  if launch_type is context.LaunchType.TEST_MULTI_THREADING:
    if lp_flags.LP_WORKER_MANAGER_V2.value:
      return _stop_mt
    else:
      def _stop():
        worker_manager.get_worker_manager()._sigterm()  
      return _stop

  if (launch_type is context.LaunchType.LOCAL_MULTI_THREADING and
      lp_flags.LP_WORKER_MANAGER_V2.value):
    return _stop_mt

  if (launch_type in [
      context.LaunchType.LOCAL_MULTI_PROCESSING,
      context.LaunchType.TEST_MULTI_PROCESSING
  ] and lp_flags.LP_WORKER_MANAGER_V2.value):

    def _sigint_to_launcher(launcher_process_id, mark_as_completed=False):
      del mark_as_completed
      # Here we send a SIGINT to the launcher process for simplicity, but it
      # will bring down all programs launched from that process. A better
      # approach is to communicate to the launcher process through a unix pipe
      # (named after the launcher pid), so that the launcher process will only
      # kill subprocesses associated with the specific program.
      os.kill(launcher_process_id, signal.SIGINT)
    # In local_mp, we treat lp.stop() as a user-requested stop. The reason is
    # that it provides convenience in observing preemption handling logic (using
    # `lp.wait_for_stop()` or the stop event) being triggered locally.
    return functools.partial(_sigint_to_launcher, os.getpid())

  if launch_type in [
      context.LaunchType.LOCAL_MULTI_PROCESSING,
      context.LaunchType.LOCAL_MULTI_THREADING,
      context.LaunchType.TEST_MULTI_PROCESSING,
  ]:
    return functools.partial(_ask_launcher_for_termination, os.getpid())

  if launch_type in [context.LaunchType.VERTEX_AI]:
    return _stop_vertex_ai

  raise NotImplementedError(f'{launch_type} is not yet supported.')
