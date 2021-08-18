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

"""Launch program in different ways based on FLAGS.lp_launch_type."""

import sys
import typing
from typing import Any, Dict, Mapping, Optional, Sequence, Union, cast

from absl import flags
from absl import logging
from absl.testing import absltest
from launchpad import context
from launchpad import flags as lp_flags  
from launchpad import program as lp_program
from launchpad.launch.local_multi_processing import launch as launch_local_multiprocessed
from launchpad.launch.local_multi_threading import launch as launch_local_multithreaded
from launchpad.launch.test_multi_processing import launch as launch_test_multiprocessed
from launchpad.launch.test_multi_threading import launch as launch_test_multithreaded

FLAGS = flags.FLAGS





def launch(
    programs: Union[lp_program.Program, Sequence[lp_program.Program],
                   ],
    launch_type: Optional[Union[context.LaunchType, str]] = None,
    local_resources: Optional[Dict[str, Any]] = None,
    test_case: Optional[absltest.TestCase] = None,
    terminal: Optional[str] = None,
) -> Any:
  """Launches a Launchpad program.

  Args:
    programs: One or more programs to launch, or a Controller instance to launch
      a workflow.
    launch_type: Type of launch. If this is None it will read from
      FLAGS.lp_launch_type. See the definition of context.LaunchType for the
      valid choices. The benefit of setting it to None is you can control the
      launch type from command line (by just passing --lp_launch_type=...).
    local_resources: (for local/test multiprocessing launch) A dictionary to
      specify per-node launch configuration.
    test_case: (for test multiprocessing launch) test case in which the program
      is launched.
    terminal: (for local multiprocessing launch) Terminal to use to run the
      commands. Valid choices are gnome-terminal, gnome-terminal-tabs, xterm,
      tmux_session, current_terminal, and output_to_files.

  Returns:
    Anything returns from the specific launcher.
  """

  # Make sure that flags are parsed before launching the program. Not all users
  # parse the flags.
  if not FLAGS.is_parsed():
    FLAGS(sys.argv, known_only=True)

  launch_type = launch_type or FLAGS.lp_launch_type
  if isinstance(launch_type, str):
    launch_type = context.LaunchType(launch_type)


  if not isinstance(programs, Sequence):
    programs = cast(Sequence[lp_program.Program], [programs])

  if len(programs) > 1:
    writer = print
    writer(
        'Multiple programs are provided but launch type is {}. Launching only '
        'the first program...'.format(launch_type))
  program = programs[0]

  if launch_type is context.LaunchType.LOCAL_MULTI_THREADING:
    return launch_local_multithreaded.launch(program)
  elif launch_type is context.LaunchType.LOCAL_MULTI_PROCESSING:
    return launch_local_multiprocessed.launch(program, local_resources,
                                              terminal)
  elif launch_type is context.LaunchType.TEST_MULTI_THREADING:
    return launch_test_multithreaded.launch(program, test_case=test_case)
  elif launch_type is context.LaunchType.TEST_MULTI_PROCESSING:
    assert test_case is not None
    return launch_test_multiprocessed.launch(
        program, test_case=test_case, local_resources=local_resources)
  else:
    logging.fatal('Unknown launch type: %s', launch_type)
