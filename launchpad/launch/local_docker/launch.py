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

"""Launches a Launchpad program using local docker containers via XManager."""

from typing import Any, Mapping, Optional
from absl import logging

from launchpad import context
from launchpad import program as lp_program
from launchpad.launch import signal_handling
from xmanager import xm
from xmanager import xm_local


def launch(program: lp_program.Program,
           launch_type: context.LaunchType,
           local_resources: Optional[Mapping[str, Any]] = None):
  """Launches a program using local docker containers via XManager."""
  # Set up the launch context (launch type & launch config) for all nodes
  local_resources = local_resources or {}
  for label, nodes in program.groups.items():
    launch_config = local_resources.get(label, None)
    for node in nodes:
      node._initialize_context(  
          launch_type, launch_config=launch_config)

  # Notify the input handles
  for label, nodes in program.groups.items():
    for node in nodes:
      for handle in node._input_handles:  
        handle.connect(node, label)

  # Bind addresses
  for node in program.get_all_nodes():
    node.bind_addresses()

  containers = []
  for label, nodes in program.groups.items():
    # to_executables() is a static method, so we can call it from any of the
    # nodes in this group.
    
    # pytype: disable=wrong-arg-count

    # this is to_docker_executables based on LaunchType
    docker_executables = nodes[0].to_executables(nodes, label,
                                                 nodes[0]._launch_context)
    containers.append(docker_executables)
    
    # pytype: enable=wrong-arg-count

  signal_handling.exit_gracefully_on_sigint()
  signal_handling.exit_gracefully_on_sigquit()

  if launch_type == context.LaunchType.LOCAL_DOCKER:
    executor = xm_local.Local()
    executor_spec = xm_local.Local.Spec()
  elif launch_type == context.LaunchType.CAIP:
    executor = xm_local.Caip(requirements=xm.JobRequirements(cpu=1))
    executor_spec = xm_local.Caip.Spec()
  else:
    logging.fatal('Unknown launch type: %s', launch_type)

  with xm_local.create_experiment(experiment_title=program.name) as experiment:
    jobs = {}
    job_id = 0
    for docker_executables in containers:
      for spec in docker_executables:
        [executable] = experiment.package([
            xm.Packageable(
                executable_spec=spec,
                executor_spec=executor_spec
            ),
        ])

        job_id += 1
        jobs[str(job_id)] = xm.Job(executable=executable, executor=executor)
    experiment.add(xm.JobGroup(**jobs))
  print('Launch completed and successful.')
