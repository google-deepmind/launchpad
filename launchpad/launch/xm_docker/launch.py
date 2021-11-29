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

import collections
import hashlib
from typing import Any, Dict, Optional

from absl import logging
from google.auth import exceptions as google_auth_exceptions
from launchpad import context
from launchpad import program as lp_program
from launchpad.launch import signal_handling
import termcolor
from xmanager import xm
from xmanager import xm_local


def launch(program: lp_program.Program,
           launch_type: context.LaunchType,
           xm_resources: Optional[Dict[str, Any]] = None):
  """Launches a program using local docker containers via XManager."""
  # Set up the launch context (launch type & launch config) for all nodes
  xm_resources = xm_resources or {}
  for label, nodes in program.groups.items():
    for node in nodes:
      if label in xm_resources:
        launch_config = xm_resources[label]
      else:
        # If launch config not specified, try to use a default.
        launch_config = node.default_launch_config(launch_type)
        xm_resources[label] = launch_config
      node._initialize_context(  
          launch_type, launch_config=launch_config)

  # Notify the input handles
  for label, nodes in program.groups.items():
    for node in nodes:
      for handle in node.input_handles:
        handle.connect(node, label)

  # Vertex AI supports only 4 worker pools, so we group nodes with the same
  # requirements.
  nodes_by_container = collections.defaultdict(list)
  for label, nodes in program.groups.items():
    launch_config = nodes[0].launch_context.launch_config
    hash_value = hashlib.md5()
    hash_value.update((launch_config.code_directory).encode())
    hash_value.update((launch_config.docker_requirements).encode())
    hash_value.update(str(launch_config.hw_requirements).encode())
    nodes_by_container[hash_value.hexdigest()].extend([
        (node, label) for node in nodes
    ])

  # Vertex AI requires the first worker pool to have exactly 1 replica...
  nodes_for_jobs = list(nodes_by_container.values())
  for index, nodes in enumerate(nodes_for_jobs):
    if len(nodes) == 1:
      nodes_for_jobs = [nodes_for_jobs[
          index]] + nodes_for_jobs[:index] + nodes_for_jobs[index + 1:]
      break
  if len(nodes_for_jobs[0]) != 1:
    nodes_for_jobs.append(nodes_for_jobs[0][1:])
    nodes_for_jobs[0] = [nodes_for_jobs[0][0]]

  # Make sure there are at most 4 worker pools (required by Vertex AI).
  cluster_names = ['workerpool0', 'workerpool1', 'workerpool2', 'workerpool3']
  if len(nodes_for_jobs) > len(cluster_names):
    raise RuntimeError((
        'Too many nodes with different requirements specified.'
        f'Vertex AI supports up to {len(cluster_names)} types.'
    ))

  # Bind addresses
  for index, nodes_with_labels in enumerate(nodes_for_jobs):
    for instance, (node, label) in enumerate(nodes_with_labels):
      node.bind_addresses(cluster=cluster_names[index], instance=instance)

  containers = []
  for index, nodes_with_labels in enumerate(nodes_for_jobs):
    nodes = [node for (node, label) in nodes_with_labels]
    # find the container

    # to_executables() is a static method, so we can call it from any of the
    # nodes in this group.
    # pytype: disable=wrong-arg-count

    # this is to_docker_executables based on LaunchType
    docker_executables = nodes[0].to_executables(nodes, cluster_names[index],
                                                 nodes[0].launch_context)
    assert len(docker_executables) == 1
    containers.append(docker_executables[0])
    # pytype: enable=wrong-arg-count

  signal_handling.exit_gracefully_on_sigint()
  signal_handling.exit_gracefully_on_sigquit()

  with xm_local.create_experiment(experiment_title=program.name) as experiment:
    jobs = {}
    job_id = 0
    for executable_spec, requirements in containers:
      if launch_type == context.LaunchType.VERTEX_AI:
        executor = xm_local.Caip(requirements=requirements)
        executor_spec = xm_local.Caip.Spec()
      else:
        logging.fatal('Unknown launch type: %s', launch_type)

      try:
        [executable] = experiment.package([
            xm.Packageable(
                executable_spec=executable_spec,
                executor_spec=executor_spec
            ),
        ])
      except google_auth_exceptions.DefaultCredentialsError:
        raise google_auth_exceptions.DefaultCredentialsError(
            'GCP project seems not to be configured correctly. Please follow'
            ' instructions at '
            'https://github.com/deepmind/xmanager#create-a-gcp-project.')

      job_id += 1
      jobs[str(job_id)] = xm.Job(executable=executable, executor=executor)
    experiment.add(xm.JobGroup(**jobs))

  print(termcolor.colored('Program launched successfully.', 'blue'))
  print(
      termcolor.colored('Node names mapping used in Vertex AI runtime:',
                        'blue'))
  # Print nodes' labels mapping to the worker pool names.
  def _name_range(name: str, start_idx: int, count: int):
    if count == 1:
      return f'{name}-{start_idx}'
    return f'{name}-[{start_idx}:{start_idx+count}]'

  node_index = collections.defaultdict(int)
  for cluster_index, nodes in enumerate(nodes_for_jobs):
    node_count = 0
    for i, (node, label) in enumerate(nodes):
      node_count += 1
      if i == len(nodes) - 1 or label != nodes[i+1][1]:
        start_idx = node_index[label]
        label_range = _name_range(label, start_idx, node_count)
        worker_range = _name_range(cluster_names[cluster_index],
                                   i - node_count + 1, node_count)
        print(termcolor.colored(f'{label_range} -> {worker_range}', 'blue'))
        node_index[label] += node_count
        node_count = 0
