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

"""Utilities to run PyNodes in Docker containers using XManager."""

import atexit
import copy
import dataclasses
from distutils import dir_util
import functools
import os
import pathlib
import shutil
import sys
import tempfile
from typing import Any, List, Optional, Sequence, Tuple

import cloudpickle
from launchpad.launch import serialization

try:
  from xmanager import xm  
except ModuleNotFoundError:
  raise Exception('Launchpad requires `xmanager` for XM-based runtimes.'
                  'Please run `pip install xmanager`.')


_DATA_FILE_NAME = 'job.pkl'
_INIT_FILE_NAME = 'init.pkl'


@dataclasses.dataclass
class DockerConfig:
  """Local docker launch configuration.

  Attributes:
    code_directory: Path to directory containing any user code that may be
      required inside the Docker image. The user code from this directory is
      copied over into the Docker containers, as the user code may be needed
      during program execution. If needed, modify docker_instructions in
      xm.PythonContainer construction below if user code needs installation.
    docker_requirements: Path to requirements.txt specifying Python packages to
      install inside the Docker image.
    hw_requirements: Hardware requirements.
    python_path: Additional paths to be added to PYTHONPATH prior to executing
      an entry point.
  """
  code_directory: Optional[str] = None
  docker_requirements: Optional[str] = None
  hw_requirements: Optional[xm.JobRequirements] = None
  python_path: Optional[List[str]] = None


def initializer(python_path):
  sys.path = python_path + sys.path


def to_docker_executables(
    nodes: Sequence[Any],
    label: str,
    docker_config: DockerConfig,
) -> List[Tuple[xm.PythonContainer, xm.JobRequirements]]:

  """Returns a list of `PythonContainer`s objects for the given `PyNode`s."""

  if docker_config.code_directory is None or docker_config.docker_requirements is None:
    raise ValueError(
        'code_directory and docker_requirements must be specified through'
        'DockerConfig via local_resources when using "xm_docker" launch type.')

  # Generate tmp dir without '_' in the name, Vertex AI fails otherwise.
  tmp_dir = '_'
  while '_' in tmp_dir:
    tmp_dir = tempfile.mkdtemp()
  atexit.register(shutil.rmtree, tmp_dir, ignore_errors=True)

  command_line = f'python -m process_entry --data_file={_DATA_FILE_NAME}'

  # Add common initialization function for all nodes which sets up PYTHONPATH.
  if docker_config.python_path:
    command_line += f' --init_file={_INIT_FILE_NAME}'
    # Local 'path' is copied under 'tmp_dir' (no /tmp prefix) inside Docker.
    python_path = [
        '/' + os.path.basename(tmp_dir) + os.path.abspath(path)
        for path in docker_config.python_path
    ]
    initializer_file_path = pathlib.Path(tmp_dir, _INIT_FILE_NAME)
    with open(initializer_file_path, 'wb') as f:
      cloudpickle.dump(functools.partial(initializer, python_path), f)

  data_file_path = str(pathlib.Path(tmp_dir, _DATA_FILE_NAME))
  serialization.serialize_functions(data_file_path, label,
                                    [n.function for n in nodes])

  file_path = pathlib.Path(__file__).absolute()

  shutil.copy(pathlib.Path(file_path.parent, 'process_entry.py'), tmp_dir)
  dir_util.copy_tree(docker_config.code_directory, tmp_dir)
  shutil.copy(docker_config.docker_requirements,
              pathlib.Path(tmp_dir, 'requirements.txt'))

  workdir_path = pathlib.Path(tmp_dir).name

  if not os.path.exists(docker_config.docker_requirements):
    raise FileNotFoundError('Please specify a path to a file with Python'
                            'package requirements through'
                            'docker_config.docker_requirements.')
  job_requirements = docker_config.hw_requirements
  if not job_requirements:
    job_requirements = xm.JobRequirements()

  # Make a copy of requirements since they are being mutated below.
  job_requirements = copy.deepcopy(job_requirements)

  if job_requirements.replicas != 1:
    raise ValueError(
        'Number of replicas is computed by the runtime. '
        'Please do not set it explicitly in the requirements.'
    )

  job_requirements.replicas = len(nodes)
  python_version = f'{sys.version_info.major}.{sys.version_info.minor}'
  base_image = f'python:{python_version}'
  return [(xm.PythonContainer(
      path=tmp_dir,
      base_image=base_image,
      entrypoint=xm.CommandList([command_line]),
      docker_instructions=[
          'RUN apt-get update && apt-get install -y git',
          'RUN python -m pip install --upgrade pip',
          f'RUN apt-get -y install libpython{python_version}',
          f'COPY {workdir_path}/requirements.txt requirements.txt',
          'RUN python -m pip install xmanager',
          'RUN python -m pip install -r requirements.txt',
          f'COPY {workdir_path}/ {workdir_path}',
          f'WORKDIR {workdir_path}',
      ]), job_requirements)]
