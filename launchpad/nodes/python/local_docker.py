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
import dataclasses
from distutils import dir_util
import os
import pathlib
import shutil
import tempfile
from typing import Any, List, Optional, Sequence

import cloudpickle
from xmanager import xm

_DATA_FILE_NAME = 'job.pkl'


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
  """
  code_directory: Optional[str] = None
  docker_requirements: Optional[str] = None


def to_docker_executables(
    nodes: Sequence[Any],
    docker_config: DockerConfig,
    base_image: str = 'python:3.9'
) -> List[xm.PythonContainer]:
  """Returns a list of `PythonContainer`s objects for the given `PyNode`s."""

  if docker_config.code_directory is None or docker_config.docker_requirements is None:
    raise ValueError(
        'code_directory and docker_requirements must be specified through'
        'DockerConfig via local_resources when using "local_docker" launch'
        'type.')

  # Generate tmp dir without '_' in the name, CAIP fails otherwise.
  tmp_dir = '_'
  while '_' in tmp_dir:
    tmp_dir = tempfile.mkdtemp()
  atexit.register(shutil.rmtree, tmp_dir, ignore_errors=True)

  data_file_path = pathlib.Path(tmp_dir, _DATA_FILE_NAME)
  with open(data_file_path, 'wb') as f:
    cloudpickle.dump([node.function for node in nodes], f)

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

  return [xm.PythonContainer(
      path=tmp_dir,
      base_image=base_image,
      entrypoint=xm.CommandList(
          [f'python -m process_entry --data_file={_DATA_FILE_NAME}']),
      docker_instructions=[
          'RUN apt-get update && apt-get install -y git',
          'RUN python -m pip install --upgrade pip',
          'RUN apt-get -y install libpython3.9',
          f'COPY {workdir_path}/requirements.txt requirements.txt',
          'RUN python -m pip install xmanager',

          'RUN python -m pip install -r requirements.txt',
          f'COPY {workdir_path}/ {workdir_path}',
          f'WORKDIR {workdir_path}',
      ])]
