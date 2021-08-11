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

"""A node wrapper to colocate multiple PyNodes.

See docstring of MultiProcessingColocation for usage.
"""

import atexit
import collections
from concurrent import futures
import itertools
import os
import shutil
import subprocess
import tempfile
from typing import Any

from absl import flags
from absl import logging

import cloudpickle

from launchpad import address as lp_address
from launchpad.nodes.python import node as python

HandleType = Any


class _ResolvedAddressBuilder(lp_address.AbstractAddressBuilder):

  def __init__(self, resolved_address):
    self._resolved_address = resolved_address

  def build(self):
    return self._resolved_address


class MultiProcessingColocation(python.PyNode):
  """A special node type for colocating multiple PyNodes as subprocesses.

  Please don't add inner nodes to the program, as they will become part of this
  colocation node.

  Example:

      actor_nodes = []
      for _ in range(10):
        actor_nodes.append(lp.CourierNode(Actor, ...))
      program.add_node(lp.MultiProcessingColocation(actor_nodes))
  """

  def __init__(self, nodes, num_retries_on_failure: int = 0):
    super().__init__(self.run)
    self._nodes = []
    self._name_uniquifier = collections.defaultdict(itertools.count)
    self._num_retries_on_failure = num_retries_on_failure
    self._num_restarts = 0
    for node in nodes:
      self.add_node(node)

  def add_node(self, node: python.PyNode) -> HandleType:
    if not isinstance(node, python.PyNode):
      raise ValueError('MultiProcessingColocation only works with PyNodes.')
    self._nodes.append(node)
    # Take over the addresses of the node.
    for address in node.addresses:
      self.allocate_address(address)
      # Ensure unique address names (avoid name clash when creating named ports)
      address.name = address.name or 'lp'  # Name might not be specified
      unique_id = str(next(self._name_uniquifier[address.name]))
      address.name = address.name + unique_id
    return node.create_handle()

  @property
  def nodes(self):
    return self._nodes

  def run(self):
    if not self._nodes:
      raise ValueError('MultiProcessingColocation requires at least one node.')

    for address in self.addresses:
      address._address_builder = _ResolvedAddressBuilder(address.resolve())  

    running_processes = []
    with futures.ThreadPoolExecutor(max_workers=len(self._nodes)) as e:
      for node in self._nodes:

        running_processes.append(e.submit(self._run_subprocess, node.function))

    done, _ = futures.wait(
        running_processes, return_when=futures.FIRST_EXCEPTION)
    for f in done:
      f.result()

  def _run_subprocess(self, function):
    _, data_file_path = tempfile.mkstemp()
    with open(data_file_path, 'wb') as f:
      cloudpickle.dump([function], f)
      atexit.register(os.remove, data_file_path)
    subprocess_env = {}
    subprocess_env.update(os.environ)
    while True:
      temp_dir = tempfile.mkdtemp()
      subprocess_env['TMPDIR'] = temp_dir
      entry_script_path = os.path.join(
          os.path.dirname(__file__), 'process_entry.py')
      process = subprocess.Popen([
          os.environ['_'],
          entry_script_path,
          '--',
          '--lp_task_id',
          '0',
          '--data_file',
          data_file_path,
      ],
                                 env=subprocess_env)
      exit_code = process.wait()
      shutil.rmtree(temp_dir)

      if exit_code == 0:
        return
      if self._num_restarts == self._num_retries_on_failure:
        raise RuntimeError('num_retries_on_failure (=%d) is reached.' %
                           self._num_retries_on_failure)
      logging.info('Subprocess %d exited abnormally! Restarting.', process.pid)
      self._num_restarts += 1
