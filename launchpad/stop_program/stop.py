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

"""Stop a running Launchpad program."""

from absl import flags

from launchpad import context
from launchpad.launch import worker_manager

FLAGS = flags.FLAGS


def stop():
  """Terminates the entire experiment."""
  context.get_context().program_stopper()


def wait_for_stop():
  """Waits for program's termination/preemption signal."""
  worker_manager.get_worker_manager().wait_for_stop()
