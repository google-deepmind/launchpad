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

"""Redirect lp.stop() to both versions of WorkerManager."""
from typing import Optional

from launchpad import flags as lp_flags
from launchpad.launch import worker_manager
from launchpad.launch import worker_manager_v2


def wait_for_stop(timeout_secs: Optional[float] = None):
  if lp_flags.LP_WORKER_MANAGER_V2.value:
    worker_manager_v2.wait_for_stop(timeout_secs)
  else:
    worker_manager.wait_for_stop(timeout_secs)
