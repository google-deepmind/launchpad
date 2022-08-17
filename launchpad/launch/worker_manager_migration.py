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

"""Utilities to redirect calls to both versions of WorkerManager."""
from typing import Optional, Union

from launchpad import flags as lp_flags
from launchpad.launch import worker_manager
from launchpad.launch import worker_manager_v2


def wait_for_stop(timeout_secs: Optional[float] = None):
  if lp_flags.LP_WORKER_MANAGER_V2.value:
    worker_manager_v2.wait_for_stop(timeout_secs)
  else:
    worker_manager.wait_for_stop(timeout_secs)


def get_worker_manager(
) -> Union[worker_manager.WorkerManager, worker_manager_v2.WorkerManager]:
  if lp_flags.LP_WORKER_MANAGER_V2.value:
    return worker_manager_v2.get_worker_manager()
  else:
    return worker_manager.get_worker_manager()


def register_stop_handler(handler):
  if lp_flags.LP_WORKER_MANAGER_V2.value:
    worker_manager_v2.get_worker_manager().register_stop_handler(handler)
  else:
    return worker_manager.register_stop_handler(handler)


def unregister_stop_handler(handler):
  if lp_flags.LP_WORKER_MANAGER_V2.value:
    worker_manager_v2.get_worker_manager().unregister_stop_handler(handler)
  else:
    return worker_manager.unregister_stop_handler(handler)


def stop_event():
  if lp_flags.LP_WORKER_MANAGER_V2.value:
    return worker_manager_v2.get_worker_manager().stop_event
  else:
    return worker_manager.get_worker_manager().stop_event()
