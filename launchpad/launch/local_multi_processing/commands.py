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

"""Commands to run for multiple processes."""

from typing import Any, Mapping, List


class Command(object):

  def __init__(self, command_as_list: List[str],
               env_overrides: Mapping[str, Any], title: str):
    self.command_as_list = command_as_list
    self.env_overrides = env_overrides or {}
    self.title = title
