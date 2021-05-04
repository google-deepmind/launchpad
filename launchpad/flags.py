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

"""Module containing all Launchpad flags."""

from absl import flags
from launchpad import context
FLAGS = flags.FLAGS

# For inferring flag types only.
flags.DEFINE_string('lp_dummy_str', '', 'Internal implementation details.')
flags.DEFINE_float('lp_dummy_float', 0., 'Internal implementation details.')
flags.DEFINE_integer('lp_dummy_int', 0, 'Internal implementation details.')
flags.DEFINE_boolean('lp_dummy_bool', False, 'Internal implementation details.')
flags.DEFINE_list('lp_dummy_list', [], 'Internal implementation details.')
flags.DEFINE_enum('lp_dummy_enum', '', [''], 'Internal implementation details.')

_DEFAULT_LAUNCH_TYPE = context.LaunchType.LOCAL_MULTI_THREADING.value
flags.DEFINE_enum(
    'lp_launch_type',
    _DEFAULT_LAUNCH_TYPE, [t.value for t in context.LaunchType],
    'How to launch a Launchpad program when launch() is called',
    allow_override=True)
flags.DEFINE_string('tmux_open_window', None,
                    'Window in new Tmux session to switch to.')
flags.DEFINE_string('tmux_session_name', 'launchpad',
                    'Prefix session name to use for the Tmux session.')

