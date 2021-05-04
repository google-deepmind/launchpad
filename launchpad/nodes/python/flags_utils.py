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

"""Utilities for command line flags."""

import sys
from typing import Any, Dict, List

from absl import flags

from launchpad import flags as lp_flags  


FLAGS = flags.FLAGS


def _flag_to_placeholder(key):
  """Maps flag_type to a value so that entry script can add flag definition."""
  flag_type = FLAGS[key].flag_type()
  flag_type_to_placeholder = {
      FLAGS['lp_dummy_str'].flag_type(): FLAGS.lp_dummy_str,
      FLAGS['lp_dummy_float'].flag_type(): FLAGS.lp_dummy_float,
      FLAGS['lp_dummy_int'].flag_type(): FLAGS.lp_dummy_int,
      FLAGS['lp_dummy_bool'].flag_type(): FLAGS.lp_dummy_bool,
      FLAGS['lp_dummy_list'].flag_type(): FLAGS.lp_dummy_list,
      FLAGS['lp_dummy_enum'].flag_type(): FLAGS.lp_dummy_enum,
      FLAGS['lp_dummy_config'].flag_type(): {},
  }

  if flag_type not in flag_type_to_placeholder:
    raise NotImplementedError(f'Flag type {flag_type} is not yet supported.')
  placeholder = flag_type_to_placeholder[flag_type]
  return placeholder


def _maybe_parse_jax_flags():
  if 'jax' in sys.modules and not sys.modules['jax'].config.use_absl:
    # JAX doesn't add absl flags by default. Making them visible in flags.FLAGS
    # allows us to figure out the correct flag types.
    sys.modules['jax'].config.config_with_absl()


def get_flags_to_populate(args: List[Any]) -> Dict[str, Any]:
  """Finds flags that require definitions for the default entry script."""
  _maybe_parse_jax_flags()
  flags_to_populate = {}
  for arg in args:
    if not isinstance(arg, tuple):
      continue
    key, _ = arg
    # Additional C++ flags, possibly linked only in the worker processes.
    if key not in FLAGS or FLAGS[key].flag_type() == '[C++]':
      continue
    flags_to_populate[key] = _flag_to_placeholder(key)
  return flags_to_populate
