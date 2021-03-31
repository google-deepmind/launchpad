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

"""Courier utilities."""

from typing import Any, Callable, Text

from absl import logging

import courier


def _should_expose_method(func: Callable[..., Any], method_name: Text) -> bool:
  return (callable(func) and not method_name.startswith('_') and
          method_name != 'set_courier_server')


def make_courier_server(instance: Any, *courier_args,
                        **courier_kwargs) -> courier.Server:
  """Builds a courier.Server for an instance.


  Args:
    instance: The instance that the courier server associates with.
    *courier_args: positional arguments to pass to courier.Server().
    **courier_kwargs: keyword arguments to pass to courier.Server().

  Returns:
    A courier.Server object.
  """
  server = courier.Server(*courier_args, **courier_kwargs)

  # Bind all non-private user-defined local methods.
  for method_name in dir(instance):
    if method_name.startswith('_'):
      continue

    func = getattr(instance, method_name)
    logging.info('Binding: %s', method_name)
    if _should_expose_method(func, method_name):
      server.Bind(method_name, func)

  return server
