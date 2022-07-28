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

"""Delaying object constructions."""

import abc
from typing import Callable, Generic, TypeVar, Union

from absl import flags

import tree


T = TypeVar('T')

_LP_CATCH_DEFERRED_EXCEPTION = flags.DEFINE_boolean(
    'lp_catch_deferred_exception', True,
    'Whether exceptions raised by the constructors of deferred objects should '
    'be caught and annotated with the initiation stack of the object. A negative '
    'side effect of using this is that pdb-on-error will enter at the re-raise '
    'point rather than the source of the original exception.')


class _Uninitialized:
  pass


class Dereferenceable(Generic[T], abc.ABC):

  @abc.abstractmethod
  def dereference(self) -> T:
    """Dereferences itself."""


def maybe_dereference(obj: Union[T, Dereferenceable[T]]) -> T:
  if isinstance(obj, Dereferenceable):
    return obj.dereference()
  return obj


_EXCEPTION_MESSAGE = ('Error ({error_msg}) occurred when evaluating Deferred '
                      'defined at:\n{init_stack}\nNOTE! If you want pdb to '
                      'enter at raise point of the original exception, '
                      'please rerun with flag --nolp_catch_deferred_exception')


class Deferred(Dereferenceable[T], Generic[T]):
  """Delays object construction to achieve serializability.

  Assuming we want to pass a non-serializable Python object, say an environment
  object, to an Actor, the following will lead to a serialization error:

      program.add_node(lp.CourierNode(Actor, envloader.load_from_settings(
          platform='Atari',
          settings={
              'levelName': 'pong',
              'env_loader.version': requested_version,
              'zero_indexed_actions': True,
              'interleaved_pixels': True,
          })))

  This helper class delays the object construction and fixes this error. The
  object is constructed when the Actor is actually instantiated remotely, where
  the Actor constructed will receive an actual environment object (just like how
  handles are dereferenced automatically):

      program.add_node(lp.CourierNode(Actor, lp.Deferred(
          envloader.load_from_settings,
          platform='Atari',
          settings={
              'levelName': 'pong',
              'env_loader.version': requested_version,
              'zero_indexed_actions': True,
              'interleaved_pixels': True,
          })))
  """

  def __init__(self, constructor: Callable[..., T], *args, **kwargs) -> None:
    self._constructor = constructor
    self._args = args
    self._kwargs = kwargs
    self._init_stack = 'Stack trace missing'
    self._deferred_object = _Uninitialized()


  def dereference(self) -> T:
    if isinstance(self._deferred_object, _Uninitialized):
      args, kwargs = tree.map_structure(maybe_dereference,
                                        (self._args, self._kwargs))
      if not _LP_CATCH_DEFERRED_EXCEPTION.value:
        # Allows the user to pdb where the exception happens, but won't show
        # where the deferred object was originally defined.
        return self._constructor(*args, **kwargs)

      try:
        self._deferred_object = self._constructor(*args, **kwargs)
      except Exception as e:  
        new_message = _EXCEPTION_MESSAGE.format(
            init_stack=''.join(self._init_stack),
            # For clarity during pdb, we also inline the internal error message.
            error_msg=str(e))
        raise RuntimeError(new_message) from e

    return self._deferred_object

  def _apply_to_args(self, fn):
    return tree.map_structure(fn, (self._args, self._kwargs))
