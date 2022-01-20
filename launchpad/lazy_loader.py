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

"""LazyLoader providing support for loading symbols upon first reference."""

import importlib
import sys
import typing

# Set to true in your local workspace to work around issues with lazy imports.
# Please report any cases of usage to stanczyk@, so we can address them.
_DISABLE_LAZY_IMPORTS = False


class LazyImports():
  """Turns all imports performed in its scope into lazy.

  It is used to avoid pulling in large dependencies.
  """

  def __init__(self, parent, add_to_globals=True):
    """Initializes LazyImports.

    Args:
      parent: Name of the parent module doing the imports.
      add_to_globals: Whether imported symbols should be added to importing
          module globals. Rule of thumb is to not add for __init__ modules,
          as it is important for these modules to resolve Lazy symbol upon its
          first access (not when accessing members of the Lazy import).
    """
    self._parent = sys.modules[parent]
    self._globals = vars(self._parent)
    self._add_to_globals = add_to_globals
    self._symbols = dict()

  def parent(self):
    return self._parent

  def _find_and_load(self, name, import_):
    del import_
    return _ModuleToLoad(name, self)

  def _handle_fromlist(self, module, fromlist, import_):
    del import_
    assert len(fromlist) == 1
    if not isinstance(module, _ModuleToLoad):
      return _ModuleToLoad(module.__name__, self)
    return module

  def __enter__(self):
    if typing.TYPE_CHECKING or _DISABLE_LAZY_IMPORTS:
      return
    # Start capturing import statements.
    self._org_find_and_load = importlib._bootstrap._find_and_load
    self._org_handle_fromlist = importlib._bootstrap._handle_fromlist
    importlib._bootstrap._find_and_load = self._find_and_load
    importlib._bootstrap._handle_fromlist = self._handle_fromlist

  def __exit__(self, type_, value, traceback):
    if typing.TYPE_CHECKING or _DISABLE_LAZY_IMPORTS:
      return
    # Stop capturing import statements.
    importlib._bootstrap._find_and_load = self._org_find_and_load
    importlib._bootstrap._handle_fromlist = self._org_handle_fromlist

    # If symbols are not added to globals then __getattr__ has to be patched.
    if not self._add_to_globals:
      self._parent.__getattr__ = self.__getattr__

    # Make sure lazy symbols know their local name within the importing module.
    for name in list(self._globals.keys()):
      member = self._globals[name]
      if isinstance(member, _MemberToLoad):
        member.set_export_name(name)
        if not self._add_to_globals:
          del self._globals[name]
          self._symbols[name] = member

  def __getattr__(self, name):
    if name in self._symbols:
      res = self._symbols[name].load()
      self._parent.__dict__[name] = res
      return res
    raise AttributeError(f'module {self._parent!r} has no attribute {name!r}')


class _MemberToLoad():
  """Module member to load in a lazy way."""

  def __init__(self, name, parent):
    self._name = name
    self._export_name = name
    self._parent = parent

  def __getattr__(self, item):
    return getattr(self.load(), item)

  def __call__(self, *args, **kwargs):
    return self.load()(*args, **kwargs)

  def load(self):
    try:
      res = importlib.import_module(f'{self._parent.name()}.{self._name}')
    except ModuleNotFoundError:
      res = importlib.import_module(self._parent.name()).__dict__[self._name]

    self._parent.parent().parent().__dict__[self._export_name] = res
    return res

  def set_export_name(self, name):
    self._export_name = name


class _ModuleToLoad():
  """Module to load in a lazy way."""

  def __init__(self, name, parent):
    self._name = name
    self._parent = parent

  def name(self):
    return self._name

  def parent(self):
    return self._parent

  def __getattr__(self, item):
    return _MemberToLoad(item, self)
