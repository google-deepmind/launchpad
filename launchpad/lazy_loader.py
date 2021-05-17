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

import collections
import importlib
import sys
import types
import typing

ImportSymbol = collections.namedtuple('ImportSymbol', 'module member')

# Set to true in your local workspace to work around issues with lazy imports.
# Please report any cases of usage to stanczyk@, so we can address them.
_DISABLE_LAZY_IMPORTS = False


class _MemberToLoad():
  """Module member to load in a lazy way."""

  def __init__(self, name, parent):
    self._name = name
    self._parent = parent


class _ModuleToLoad():
  """Module to load in a lazy way."""

  def __init__(self, name, parent):
    self._name = name
    self._parent = parent

  def __getattr__(self, item):
    return _MemberToLoad(item, self)


class LazyModule(types.ModuleType):
  """Turns a given __init__ module into lazily importing specified symbols."""

  def __init__(self, parent):
    self._parent = parent
    self._symbols = dict()
    super(LazyModule, self).__init__(parent)
    self.__dict__.update(sys.modules[self._parent].__dict__)

  def _find_and_load(self, name, import_):
    del import_
    return _ModuleToLoad(name, self)

  def __enter__(self):
    if typing.TYPE_CHECKING or _DISABLE_LAZY_IMPORTS:
      return
    self._org_find_and_load = importlib._bootstrap._find_and_load
    # Start capturing import statements.
    importlib._bootstrap._find_and_load = self._find_and_load

  def __exit__(self, type_, value, traceback):
    if typing.TYPE_CHECKING or _DISABLE_LAZY_IMPORTS:
      return
    # Stop capturing import statements and register all lazy imports.
    importlib._bootstrap._find_and_load = self._org_find_and_load
    members = sys.modules[self._parent].__dict__
    for name in members:
      member = members[name]
      if isinstance(member, _MemberToLoad):
        self.add(name, member._parent._name, member._name)
    # Replace original module with LazyModule.
    sys.modules[self._parent] = self

  def add(self, local_name, module, member=None):
    symbol = ImportSymbol(module, member)
    self._symbols[local_name] = symbol
    if typing.TYPE_CHECKING or _DISABLE_LAZY_IMPORTS:
      return self.__getattr__(local_name)
    return symbol

  def __getattr__(self, name):
    if name in self._symbols:
      symbol = self._symbols[name]
      res = importlib.import_module(symbol.module)
      if symbol.member:
        res = res.__dict__[symbol.member]
      self.__dict__[name] = res
      return res
    raise AttributeError(f'module {self._parent!r} has no attribute {name!r}')

  def __reduce__(self):
    return self.__class__, (self._parent,)


class LazyImport(types.ModuleType):
  """Lazily import a module, mainly to avoid pulling in large dependencies."""

  def __init__(self, local_name, parent_module_globals, name):
    self._local_name = local_name
    self._parent_module_globals = parent_module_globals
    super(LazyImport, self).__init__(name)
    if _DISABLE_LAZY_IMPORTS:
      self._load()

  def _load(self):
    """Load the module and insert it into the parent's globals."""
    # Import the target module and insert it into the parent's namespace
    module = importlib.import_module(self.__name__)
    self._parent_module_globals[self._local_name] = module
    self.__dict__.update(module.__dict__)
    return module

  def __getattr__(self, item):
    module = self._load()
    return getattr(module, item)

  def __dir__(self):
    module = self._load()
    return dir(module)
