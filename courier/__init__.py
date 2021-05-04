from ctypes import cdll
import pkgutil
import os
tf = pkgutil.get_loader("tensorflow")
if tf:
  cdll.LoadLibrary(os.path.join(os.path.dirname(tf.path), 'libtensorflow_framework.so.2'))  # pytype:disable=attribute-error
del tf
del cdll
del pkgutil
del os
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

"""Courier module."""
from courier.python.client import Client  # pytype: disable=import-error
from courier.python.client import list_methods  # pytype: disable=import-error
from courier.python.py_server import Server  # pytype: disable=import-error
