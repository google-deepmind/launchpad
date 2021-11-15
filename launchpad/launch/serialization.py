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

"""A utility to serialize nodes and raise an error if they are not serializable.


For the launch configurations that use this feature, one can test them using:

```
from launchpad.launch import serialization_test

class SerializationTest(serialization_test.ErrorOnSerializationMixin):

  @property
  def _launch(self):
    return launch.launch
```
"""

from absl import flags
import cloudpickle
from launchpad import flags as lp_flags  

FLAGS = flags.FLAGS


def check_nodes_are_serializable(label, nodes):
  try:
    cloudpickle.dumps([node.function for node in nodes])
  except Exception as e:
    raise RuntimeError(
        f"The nodes associated to the label '{label}' were not serializable "
        "using cloudpickle. Make them pickable, or `serialize_py_nodes=False` "
        "to `lp.launch` if you want to disable this check. "
    ) from e
