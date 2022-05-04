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

"""This example introduces basic notions in Launchpad."""



from absl import app
from absl import flags
import launchpad as lp
from launchpad.examples.consumer_producers.program import make_program

_NUM_PRODUCERS = flags.DEFINE_integer('num_producers', 2,
                                      'The number of concurrent producers.')


def main(argv):
  if len(argv) > 1:
    raise app.UsageError('Too many command-line arguments.')

  # Define a program which describes the topology of communicating nodes and
  # edges. In more involved examples, several programs can be defined and
  # launched at once.
  program = make_program(num_producers=_NUM_PRODUCERS.value)

  # Note that at launch time, none of the producers has been instantiated.
  # Producers are instantiated only at runtime.
  lp.launch(program)  


if __name__ == '__main__':
  app.run(main)
