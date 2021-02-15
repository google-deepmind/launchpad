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

"""This example is the simplest possible Launchpad program."""


from absl import app

import launchpad as lp


class HelloWorld:
  """A node that prints hello world and exits."""

  def __init__(self) -> None:
    """Initializes Hello World."""
    pass

  def run(self) -> None:
    """Entry point."""
    print('Hello World!!!')


def make_program() -> lp.Program:
  """Define the program topology."""
  program = lp.Program('hello_world')

  node = lp.PyClassNode(HelloWorld)
  program.add_node(node, label='hello_printer')
  return program


def main(_):
  program = make_program()
  lp.launch(program)


if __name__ == '__main__':
  app.run(main)
