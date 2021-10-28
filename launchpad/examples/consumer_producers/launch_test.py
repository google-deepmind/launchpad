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

"""Launchpad integration test for the consumer_producers example."""

from absl.testing import absltest
import launchpad as lp
from launchpad.examples.consumer_producers.program import make_program


class LaunchTest(absltest.TestCase):

  def test_consumer_steps(self):
    """Runs the program and makes sure the consumer can run 10 steps."""
    program = make_program(num_producers=2)

    # Retrieve the consumer node from the program. Nodes are organized as a
    # mapping of label->nodes, stored as a dict in `program.groups`
    (consumer_node,) = program.groups['consumer']
    # Disable the automatic execution of its `run()` method.
    consumer_node.disable_run()  # pytype: disable=attribute-error

    # Launch all workers declared by the program. Remember to set the launch
    # type here (test & multithreaded).
    lp.launch(program, launch_type='test_mt', test_case=self)

    # Dereference `consumer_node`'s courier handle explicitly to obtain courier
    # client of it.
    consumer = consumer_node.create_handle().dereference()

    # Success criteria for this integration test defined as consumer being
    # able to take 10 steps.
    for _ in range(10):
      consumer.step()


if __name__ == '__main__':
  absltest.main()
