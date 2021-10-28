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

"""Implementation of the example producer-consumer program."""
from typing import List

from absl import logging
import launchpad as lp


class Consumer:
  """A simple consumer that calls producers to perform some work."""

  def __init__(
      self,
      producers: List[lp.CourierClient],
  ) -> None:
    """Initializes a Consumer.

    Args:
      producers: a list of Producer handles.
    """
    self._producers = producers

  def run(self) -> None:
    """Entry point of the consumer."""
    # As a toy example we run 10 steps to interact with producers. Typically,
    # this would be replaced with an infinite loop or a loop with some stopping
    # criterion.
    for _ in range(10):
      self.step()

    # Stop the whole program (consumer and producers). Simply returning here
    # would stop the consumer but not the producers.
    lp.stop()

  def step(self) -> None:
    """Tells all the producers to perform one step of work."""
    # Call the producers to asynchronously produce work given a dummy context
    # represented by a counter.
    futures = [
        producer.futures.work(context)
        for context, producer in enumerate(self._producers)
    ]

    # Block to gather the results of all the producers.
    results = [future.result() for future in futures]
    logging.info('Results: %s', results)


class Producer:
  """A bare-bones producer."""

  def work(self, context: int) -> int:
    # Add code here to perform work. Note that this method can be called in
    # multiple threads because of the use of Courier futures, and so it has to
    # be thread safe! In this example the producer is stateless, so thread
    # safety is not a concern.
    return context


def make_program(num_producers: int) -> lp.Program:
  """Define the distributed program topology."""
  program = lp.Program('consumer_producers')

  # Use `program.group()` to group homogeneous nodes.
  with program.group('producer'):
    # Add a `CourierNode` to the program. `lp.CourierNode()` takes the producer
    # constructor and its arguments, and exposes it as an RPC server.
    # `program.add_node(lp.CourierNode(...))` returns a handle to this server.
    # These handles can then be passed to other nodes.
    producers = [
        program.add_node(lp.CourierNode(Producer)) for _ in range(num_producers)
    ]

  # Launch a single consumer that connects to the list of producers.
  # Note: The use of `label` here actually creates a group with one single node.
  node = lp.CourierNode(Consumer, producers=producers)
  program.add_node(node, label='consumer')

  return program
