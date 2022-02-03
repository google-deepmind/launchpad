# Getting started with Launchpad

## Implement nodes

The setup in this example is as follows.
LaunchPad program launches one consumer and a number of producers to perform
some time consuming task. Consumer sends requests to producers and waits for
work completion. Afterwards it summarizes the work.

![consumer-producer-topology](https://github.com/deepmind/launchpad/raw/master/docs/images/consumer_producers.png)

The standard imports and flag setup are as follows.
The `num_producers` flag defined here can be used to control the number of
producers.

```python
from absl import app
from absl import flags
from absl import logging
import launchpad as lp

FLAGS = flags.FLAGS
flags.DEFINE_integer('num_producers', 2, 'The number of concurrent producers.')
```

The interface for the producer has just one method which performs some work (for
you to implement) in a given context provided by the caller. Any method of the
class can be exposed for other nodes to call by wrapping a node in a
**CourierNode**. In a typical setup, all nodes live in separate processes or on
distinct machines, while the communication between the nodes is taken care of
transparently by Launchpad. Some care has to be taken though. For
example, the `work()` method may be called from multiple threads within the same
process, so if the producer were to have any shared state then access to it must
be made thread-safe. In this case, the producer is stateless so it is not a
concern.

```python
class Producer:
  def work(self, context):
    return context
```

The consumer defines an initializer and a `run()` method. The initializer takes
a list of handles to the producers (**CourierNode**s).

Any Launchpad **PyClassNode** with a `run()` method will have that method called
automatically upon program entry. Here the `run()` method simply calls `work()`
on each producer and collects the results. At the end, it calls
`launchpad.stop()` to terminate all nodes running within a program.

```python
class Consumer:
  def __init__(self, producers):
    self._producers = producers

  def run(self):
    results = [producer.work(context)
               for context, producer in enumerate(self._producers)]
    logging.info('Results: %s', results)
    lp.stop()
```

In the example above, `work()` methods are called sequentially, so there is no
benefit in running this program distributed. Launchpad, however, allows for
asynchronous calls as well through the use of futures. In the example below
all producers will perform their work in parallel while consumer waits
on all of their results when calling `future.result()`. Note that futures are
only supported by the **CourierNode**.

```python
class Consumer:
  def __init__(self, producers):
    self._producers = producers

  def run(self):
    futures = [producer.futures.work(context)
               for context, producer in enumerate(self._producers)]
    results = [future.result() for future in futures]
    logging.info('Results: %s', results)
    lp.stop()
```

## Define topology

The next step is to instantiate nodes for the consumer and producers and then
connect them so that the consumer can call methods on the producers. The
connections between nodes define the topology of the distributed program.

Launchpad uses an `lp.Program` class to hold all the nodes. There are several
different types of nodes but here `lp.CourierNode` is used since it is the
simplest type which supports communication between nodes. The parameters to
`lp.CourierNode` are the name of the class and the parameters of its
initializer. Connecting the consumer node to the producer nodes is as simple as
passing in handles to all producers in the initializer of the consumer.
The handles themselves are returned by `lp.Program.add_node()`.

```python
def make_program(num_producers):
  program = lp.Program('consumer_producers')
  with program.group('producer'):
    producers = [
        program.add_node(lp.CourierNode(Producer)) for _ in range(num_producers)
    ]
  node = lp.CourierNode(
      Consumer,
      producers=producers)
  program.add_node(node, label='consumer')
  return program
```

With the above function defining the topology all that remains is to implement
`main()` for Launchpad:

```python
def main(_):
  program = make_program(num_producers=FLAGS.num_producers)
  lp.launch(program)

if __name__ == '__main__':
  app.run(main)
```

## Launch

To launch the program (assuming it is called `launch.py`), simply run:

```sh
python3 -m launch --lp_launch_type=local_mp
```

The `--lp_launch_type` controls how the program is launched. In the above case
it is launched locally with each node executed in a separate process.
List of supported execution modes can be found
[here](<https://github.com/deepmind/launchpad/search?q="class%20LaunchType">).

## Add a test

Here are some points to keep in mind when creating a test for a Launchpad
program.

*   The easiest way to add a test for your program is to reuse the same topology
    for an integration test (i.e. call `make_program()` from above in
    this example).
*   Launch a test by calling `lp.launch()` just like in `main()` in the above
    example, but explicitly specify `launch_type='test_mt'` (multithreaded
    tests) or `launch_type='test_mp'` (multiprocess tests) as a parameter.
*   It is possible to disable automatic execution of a node's `run()` method
    before launching. Do so by calling `disable_run()` on the node in question.
*   In order to call methods to test on a Courier node you will need to
    explicitly dereference the handle of the node first. Do so by calling
    `create_handle()` followed by `dereference()` on the node in question.


Below is an incomplete example illustrating the above concepts. A complete
example can be found [here](https://github.com/deepmind/launchpad/raw/master/launchpad/examples/consumer_producers/launch_test.py).

```python
import launchpad as lp
from launchpad.examples.consumer_producers import launch
from absl.testing import absltest

class LaunchTest(absltest.TestCase):
  def test_consumer(self):
    program = launch.make_program(num_producers=2)
    (consumer_node,) = program.groups['consumer']
    consumer_node.disable_run()
    lp.launch(program, launch_type='test_mt')
    consumer = consumer_node.create_handle().dereference()
    # Perform actual test here by calling methods on `consumer` ...
```
