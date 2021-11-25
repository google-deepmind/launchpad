## FAQ

### How do I specify per-node flags or environment variables?

As Launchpad aims at supporting different types of runtimes (including
multi-threaded), it is not possible to specify per-node flags / environment
variables in all cases. For that reason it is recommended to use nodes'
parameters whether possible. In cases when this is not doable, you will have to
specify the overrides per each runtime used. For example, in case of a
multi-process runtime:

```
# local_resources is the resource dictionary (mapping nodes' group names to resources) for local multiprocessing launch.
# lp.PythonProcess is the corresponding local multiprocessing launch config.
local_resources = dict(
    actor=lp.PythonProcess(args=dict(foo='bar'),
                          interpreter_args=dict(bar='baz')
                          env=dict(ENV_VARIABLE='value'))
)
lp.launch(..., local_resources=local_resources)
```

### How do I configure two CourierNodes aware of each other?

Create the first node, then the second passing the first handle, and finally
update the first node with the second node handle, e.g.

```
class FirstNode:

  def __init__(self, second_node_handle = None):
    self._second_node_handle = _second_node_handle


class SecondNode:

  def __init__(self, first_node_handle):
    self._first_node_handle = first_node_handle

first_node = lp.CourierNode(FirstNode)
second_node = lp.CourierNode(SecondNode, first_node.create_handle())
# pylint: disable=protected-access
first_node._kwargs["second_node_handle"] = second_node.create_handle()
# pylint: enable=protected-access
```

### How to perform clean program termination?

Launchpad provides a mechanism to communicate experiment termination between
program's nodes. The node which wants to terminate execution of the program has
to call `lp.stop()` method. All other nodes should periodically check experiment
termination condition by calling `lp.wait_for_stop(timeout_in_seconds)`. This
function blocks execution for a given period of time or until program
termination was requested, in which case `True` is returned. In case program is
executed on a local machine, pressing `CTRL+C` also results in program
termination. Example usage:

```
def actor():
  while not lp.wait_for_stop(0):
    # Execute actor's step.
    ...
  # Do necessary cleanup.
  ...

def learner():
  for learner_step in range(1000):
    # Perform training step.
    ...
  lp.stop()

def backgroun_task():
  # Start background task.
  ...
  lp.wait_for_stop()
  # Stop background task.
  ...
```
