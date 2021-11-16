# Launchpad
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/dm-launchpad)
[![PyPI version](https://badge.fury.io/py/dm-launchpad.svg)](https://badge.fury.io/py/dm-launchpad)

Launchpad is a library that simplifies writing distributed programs by
seamlessly launching them on a variety of different platforms. Switching between
local and distributed execution requires only a flag change.

Launchpad introduces a programming model that represents a distributed system
as a graph data structure (a **Program**) describing the system’s topology.
Each node in the program graph represents a service in the distributed system,
i.e. the fundamental unit of computation that we are interested in running.
As nodes are added to this graph, Launchpad constructs a handle for each of them.
A handle ultimately represents a client to the yet-to-be-constructed service.
A directed edge in the program graph, representing communication between
two services, is created when the handle associated with one node is given
to another at construction time. This edge originates from the receiving node,
indicating that the receiving node will be the one initiating communication.
This process allows Launchpad to define cross-service communication simply
by passing handles to nodes. Launchpad provides a number of node types,
including:

*   **PyNode** - a simple node executing provided Python code upon entry.
    It is similar to a main function, but with the distinction that
    each node may be running in separate processes and on different machines.
*   **CourierNode** - it enables cross-node communication. **CourierNodes** can
    communicate by calling public methods on each other either synchronously
    or asynchronously via futures. The underlying remote procedure calls
    are handled transparently by Launchpad.
*   **ReverbNode** - it exposes functionality of Reverb, an easy-to-use data
    storage and transport system primarily used by RL algorithms as
    an experience replay. You can read more about Reverb
    [here](https://github.com/deepmind/reverb).
*   **MultiThreadingColocation** - allows to colocate multiple other nodes in
    a single process.
*   **MultiProcessingColocation** - allows to colocate multiple other nodes as
    sub processes.

Using Launchpad involves writing nodes and defining the topology of your
distributed program by passing to each node references of the other nodes that
it can communicate with. The core data structure dealing with this is called a
**Launchpad program**, which can then be executed seamlessly with a number of
supported runtimes.

## Supported launch types
Launchpad supports a number of launch types, both for running programs on
a single machine, in a distributed manner, or in a form of a test. Launch type
can be controlled by the `launch_type` argument passed to `lp.launch` method,
or specified through the `--lp_launch_type` command line flag.
Please refer to the documentation of the [LaunchType](<https://github.com/deepmind/launchpad/search?q="class LaunchType">)
for details.

## Table of Contents

-   [Installation](#installation)
-   [Quick Start](docs/get_started.md)
-   [Citing Launchpad](#citing-Launchpad)
-   [Acknowledgements](#acknowledgements)
-   [Other resources](#other-resources)

## Installation

Please keep in mind that Launchpad is not hardened for production use, and while we
do our best to keep things in working order, things may break or segfault.

> :warning: Launchpad currently only supports Linux based OSes.

The recommended way to install Launchpad is with `pip`. We also provide
instructions to build from source using the same docker images we use for
releases.

TensorFlow can be installed separately or as part of the `pip` install.
Installing TensorFlow as part of the install ensures compatibility.

```shell
$ pip install dm-launchpad[tensorflow]

# Without Tensorflow install and version dependency check.
$ pip install dm-launchpad
```

### Nightly builds

[![PyPI version](https://badge.fury.io/py/dm-launchpad-nightly.svg)](https://badge.fury.io/py/dm-launchpad-nightly)

```shell
$ pip install dm-launchpad-nightly[tensorflow]

# Without Tensorflow install and version dependency check.
$ pip install dm-launchpad-nightly
```

Similarily, [Reverb](https://github.com/deepmind/reverb) can be installed
ensuring compatibility:

```shell
$ pip install dm-launchpad[reverb]
```

### Develop Launchpad inside a docker container

The most convenient way to develop Launchpad is with Docker.
This way you can compile and test Launchpad inside a container without
having to install anything on your host machine, while you can still
use your editor of choice for making code changes.
The steps are as follows.

Checkout Launchpad's source code from GitHub.
```
$ git checkout https://github.com/deepmind/launchpad.git
$ cd launchpad
```

Build the Docker container to be used for compiling and testing Launchpad.
You can specify `tensorflow_pip` parameter to set the version
of Tensorflow to build against. You can also specify which version(s) of Python
container should support. The command below enables support for Python
3.7, 3.8 and 3.9.
```
$ docker build --tag launchpad:devel \
  --build-arg tensorflow_pip=tensorflow==2.3.0 \
  --build-arg python_version="3.7 3.8 3.9" - < docker/build.dockerfile
```

The next step is to enter the built Docker image, binding checked out
Launchpad's sources to /tmp/launchpad within the container.
```
$ docker run --rm --mount "type=bind,src=$PWD,dst=/tmp/launchpad" \
  -it launchpad:devel bash
```

At this point you can build and install Launchpad within the container by
executing:
```
$ /tmp/launchpad/oss_build.sh
```

By default it builds Python 3.8 version, you can change that with `--python`
flag.
```
$ /tmp/launchpad/oss_build.sh --python 3.8
```

To make sure installation was successful and Launchpad works as expected, you
can run some examples provided:
```
$ python3.8 -m launchpad.examples.hello_world.launch
$ python3.8 -m launchpad.examples.consumer_producers.launch --lp_launch_type=local_mp
```

To make changes to Launchpad codebase, edit sources checked out from GitHub
directly on your host machine (outside of the Docker container). All changes are
visible inside the Docker container. To recompile just run the `oss_build.sh`
script again from the Docker container. In order to reduce compilation time of
the consecutive runs, make sure to not exit the Docker container.


## Citing Launchpad

If you use Launchpad in your work, please cite the accompanying
[technical report](https://arxiv.org/pdf/2106.04516):

```bibtex
@article{yang2021launchpad,
    title={Launchpad: A Programming Model for Distributed Machine Learning
           Research},
    author={Fan Yang and Gabriel Barth-Maron and Piotr Stańczyk and Matthew
            Hoffman and Siqi Liu and Manuel Kroiss and Aedan Pope and Alban
            Rrustemi},
    year={2021},
    journal={arXiv preprint arXiv:2106.04516},
    url={https://arxiv.org/abs/2106.04516},
}
```

## Acknowledgements

We greatly appreciate all the help from [Reverb](https://github.com/deepmind/reverb)
and [TF-Agents](https://github.com/tensorflow/agents) teams in setting
up building and testing setup for Launchpad.

## Other resources

*   [FAQ](docs/faq.md)
