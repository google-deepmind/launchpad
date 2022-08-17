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

"""Launchpad is a tool to define a distributed topology."""





from launchpad import lazy_loader

# Basic types
from launchpad import flags
from launchpad.context import LaunchType
from launchpad.context import is_local_launch
from launchpad.context import is_local_launch_or_test
from launchpad.flags import LAUNCH_TYPE  # Set via --lp_launch_type
from launchpad.program import Program


# Launch function
from launchpad.launch.launch import launch

# Nodes
from launchpad.nodes.courier.node import CourierHandle
from launchpad.nodes.courier.node import CourierNode
from launchpad.nodes.courier.node import CourierClient
from launchpad.nodes.multi_threading_colocation.node import MultiThreadingColocation
from launchpad.nodes.python.node import PyClassNode
from launchpad.nodes.python.node import PyNode
# Addressing
from launchpad.address import Address
from launchpad.address import AbstractAddressBuilder
from launchpad.address import get_port_from_address
from launchpad.address import SimpleLocalAddressBuilder

# Stopping a program
from launchpad.launch.worker_manager_migration import register_stop_handler
from launchpad.launch.worker_manager_migration import stop_event
from launchpad.launch.worker_manager_migration import unregister_stop_handler
from launchpad.launch.worker_manager_migration import wait_for_stop
from launchpad.program_stopper.program_stopper import make_program_stopper
from launchpad.stop_program.stop import stop

with lazy_loader.LazyImports(__name__, False):
  from launchpad.nodes.reverb.node import ReverbNode
  from launchpad.nodes.python.xm_docker import DockerConfig
  from launchpad.nodes.courier.courier_utils import batched_handler
