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

"""Tests for launchpad.launch.xm_docker.launch."""
import os

from absl.testing import absltest
import launchpad as lp
from launchpad.nodes.python import xm_docker


class HelloWorld:

  def __init__(self) -> None:
    pass

  def run(self) -> None:
    print('Hello World!!!')


def make_program() -> lp.Program:
  program = lp.Program('hello_world')

  node = lp.PyClassNode(HelloWorld)
  program.add_node(node, label='hello_printer')
  return program


class LaunchTest(absltest.TestCase):

  def test_launch(self):
    os.environ['GOOGLE_CLOUD_BUCKET_NAME'] = 'lpbucket'
    launchpad_dir = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    docker_requirements = os.path.join(
        launchpad_dir, 'examples/consumer_producers/requirements.txt')
    docker_config = xm_docker.DockerConfig(launchpad_dir, docker_requirements)
    resources = {'hello_printer': docker_config}

    program = make_program()

    # Launch should fail accessing GCP.
    exception_msg = (
        'Request failed|404|The specified bucket does not exist|CP project '
        'seems not to be configured correctly'
        # We allow digit-only messages, assuming this is a KeyError as reported
        # in b/241629570. Should be fixed together with
        # https://github.com/python/cpython/issues/91351.
        r'|\d+')
    with self.assertRaisesRegex(Exception, exception_msg):
      lp.launch(
          program,
          launch_type=lp.LaunchType.VERTEX_AI,
          xm_resources=resources)


if __name__ == '__main__':
  absltest.main()
