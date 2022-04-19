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

"""Build and installs dm-launchpad."""
import argparse
import codecs
import datetime
import fnmatch
import os
import sys

from setuptools import setup
from setuptools.command.install import install as InstallCommandBase
from setuptools.dist import Distribution
from version import launchpad_version

# Version dependencies for a release build.
TENSORFLOW_VERSION = 'tensorflow~=2.8.0'
REVEB_VERSION = 'dm-reverb==0.7.1'


class BinaryDistribution(Distribution):

  def has_ext_modules(self):
    return True


def find_files(pattern, root):
  """Return all the files matching pattern below root dir."""
  for dirpath, _, files in os.walk(root):
    for filename in fnmatch.filter(files, pattern):
      yield os.path.join(dirpath, filename)


class InstallCommand(InstallCommandBase):
  """Override the dir where the headers go."""

  def finalize_options(self):
    ret = super().finalize_options()
    # We need to set this manually because we are not using setuptools to
    # compile the shared libraries we are distributing.
    self.install_lib = self.install_platlib
    return ret


class SetupToolsHelper(object):
  """Helper to execute `setuptools.setup()`."""

  def __init__(self, release, tf_package, reverb_package):
    """Initialize ReleaseBuilder class.

    Args:
      release: True to do a release build. False for a nightly build.
      tf_package: Version of Tensorflow to depend on.
      reverb_package: Version of Reverb to depend on.
    """
    self.release = release
    self.tf_package = tf_package
    self.reverb_package = reverb_package

  def _get_version(self):
    """Returns the version and project name to associate with the build."""
    if self.release:
      project_name = 'dm-launchpad'
      version = launchpad_version.__rel_version__
    else:
      project_name = 'dm-launchpad-nightly'
      version = launchpad_version.__dev_version__
      version += datetime.datetime.now().strftime('%Y%m%d')

    return version, project_name

  def _get_required_packages(self):
    """Returns list of required packages."""
    required_packages = [
        'absl-py',
        'cloudpickle',
        'dm-tree',
        'grpcio',
        'mock',
        'portpicker',
        'protobuf',
        'psutil',
        'termcolor',
    ]
    return required_packages

  def _get_tensorflow_packages(self):
    """Returns packages needed to install Tensorflow."""
    return [self.tf_package]

  def _get_reverb_packages(self):
    """Returns packages needed to install Reverb."""
    return [self.reverb_package]

  def run_setup(self):
    # Builds the long description from the README.
    root_path = os.path.abspath(os.path.dirname(__file__))
    with codecs.open(
        os.path.join(root_path, 'README.md'), encoding='utf-8') as f:
      long_description = f.read()

    version, project_name = self._get_version()
    setup(
        name=project_name,
        version=version,
        description=('Launchpad is a library that simplifies writing '
                     'distributed programs and seamlessly launching them '
                     'on a range of supported platforms.'),
        long_description=long_description,
        long_description_content_type='text/markdown',
        author='DeepMind',
        author_email='DeepMind <no-reply@google.com>',
        url='https://github.com/deepmind/launchpad',
        license='Apache 2.0',
        packages=['courier', 'launchpad'],
        headers=list(find_files('*.proto', 'launchpad')),
        include_package_data=True,
        install_requires=self._get_required_packages(),
        extras_require={
            'tensorflow': self._get_tensorflow_packages(),
            'reverb': self._get_reverb_packages(),
            'xmanager': ['xmanager'],
        },
        distclass=BinaryDistribution,
        cmdclass={
            'install': InstallCommand,
        },
        python_requires='>=3',
        classifiers=[
            'Development Status :: 3 - Alpha',
            'Intended Audience :: Developers',
            'Intended Audience :: Education',
            'Intended Audience :: Science/Research',
            'License :: OSI Approved :: Apache Software License',
            'Programming Language :: Python :: 3.7',
            'Programming Language :: Python :: 3.8',
            'Programming Language :: Python :: 3.9',
            'Programming Language :: Python :: 3.10',
            'Topic :: Scientific/Engineering',
            'Topic :: Scientific/Engineering :: Mathematics',
            'Topic :: Scientific/Engineering :: Artificial Intelligence',
            'Topic :: Software Development',
            'Topic :: Software Development :: Libraries',
            'Topic :: Software Development :: Libraries :: Python Modules',
        ],
        keywords='deepmind reinforcement learning machine distributed',
    )


if __name__ == '__main__':
  # Hide argparse help so `setuptools.setup` help prints. This pattern is an
  # improvement over using `sys.argv` and then `sys.argv.remove`, which also
  # did not provide help about custom arguments.
  parser = argparse.ArgumentParser(add_help=False)
  parser.add_argument(
      '--release',
      action='store_true',
      default=False,
      help='Pass as true to do a release build.')
  parser.add_argument(
      '--tf_package',
      required=True,
      help='Version of Tensorflow to depend on.')
  parser.add_argument(
      '--reverb_package',
      required=True,
      help='Version of Reverb to depend on.')
  FLAGS, unparsed = parser.parse_known_args()
  # Go forward with only non-custom flags.
  sys.argv.clear()
  # Downstream `setuptools.setup` expects args to start at the second element.
  unparsed.insert(0, 'foo')
  sys.argv.extend(unparsed)
  setup_tools_helper = SetupToolsHelper(release=FLAGS.release,
                                        tf_package=FLAGS.tf_package,
                                        reverb_package=FLAGS.reverb_package)
  setup_tools_helper.run_setup()
