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

"""Signal handling utilities for use when running LaunchPad interactively."""

import atexit
import signal
import sys

from typing import Callable

tty_write = print


def _exit_gracefully(signum, unused_frame):
  if signum == signal.SIGINT:
    # Ignore subsequent SIGINTs in order to prevent the exit handlers from being
    # interrupted during cleanup if CTRL+C is pressed multiple times.
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    tty_write('Control-C pressed. Exiting ...\n')
  elif signum == signal.SIGQUIT:
    tty_write('SIGQUIT received. Exiting ...\n')
  sys.exit(0)


def exit_gracefully_on_sigint():
  signal.signal(signal.SIGINT, _exit_gracefully)


def exit_gracefully_on_sigquit():
  signal.signal(signal.SIGQUIT, _exit_gracefully)


def register_exit_handler(handler: Callable[..., None], *args, **kargs):
  """Register an exit handler."""
  return atexit.register(handler, *args, **kargs)


def register_exit_handler_and_exit_gracefully_on_sigint(handler: Callable[...,
                                                                          None],
                                                        *args, **kargs):
  """Register an exit handler and gracefully handle SIGINT."""
  exit_handler = register_exit_handler(handler, *args, **kargs)
  exit_gracefully_on_sigint()
  return exit_handler
