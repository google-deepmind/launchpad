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

"""Ongoing refactoring for WorkerManager."""
import collections
from concurrent import futures
import dataclasses
import functools
import signal
import threading
import time
from typing import Any, Callable, Iterable, List, MutableMapping, Optional, Tuple

from absl import logging
from launchpad import flags as lp_flags
import psutil
import termcolor


_WORKER_MANAGERS = threading.local()


def _call_once(func):
  """Calls the function only once, regardless of arguments."""

  @functools.wraps(func)
  def _wrapper():
    # If we haven't been called yet, actually invoke func and save the result.
    if not _wrapper.has_run():
      _wrapper.mark_as_run()
      _wrapper.return_value = func()
    return _wrapper.return_value

  _wrapper._has_run = False  
  _wrapper.has_run = lambda: _wrapper._has_run  
  _wrapper.mark_as_run = lambda: setattr(_wrapper, '_has_run', True)
  return _wrapper


def _register_signal_handler(sig: signal.Signals, handler: Callable[[], None]):
  """Registers a signal handler."""
  # We only call the handler once.
  handler = _call_once(handler)
  old_handler = signal.getsignal(sig)

  def _run_handler(sig=sig,
                   frame=None,
                   handler=handler,
                   old_handler=old_handler):
    handler()
    if isinstance(old_handler, Callable):
      old_handler(sig, frame)

  signal.signal(sig, _run_handler)


def wait_for_stop(timeout_secs: Optional[float] = None):
  """Blocks until termination of the node's program starts or timeout passes.

  Args:
    timeout_secs: Floating point number specifying a timeout for the operation,
      in seconds. If not provided, timeout is infinite.

  Returns:
    True if program is being terminated, False if timeout was reached.

  Usage examples:
  - Perform cleanup at the end of the run:
    start_server()
    lp.wait_for_stop()
    stop_server()
    checkpoint()

  - Perform some work until program is terminated:
    while not lp.wait_for_stop(0): # Return immediately.
      ... do some work ...

  - Perform some task every 5 seconds:
    while not lp.wait_for_stop(5.0):
      ... perform periodic task ...
  """
  return get_worker_manager().wait_for_stop(timeout_secs)


def get_worker_manager():
  manager = getattr(_WORKER_MANAGERS, 'manager', None)
  if not manager:
    raise RuntimeError('Worker manager is only available from a PyNode thread.')
  return manager


@dataclasses.dataclass
class ThreadWorker:
  thread: threading.Thread
  future: futures.Future[Any]


def _get_child_processes_with_depth(process: psutil.Process,
                                    depth: int) -> Iterable[psutil.Process]:
  """Returns child processes at the given depth."""
  if depth == 0:
    return [process]
  if depth == 1:
    return process.children(recursive=False)

  children_at_depth = []
  for child in process.children(recursive=False):
    children_at_depth.extend(_get_child_processes_with_depth(child, depth - 1))
  return children_at_depth


def _send_signal_to_processes_with_depth(processes: Iterable[psutil.Process],
                                         sig: signal.Signals, depth: int):
  for process in processes:
    for child in _get_child_processes_with_depth(process, depth):
      child.send_signal(sig)


class WorkerManager:
  """Manages running threads and processes of a Launchpad Program."""

  def __init__(
      self,
      termination_notice_secs: Optional[int] = None,
      handle_user_stop: bool = False,
      handle_sigterm: bool = False,
      register_in_thread: bool = False,
      process_tree_depth: int = 0,
  ):
    """Initializes a WorkerManager.

    Args:
      termination_notice_secs: 1) when >0, it's the countdown before a SIGKILL
        is issued upon a user-requested stop (relies on handle_user_stop=True).
        2) when =0, SIGKILL happens immediately upon user-requested stop.
      handle_user_stop: Whether to handle Ctrl+C or not. This should be set to
        True in local_mt and local_mp, so that the user can stop the program
        with Ctrl+C. This will set the stop event, and also send SIGTERM to all
        subprocesses.
      handle_sigterm: When this is True, kill all workers upon SIGTERM, by 1)
        forwarding SIGTERM to process workers 2) setting stop event for thread
        workers. Set this to True in process_entry.py so that the stop event
        will be triggered in the subprocesses via SIGTERM.
      register_in_thread: Make the worker manager accessible through
        `get_worker_manager()` in the current thread (needed by `stop_event()`
        for example). It should be False if we don't need to access
        `get_worker_manager()` , e.g. at the launcher thread of local_mt and
        local_mp. It should be True for process_entry.py.
      process_tree_depth: the depth of Launchpad subprocesses in the process
        tree. For example, when the process is managed by GNOME, this value
        should be 2, so that in a tree of gnome-terminal -> bash -> interpreter
    """
    if termination_notice_secs is None:
      termination_notice_secs = lp_flags.LP_TERMINATION_NOTICE_SECS.value
    if termination_notice_secs < 0:
      raise ValueError('termination_notice_secs must be >= 0.')
    self._termination_notice_secs = termination_notice_secs
    if handle_user_stop:
      _register_signal_handler(signal.SIGINT, self._handle_user_stop)

    if handle_sigterm:
      _register_signal_handler(
          signal.SIGTERM, self._set_stop_event_and_terminate_process_workers)
    self._stop_event = threading.Event()
    self._thread_workers: MutableMapping[
        str, List[ThreadWorker]] = collections.defaultdict(list)
    self._process_workers: MutableMapping[
        str, List[psutil.Process]] = collections.defaultdict(list)
    self._mutex = threading.Lock()
    if register_in_thread:
      _WORKER_MANAGERS.manager = self
    self._process_tree_depth = process_tree_depth

  @property
  def stop_event(self):
    return self._stop_event

  def wait_for_stop(self, timeout_secs: Optional[float] = None):
    """Blocks until managed runtime is terminating or timeout is reached."""
    return self._stop_event.wait(timeout_secs)

  def thread_worker(self, name: str, function: Callable[[], Any]):
    """Registers and starts a new thread worker.

    Args:
      name: Name of the worker group.
      function: Entrypoint function to execute in a worker.
    """
    future = futures.Future()

    def run_inner(f=function, future=future, manager=self):
      _WORKER_MANAGERS.manager = manager
      try:
        future.set_result(f())
      except BaseException as e:  
        future.set_exception(e)

    builder = lambda t, n: threading.Thread(target=t, name=n)
    thread = builder(run_inner, name)
    thread.setDaemon(True)

    thread.start()
    with self._mutex:
      self._thread_workers[name].append(ThreadWorker(thread, future))

  def register_existing_process(self, name: str, pid: int):
    """Registers already started worker process.

    Args:
      name: Name of the workers' group.
      pid: Pid of the process to monitor.
    """
    with self._mutex:
      self._process_workers[name].append(psutil.Process(pid))

  def _has_active_workers(self):
    _, has_active_workers = self._update_and_get_recently_finished()
    return has_active_workers

  def _update_and_get_recently_finished(
      self) -> Tuple[List[futures.Future[Any]], bool]:
    """Update self._thread_workers and return a tuple representing the change.

    This will update self._thread_workers so that it only contains active
    workers.

    Returns:
      A tuple. The first element of the tuple are futures for recently finished
      workers, and the second is a bool indicating if there are still active
      workers.
    """
    recently_finished = []
    has_active_workers = False
    active_workers = collections.defaultdict(list)
    with self._mutex:
      for label in self._thread_workers:
        for worker in self._thread_workers[label]:
          if worker.thread.is_alive():
            active_workers[label].append(worker)
            has_active_workers = True
          else:
            recently_finished.append(worker.future)
      self._thread_workers = active_workers
      for _, processes in self._process_workers.items():
        for process in processes:
          if process.is_running():
            has_active_workers = True

            break
    return recently_finished, has_active_workers

  def check_for_thread_worker_exception(self):
    """Raises an error if there's an exception in one of the workers."""
    recently_finished, _ = self._update_and_get_recently_finished()
    for future in recently_finished:
      future.result()

  def wait(self):
    """Waits until all thread workers finish. Raises errors if any."""
    has_active_worker = True
    while has_active_worker:
      try:
        has_active_worker = False
        # Will raise errors, if any.
        self.check_for_thread_worker_exception()
        with self._mutex:
          # check_for_thread_worker_exception() will update self._thread_workers
          # so that it only contains active workers. If there are still
          # non-empty lists, it means some workers have not finished yet.
          for workers in self._thread_workers.values():
            if workers:
              has_active_worker = True
              break
        for _, processes in self._process_workers.items():
          for process in processes:
            if process.is_running():
              has_active_worker = True
              break
        time.sleep(0.1)
      except KeyboardInterrupt:
        pass

  def _set_stop_event_and_terminate_process_workers(self):
    self._stop_event.set()
    for _, processes in self._process_workers.items():
      _send_signal_to_processes_with_depth(processes, signal.SIGTERM,
                                           self._process_tree_depth)

  def _handle_user_stop(self):
    """Handles user-issued stop (Ctrl+C).

    This does the following:

    1. Set the stop event. Nodes can listen to the stop event and perform
       cleanup actions.
    2. Wait for termination_notice_secs (specified from   __init__()`), since
       the workers might need some time for cleanup.
    3. SIGKILL the remaining workers.
    """
    print(
        termcolor.colored('User-requested termination. Asking workers to stop.',
                          'blue'))
    # Notify all the thread workers.
    self._stop_event.set()

    # Notify all the process workers.
    for _, processes in self._process_workers.items():
      _send_signal_to_processes_with_depth(processes, signal.SIGTERM,
                                           self._process_tree_depth)

    if self._termination_notice_secs > 0:
      print(termcolor.colored('Press CTRL+C to terminate immediately.', 'blue'))

      def _force_stop():
        # Since we are forcefully stopping the system, we send signals to all
        # levels of the process trees. This makes sure to kill
        # tmux/gnome-terminal/etc, processes that create the Launchpad
        # subprocesses.
        for _, processes in self._process_workers.items():
          for process in processes:
            for child in process.children(recursive=True):
              child.send_signal(signal.SIGKILL)
            process.send_signal(signal.SIGKILL)
        signal.raise_signal(signal.SIGKILL)

      _register_signal_handler(signal.SIGINT, _force_stop)
      pending_secs = self._termination_notice_secs
      while self._has_active_workers() and pending_secs:
        print(
            termcolor.colored(
                f'Waiting for workers to stop for {pending_secs}s.', 'blue'),
            end='\r')
        time.sleep(1)
        pending_secs -= 1
    if self._has_active_workers():
      print(termcolor.colored('\nKilling entire runtime.', 'blue'))
      _force_stop()
