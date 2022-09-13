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

"""WorkerManager handles thread and process-based runtimes."""

import atexit
import collections
from concurrent import futures
import os
import signal
import subprocess
import sys
import threading
import time
import traceback
from typing import Optional, Sequence, Text

from absl import flags
from absl import logging
from absl.testing import absltest
from launchpad import flags as lp_flags  
import psutil
import termcolor

FLAGS = flags.FLAGS

ThreadWorker = collections.namedtuple('ThreadWorker', ['thread', 'future'])

_WORKER_MANAGERS = threading.local()
_HAS_MAIN_MANAGER = False
_SIGNAL_HANDLERS = None


def get_worker_manager():
  manager = getattr(_WORKER_MANAGERS, 'manager', None)
  if not manager:
    raise RuntimeError('Worker manager is only available from the Launchpad''s '
                       'program node thread.')
  return manager


def _signal_dispatcher(sig, frame=None):
  """Dispatches a given signal to all registered handlers."""
  if sig != signal.SIGALRM and sig != signal.SIGUSR1:
    # Notify user-registered stop handler(s) before other once.
    _signal_dispatcher(signal.SIGUSR1, frame)
  dispatchers = _SIGNAL_HANDLERS[sig].copy()
  if sig != signal.SIGALRM:
    _SIGNAL_HANDLERS[sig].clear()
  for dispatcher in dispatchers:
    try:
      try:
        dispatcher(sig, frame)
      except TypeError:
        dispatcher()  # pytype: disable=wrong-arg-count
    except KeyboardInterrupt:
      pass


def _register_signal_dispatcher(sig):
  """Registers signal dispatcher for a given signal type."""
  assert sig not in _SIGNAL_HANDLERS
  _SIGNAL_HANDLERS[sig] = set()
  try:
    old_signal = signal.signal(sig, _signal_dispatcher)
  except ValueError:
    logging.warning(
        'Launchpad cannot register its signal handler. This is likely because '
        'you are not running lp.launch() from the main thread. Launchpad will '
        'NOT attempt to handle signal %s.', sig)
  if callable(old_signal):
    _SIGNAL_HANDLERS[sig].add(old_signal)


def _register_signal_handler(sig, handler):
  """Registers a signal handler."""
  global _SIGNAL_HANDLERS
  if _SIGNAL_HANDLERS is None:
    _SIGNAL_HANDLERS = dict()
    _register_signal_dispatcher(signal.SIGTERM)
    _register_signal_dispatcher(signal.SIGQUIT)
    _register_signal_dispatcher(signal.SIGINT)
    _register_signal_dispatcher(signal.SIGALRM)
    _SIGNAL_HANDLERS[signal.SIGUSR1] = set()
  assert sig in _SIGNAL_HANDLERS
  _SIGNAL_HANDLERS[sig].add(handler)


def _remove_signal_handler(sig, handler):
  """Unregisters a signal handler."""
  if not _SIGNAL_HANDLERS:
    return
  try:
    _SIGNAL_HANDLERS[sig].remove(handler)
  except KeyError:
    pass


def register_stop_handler(handler):
  """Registers a stop handler, which is called upon program termination.

    Stop handler can also be registered outside of the program's execution
    scope. It is guaranted that handler will be called at most once.

  Args:
    handler: Handler to be called.
  """
  _register_signal_handler(signal.SIGUSR1, handler)


def unregister_stop_handler(handler):
  """Unregisters a stop handler previously registered with register_stop_handler."""
  _remove_signal_handler(signal.SIGUSR1, handler)


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


def stop_event() -> threading.Event:
  """Returns a threading.Event used to wait for termination signal on a Program.

  Usage examples:
  - Perform cleanup at the end of the run:
    start_server()
    lp.stop_event().wait()
    stop_server()
    checkpoint()
  """
  return get_worker_manager().stop_event()


class WorkerManager:
  """Encapsulates running threads and processes of a Launchpad Program."""

  def __init__(
      self,
      kill_main_thread=True,
      register_in_thread=False,
  ):
    """Initializes a WorkerManager.

    Args:
      kill_main_thread: When True (the default), it will kill the current
        process (program launcher or the node) after killing all th
        subprocesses, to guarantee complete cleanup of the program. Setting it
        to False disables this behavior, which can be useful in test_mp, where
        killing the main process causes a test to fail.
      register_in_thread: Make the worker manager accessible through
        `get_worker_manager()` in the current thread (needed by `stop_event()`
        for example). It should be False if we don't need to access
        `get_worker_manager()` , e.g. at the launcher thread of local_mt and
        local_mp.
    """
    self._mutex = threading.Lock()
    self._termination_notice_secs = -1
    handle_user_stop = True
    global _HAS_MAIN_MANAGER
    if not _HAS_MAIN_MANAGER:
      # This logic resolves the potential conflict between two WorkerManagers
      # in the same process. In particular, only the first one will execute the
      # "countdown-and-sigkill" logic upon Ctrl+C.
      self._termination_notice_secs = FLAGS.lp_termination_notice_secs
      _HAS_MAIN_MANAGER = True
    self._active_workers = collections.defaultdict(list)
    self._workers_count = collections.defaultdict(lambda: 0)
    self._first_failure = None
    self._stop_counter = 0
    self._kill_main_thread = kill_main_thread
    self._stop_event = threading.Event()
    self._main_thread = threading.current_thread().ident
    _register_signal_handler(signal.SIGTERM, self._sigterm)
    _register_signal_handler(signal.SIGQUIT, self._sigquit)
    if handle_user_stop:
      _register_signal_handler(signal.SIGINT, self._stop_by_user)
    if register_in_thread:
      _WORKER_MANAGERS.manager = self

  def _disable_signals(self):
    self._disable_alarm()
    _remove_signal_handler(signal.SIGTERM, self._sigterm)
    _remove_signal_handler(signal.SIGQUIT, self._sigquit)
    _remove_signal_handler(signal.SIGINT, self._stop_by_user)

  def _sigterm(self):
    """Handles SIGTERM by stopping the workers."""
    self._stop()

  def _sigquit(self):
    self._kill()

  def wait_for_stop(self, timeout_secs: Optional[float] = None):
    """Blocks until managed runtime is terminating or timeout is reached."""
    return self._stop_event.wait(timeout_secs)

  def stop_event(self):
    """Returns an event used to wait for termination signal on a Program."""
    return self._stop_event

  def thread_worker(self, name, function):
    """Registers and starts a new thread worker.

    Args:
      name: Name of the worker group.
      function: Entrypoint function to execute in a worker.
    """
    with self._mutex:
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
      self._workers_count[name] += 1
      worker = ThreadWorker(thread=thread, future=future)
      self._active_workers[name].append(worker)

  def process_worker(self, name, command, env=None, **kwargs):
    """Adds process worker to the runtime.

    Args:
      name: Name of the worker's group.
      command: Command to execute in the worker.
      env: Environment variables to set for the worker.
      **kwargs: Other parameters to be passed to `subprocess.Popen`.
    """
    with self._mutex:
      process = subprocess.Popen(command, env=env or {}, **kwargs)  
      self._workers_count[name] += 1
      self._active_workers[name].append(process)

  def register_existing_process(self, name: str, pid: int):
    """Registers already started worker process.

    Args:
      name: Name of the workers' group.
      pid: Pid of the process to monitor.
    """
    with self._mutex:
      self._workers_count[name] += 1
      self._active_workers[name].append(psutil.Process(pid))

  def _stop_by_user(self):
    """Handles stopping of the runtime by a user."""
    print(
        termcolor.colored(
            'User-requested termination. Asking workers to stop.', 'blue'))
    if self._termination_notice_secs > 0:
      print(termcolor.colored('Press CTRL+C to terminate immediately.', 'blue'))
    if self._termination_notice_secs >= 0:
      signal.signal(signal.SIGINT, lambda sig, frame: self._kill())
    self._stop()

  def _kill_process_tree(self, pid):
    """Kills all child processes of the current process."""
    parent = psutil.Process(pid)
    processes = [parent]
    for process in parent.children(recursive=True):
      processes.append(process)
    for process in processes:
      try:
        process.send_signal(signal.SIGKILL)
      except psutil.NoSuchProcess:
        pass

  def _kill(self):
    """Kills all workers (and main thread/process if needed)."""
    print(termcolor.colored('\nKilling entire runtime.', 'blue'))
    kill_self = self._kill_main_thread
    for workers in self._active_workers.values():
      for worker in workers:
        if isinstance(worker, ThreadWorker):
          # Not possible to kill a thread without killing the process.
          kill_self = True
        else:
          self._kill_process_tree(worker.pid)
    if kill_self:
      self._kill_process_tree(os.getpid())

  def _stop_or_kill(self):
    """Stops all workers; kills them if they don't stop on time."""
    pending_secs = self._termination_notice_secs - self._stop_counter
    if pending_secs == 0:
      if self._termination_notice_secs > 0:
        still_running = [
            label for label in self._active_workers
            if self._active_workers[label]
        ]
        print(
            termcolor.colored(
                f'Worker groups that did not terminate in time: {still_running}',
                'red'))
      self._kill()
      return
    if pending_secs >= 0:
      print(
          termcolor.colored(f'Waiting for workers to stop for {pending_secs}s.',
                            'blue'),
          end='\r')
    self._stop_counter += 1
    # Notify ThreadWorkers which registered for notifications.
    _signal_dispatcher(signal.SIGUSR1)
    for workers in self._active_workers.values():
      for worker in workers:
        if isinstance(worker, ThreadWorker):
          # Thread workers should use wait_for_stop or register_stop_handler.
          pass
        elif isinstance(worker, subprocess.Popen):
          try:
            worker.send_signal(signal.SIGTERM)
          except psutil.NoSuchProcess:
            pass
        else:
          # Notify all workers running under a proxy process.
          children = worker.children(recursive=True)
          worker_found = False
          for process in children:
            try:
              process_name = process.name()
              if process_name != 'bash' and 'envelope_' not in process_name:
                worker_found = True
                process.send_signal(signal.SIGTERM)
            except psutil.NoSuchProcess:
              pass
          if not worker_found:
            # No more workers running, so we can kill the proxy itself.
            try:
              worker.send_signal(signal.SIGKILL)
            except psutil.NoSuchProcess:
              pass

    if pending_secs >= 0:
      signal.alarm(1)

  def _stop(self):
    """Requests all workers to stop and schedule delayed termination."""
    self._stop_event.set()
    try:
      if self._termination_notice_secs > 0:
        _register_signal_handler(signal.SIGALRM, self._stop_or_kill)
    except ValueError:
      # This happens when we attempt to register a signal handler but not in the
      # main thread. Send a SIGTERM to redirect to the main thread.
      psutil.Process(os.getpid()).send_signal(signal.SIGTERM)
      return

    self._stop_or_kill()

  def _disable_alarm(self):
    _remove_signal_handler(signal.SIGALRM, self._stop_or_kill)
    signal.alarm(0)

  def stop_and_wait(self):
    """Requests stopping all workers and wait for termination."""
    with self._mutex:
      self._stop()
    self.wait(raise_error=False)

  def join(self):
    self.wait()

  def wait(self,
           labels_to_wait_for: Optional[Sequence[Text]] = None,
           raise_error=True,
           return_on_first_completed=False):
    """Waits for workers to finish. Also stops the program upon worker failures.

    Args:
      labels_to_wait_for: If supplied, only wait for these groups' workers to
        finish. Wait for all workers otherwise.
      raise_error: Raise an exception upon any worker failure.
      return_on_first_completed: Whether to return upon the first completed (or
        failed) worker.

    Raises:
      RuntimeError: if any worker raises an exception.
    """
    active_workers = True
    while active_workers:
      with self._mutex:
        self._check_workers()
        active_workers = False
        if self._first_failure and raise_error:
          failure = self._first_failure
          self._first_failure = None
          raise failure
        for label in labels_to_wait_for or self._active_workers.keys():
          if self._active_workers[label]:
            active_workers = True
          if (return_on_first_completed and len(self._active_workers[label])
              < self._workers_count[label]):
            return
      time.sleep(0.1)

  def cleanup_after_test(self, test_case: absltest.TestCase):
    """Cleanups runtime after a test."""
    del test_case
    with self._mutex:
      self._check_workers()
      self._stop()
      self._disable_signals()
    self.wait(raise_error=False)
    with self._mutex:
      if self._first_failure:
        raise self._first_failure


  def check_for_thread_worker_exception(self):
    with self._mutex:
      for label in self._active_workers:
        for worker in self._active_workers[label]:
          if not worker.thread.is_alive():
            worker.thread.join()
            # This will raise the exception, if any.
            worker.future.result()

  def _check_workers(self):
    """Checks status of running workers, terminate runtime in case of errors.

    This REQUIRES holding self._mutex.
    """
    has_workers = False
    for label in self._active_workers:
      still_active = []
      for worker in self._active_workers[label]:
        active = True
        if isinstance(worker, ThreadWorker):
          if not worker.thread.is_alive():
            worker.thread.join()
            if not self._stop_counter:
              try:
                worker.future.result()
              except BaseException as e:  
                if not self._first_failure and not self._stop_counter:
                  self._first_failure = e
                  print(f'Node {worker} crashed:')
                  traceback.print_exc()
            active = False
        elif isinstance(worker, subprocess.Popen):
          try:
            res = worker.wait(0)
            active = False
            if res and not self._first_failure and not self._stop_counter:
              self._first_failure = RuntimeError(
                  f'One of the workers exited with code {res}.')
          except subprocess.TimeoutExpired:
            pass
        else:
          try:
            # We can't obtain return code of external process, so clean
            # termination is assumed.
            res = worker.wait(0)
            active = False
          except psutil.TimeoutExpired:
            pass
        if active:
          has_workers = True
          still_active.append(worker)
      self._active_workers[label] = still_active
    if has_workers and self._first_failure and not self._stop_counter:
      self._stop()
    elif not has_workers:
      self._disable_alarm()

  def __del__(self):
    try:
      if sys.is_finalizing():
        return
    except AttributeError:
      # AttributeError can be thrown when `sys` was already destroyed upon
      # finalization.
      return
    self._disable_signals()
