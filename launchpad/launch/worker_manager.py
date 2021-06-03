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

import collections
from concurrent import futures
import ctypes
import os
import signal
import subprocess
import threading
import time
from typing import Optional, Sequence, Text

from absl.testing import absltest
import psutil
import termcolor

ThreadWorker = collections.namedtuple('ThreadWorker', ['thread', 'future'])

_WORKER_MANAGERS = threading.local()


def get_worker_manager():
  manager = getattr(_WORKER_MANAGERS, 'manager', None)
  assert manager, 'Worker manager is not available in the current thread'
  return manager


def wait_for_stop():
  """Blocks until termination of the node's program is requested.

    Can be used to perform cleanup at the end of the run, for example:
      start_server()
      lp.wait_for_stop()
      stop_server()
      checkpoint()
  """
  get_worker_manager().wait_for_stop()


class WorkerManager:
  """Encapsulates running threads and processes of a Launchpad Program."""

  def __init__(self,
               stop_main_thread=False,
               kill_main_thread=True,
               register_in_thread=False,
               handle_user_stop=True,
               daemon_workers=False):
    """Initializes a WorkerManager.

    Args:
      stop_main_thread: Should main thread be notified about termination.
      kill_main_thread: When set to false try not to kill the launcher while
        killing workers. This is not possible when thread workers run in the
        same process.
      register_in_thread: TODO
      handle_user_stop: TODO
      daemon_workers: Start thread worker processes as daemons.
    """
    self._active_workers = collections.defaultdict(list)
    self._workers_count = collections.defaultdict(lambda: 0)
    self._first_failure = None
    self._stop_counter = 0
    self._kill_main_thread = kill_main_thread
    self._stop_event = threading.Event()
    self._handle_user_stop = handle_user_stop
    self._daemon_workers = daemon_workers
    self._old_sigterm = signal.signal(signal.SIGTERM, self._sigterm)
    self._old_sigquit = signal.signal(signal.SIGQUIT, self._sigquit)
    if handle_user_stop:
      signal.signal(signal.SIGINT, lambda sig, frame: self._stop_by_user())
    self._stop_main_thread = stop_main_thread
    if register_in_thread:
      _WORKER_MANAGERS.manager = self

  def _sigterm(self, sig, frame):
    if callable(self._old_sigterm):
      self._old_sigterm(sig, frame)
    self._stop()

  def _sigquit(self, sig, frame):
    if callable(self._old_sigquit):
      self._old_sigquit(sig, frame)
    self._kill()

  def wait_for_stop(self):
    """Blocks until managed runtime is being terminated."""
    self._stop_event.wait()

  def thread_worker(self, name, function):
    """Registers and start a new thread worker.

    Args:
      name: Name of the worker group.
      function: Entrypoint function to execute in a worker.
    """
    future = futures.Future()
    def run_inner(f=function, future=future, manager=self):
      _WORKER_MANAGERS.manager = manager
      try:
        future.set_result(f())
      except Exception as e:  
        future.set_exception(e)
        raise e
    builder = lambda t, n: threading.Thread(target=t, name=n)
    thread = builder(run_inner, name)
    if self._daemon_workers:
      thread.setDaemon(True)

    self._workers_count[name] += 1
    self._active_workers[name].append(
        ThreadWorker(thread=thread, future=future))
    thread.start()

  def process_worker(self, name, command, env=None, preexec_fn=None):
    """Adds process worker to the runtime.

    Args:
      name: Name of the worker's group.
      command: Command to execute in the worker.
      env: Environment variables to set for the worker.
      preexec_fn: Run `preexec_fn` before running commands inside the worker.
    """
    process = subprocess.Popen(command, env=env or {}, preexec_fn=preexec_fn)  
    self._workers_count[name] += 1
    self._active_workers[name].append(process)

  def _stop_by_user(self):
    """Handles stopping of the runtime by a user."""
    print(
        termcolor.colored('User-requested termination. Asking workers to stop.',
                          'blue'))
    print(termcolor.colored('Press CTRL+C to terminate immediately.', 'blue'))
    signal.signal(signal.SIGINT, lambda sig, frame: self._kill())
    self._stop()

  def _kill_all_processes(self):
    """Kills all child processes of the current process."""
    parent = psutil.Process(os.getpid())
    for process in parent.children(recursive=True):
      try:
        process.send_signal(signal.SIGKILL)
      except psutil.NoSuchProcess:
        pass
    parent.send_signal(signal.SIGKILL)

  def _kill(self):
    """Kills all workers (and main thread/process if needed)."""
    print(termcolor.colored('Killing entire runtime.', 'blue'))
    if self._kill_main_thread:
      self._kill_all_processes()
    for workers in self._active_workers.values():
      for worker in workers:
        if isinstance(worker, ThreadWorker):
          # Not possible to kill a thread without killing the process.
          self._kill_all_processes()
        else:
          worker.send_signal(signal.SIGKILL)

  def _stop_or_kill(self):
    """Stops all workers; kills them if they don't stop in 10 seconds."""
    self._stop_counter += 1
    pending_secs = 10 - self._stop_counter
    print(
        termcolor.colored(f'Waiting for workers to stop for {pending_secs}s.',
                          'blue'),
        end='\r')
    if pending_secs <= 0:
      self._kill()
    for workers in self._active_workers.values():
      for worker in workers:
        if isinstance(worker, ThreadWorker):
          if self._stop_counter == 1:
            res = ctypes.pythonapi.PyThreadState_SetAsyncExc(
                ctypes.c_long(worker.thread.ident),
                ctypes.py_object(SystemExit))
            assert res < 2, 'Exception raise failure'
        else:
          worker.send_signal(signal.SIGTERM)
    if self._stop_main_thread:
      res = ctypes.pythonapi.PyThreadState_SetAsyncExc(
          ctypes.c_long(threading.main_thread().ident),
          ctypes.py_object(SystemExit))
      assert res < 2, 'Exception raise failure'
    signal.alarm(1)

  def _stop(self):
    """Requests all workers to stop and schedule delayed termination."""
    if self._stop_counter == 0:
      self._stop_event.set()
      old_sig = signal.signal(signal.SIGALRM, lambda s, f: self._stop_or_kill())
      assert old_sig == signal.SIG_DFL
      self._stop_or_kill()

  def _dont_kill(self):
    # All workers have finished, don't kill the runtime.
    signal.alarm(0)
    signal.signal(signal.SIGALRM, signal.SIG_DFL)

  def stop_and_wait(self):
    """Requests stopping all workers and wait for termination."""
    self._stop()
    self.wait(raise_error=False)
    self._dont_kill()

  def join(self):
    self.wait()

  def wait(self, labels_to_wait_for: Optional[Sequence[Text]] = None,
           raise_error=True, return_on_first_completed=False):
    """Waits for workers to finish.

    Args:
      labels_to_wait_for: If supplied, only wait for these groups' workers to
        finish. Wait for all workers otherwise.
      raise_error: Raise an exception upon any worker failure.
      return_on_first_completed: Whether to return upon the first completed
        (or failed) worker.

    Raises:
      RuntimeError: if any worker raises an exception.
    """
    while True:
      try:
        active_workers = True
        while active_workers:
          self._check_workers()
          active_workers = False
          if self._first_failure and raise_error:
            failure = self._first_failure
            self._first_failure = None
            raise failure
          for label in labels_to_wait_for or self._active_workers.keys():
            if self._active_workers[label]:
              active_workers = True
            if (return_on_first_completed and
                len(self._active_workers[label]) < self._workers_count[label]):
              return
          time.sleep(0.1)
        return
      except SystemExit:
        # Keep waiting for all workers to finish.
        pass

  def cleanup_after_test(self, test_case: absltest.TestCase):
    """Cleanups runtime after a test."""
    self._check_workers()
    self._stop()
    self.wait(raise_error=False)
    self._dont_kill()
    test_case.assertIsNone(self._first_failure)

  def _check_workers(self):
    """Checks status of running workers, terminate runtime in case of errors."""
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
              except Exception as e:  
                if not self._first_failure and not self._stop_counter:
                  self._first_failure = e
            active = False
        else:
          try:
            res = worker.wait(0)
            active = False
            if res and not self._first_failure and not self._stop_counter:
              self._first_failure = RuntimeError('One of the workers failed.')
          except subprocess.TimeoutExpired:
            pass
        if active:
          still_active.append(worker)
      self._active_workers[label] = still_active
    if self._first_failure and not self._stop_counter:
      self._stop()
