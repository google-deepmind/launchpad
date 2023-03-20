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

"""Tests for courier.python.py_client."""

from concurrent import futures
import datetime
import pickle
import threading
import time
from absl.testing import absltest
from absl.testing import parameterized
from courier.python import client  # pytype: disable=import-error
from courier.python import py_server  # pytype: disable=import-error

import mock
import numpy as np

from pybind11_abseil.status import StatusNotOk  # pytype: disable=import-error


class _A:

  def add(self, a, b):
    return a + b


class PyIntegrationTest(parameterized.TestCase):

  def _call_sleep(self, duration, use_async):
    if use_async:
      self._client.futures.sleep(duration).result()
    else:
      self._client.sleep(duration)

  def setUp(self):
    super(PyIntegrationTest, self).setUp()
    self._server = py_server.Server()
    self._server.Bind('no_args', lambda: 1000)
    self._server.Bind('lambda_add', lambda a, b: a + b)
    self._server.Bind('method_add', _A().add)
    self._server.Bind('add_default', lambda a, b=100: a + b)
    self._server.Bind('echo', lambda a: a)

    def _exception_method():
      raise ValueError('Exception method called')

    self._server.Bind('exception_method', _exception_method)
    self._server.Bind('sleep', time.sleep)
    self._server.Bind('rebind', lambda: 1234)
    self._server.Bind('bytes_value', lambda: b'1234')
    self._server.Bind('unicode_value', lambda: u'1234')

    self._server.Start()

    self._client = client.Client(self._server.address)

  def tearDown(self):
    self._server.Stop()
    self._server.Join()
    super(PyIntegrationTest, self).tearDown()

  def testLambdaCall(self):
    result = self._client.lambda_add(12, 5)
    self.assertEqual(result, 17)

  def testClassMethodCall(self):
    result = self._client.method_add(12, 5)
    self.assertEqual(result, 17)

  def testCallWithoutArguments(self):
    result = self._client.no_args()
    self.assertEqual(result, 1000)

  def testCallRebind(self):
    result = self._client.rebind()
    self.assertEqual(result, 1234)
    self._server.Bind('rebind', lambda: 2345)
    result = self._client.rebind()
    self.assertEqual(result, 2345)

    expected_msg = 'method rebind not found'
    self._server.Unbind('rebind')
    with self.assertRaisesRegex(StatusNotOk, expected_msg):
      result = self._client.rebind()
    self._server.Bind('rebind', lambda: 1234)
    result = self._client.rebind()
    self.assertEqual(result, 1234)

  def testCallWithDefaultArguments(self):
    result = self._client.add_default(23)
    self.assertEqual(result, 123)

  def testCallWithKwargs(self):
    result = self._client.add_default(23, b=500)
    self.assertEqual(result, 523)

  def testPythonErrorIsRaised(self):
    expected_msg = r'Exception method called'
    with self.assertRaisesRegex(StatusNotOk, expected_msg):
      self._client.exception_method()

  def testAsyncFutureCall(self):
    future = self._client.futures.add_default(23)
    self.assertEqual(future.result(), 123)

  def testAsyncFutureCancel(self):
    future = self._client.futures.sleep(2)
    self.assertTrue(future.cancel())
    try:
      future.result()
      self.fail('Expected future to raise cancelled exception')
    except futures.CancelledError:
      pass
    except StatusNotOk as e:
      self.assertIn('CANCEL', e.message)

  def testAsyncFutureException(self):
    future = self._client.futures.exception_method()
    expected_msg = r'Exception method called'
    with self.assertRaisesRegex(StatusNotOk, expected_msg):
      future.result()

  def testListMethods(self):
    self.assertCountEqual(
        client.list_methods(self._client),
        [
            'no_args',
            'lambda_add',
            'add_default',
            'exception_method',
            'sleep',
            'method_add',
            'rebind',
            'bytes_value',
            'unicode_value',
            'echo',
        ])


  def testUnicodeAddress(self):
    client.Client(u'test')
    py_server.Server(u'test')

  def testBytesValue(self):
    result = self._client.bytes_value()
    self.assertEqual(result, b'1234')

  def testUnicodeValue(self):
    result = self._client.unicode_value()
    self.assertEqual(result, u'1234')

  def testClientWaitsUntilServerIsUp(self):
    my_server = py_server.Server()
    my_client = client.Client(my_server.address)
    f = my_client.futures.no_args()
    my_server.Bind('no_args', lambda: 1000)
    my_server.Start()
    self.assertEqual(f.result(), 1000)
    my_server.Stop()

  @parameterized.named_parameters(('async', True), ('sync', False))
  def testNoErrorWhenDurationLessThanTimeout(self, use_async: bool):
    self._client = client.Client(
        self._server.address, call_timeout=datetime.timedelta(seconds=3)
    )
    self._call_sleep(duration=2, use_async=use_async)

  @parameterized.named_parameters(('async', True), ('sync', False))
  def testErrorDeadlineExceededWhenDurationGreaterThanTimeout(
      self, use_async: bool
  ):
    self._client = client.Client(
        self._server.address, call_timeout=datetime.timedelta(seconds=1)
    )
    with self.assertRaisesRegex(StatusNotOk, 'Deadline Exceeded'):
      self._call_sleep(duration=2, use_async=use_async)

  @parameterized.named_parameters(('async', True), ('sync', False))
  def testErrorDeadlineExceededWhenUnknownServerAddress(self, use_async: bool):
    self._client = client.Client(
        '[::]:12345', call_timeout=datetime.timedelta(seconds=1)
    )
    with self.assertRaisesRegex(StatusNotOk, 'Deadline Exceeded'):
      self._call_sleep(duration=0, use_async=use_async)

  @parameterized.named_parameters(('async', True), ('sync', False))
  def testErrorNotFoundWhenMethodDoesNotExist(self, use_async: bool):
    self._client = client.Client(
        self._server.address, call_timeout=datetime.timedelta(seconds=1)
    )
    with self.assertRaisesRegex(
        StatusNotOk, 'method nonexistent_method not found'
    ):
      if use_async:
        self._client.futures.nonexistent_method().result()
      else:
        self._client.nonexistent_method()


  def testWaitForReady(self):
    my_client_bad = client.Client('[::]:12345', wait_for_ready=False)
    with self.assertRaisesRegex(StatusNotOk,
                                'failed to connect to all addresses'):
      my_client_bad.blah()


if __name__ == '__main__':
  absltest.main()
