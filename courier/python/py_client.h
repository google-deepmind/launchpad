// Copyright 2020 DeepMind Technologies Limited.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef COURIER_PYTHON_PY_CLIENT_H_
#define COURIER_PYTHON_PY_CLIENT_H_

#include <pybind11/pybind11.h>

#include <functional>
#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/time/time.h"
#include "courier/client.h"
#include <pybind11/pybind11.h>

namespace courier {

// Helper object which allows Python to cancel async calls created by
// PyClient::AsyncCall.
class PyClientCallCanceller {
 public:
  explicit PyClientCallCanceller(std::function<void()> fn)
      : fn_(std::move(fn)) {}

  void Cancel() { fn_(); }

 private:
  std::function<void()> fn_;
};

// PyClient implements a Python interface for Client.
//
// Python example:
//   server = courier.Server('example_server', 10000)
//   server.Bind('please_add', lambda a, b: a + b)
//   server.Start()
//
//   client = courier.Client('example_server')
//   print client.please_add(4, 7)  # 11, computed on the server.
class PyClient : public Client {
 public:
  using Client::Client;  // inherit all constructors from Client.
  using PyObjectCallback = std::function<void(pybind11::object)>;

  // Calls a method on the server with a list of arguments.
  // The result from calling the method will be returned as a Python object.
  // A non-zero `timeout` will be used as the timeout for the RPC call.
  absl::StatusOr<pybind11::object> PyCall(const std::string& method,
                                          const pybind11::list& args,
                                          const pybind11::dict& kwargs,
                                          bool wait_for_ready,
                                          absl::Duration timeout, bool compress,
                                          bool chunk_tensors);

  // Asynchronous variant of PyCall.
  // A non-zero `timeout` will be used as the timeout for the RPC call.
  // Returns a function to cancel the call. Calling this function after the call
  // has finished is legal and results in a no-op.
  absl::StatusOr<PyClientCallCanceller> AsyncPyCall(
      const std::string& method, const pybind11::list& args,
      const pybind11::dict& kwargs, PyObjectCallback result_cb,
      PyObjectCallback exception_cb, bool wait_for_ready,
      absl::Duration timeout, bool compress, bool chunk_tensors);
};

}  // namespace courier

#endif  // COURIER_PYTHON_PY_CLIENT_H_
