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

#include "courier/python/py_client.h"

#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>


#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/time/time.h"
#include "courier/call_context.h"
#include "courier/client.h"
#include "courier/platform/logging.h"
#include "courier/platform/status_macros.h"
#include "courier/serialization/py_serialize.h"
#include "courier/serialization/pybind_serialize.h"
#include "courier/serialization/serialization.pb.h"
#include "pybind11_abseil/absl_casters.h"
#include "pybind11_abseil/status_casters.h"

namespace courier {

namespace py = pybind11;

absl::StatusOr<py::object> PyClient::PyCall(const std::string& method,
                                            const py::list& args,
                                            const py::dict& kwargs,
                                            bool wait_for_ready,
                                            absl::Duration timeout,
                                            bool compress, bool chunk_tensors) {
  auto arguments = std::make_unique<courier::CallArguments>();
  COURIER_RETURN_IF_ERROR(SerializePybindArgs(args, kwargs, arguments.get()));

  PyThreadState* thread_state = PyEval_SaveThread();
  CallContext context(timeout, /*wait_for_ready=*/wait_for_ready,
                      /*compress=*/compress, /*interruptible=*/true,
                      /*chunk_tensors=*/chunk_tensors);
  absl::StatusOr<courier::CallResult> result_or =
      CallF(&context, method, std::move(arguments));
  PyEval_RestoreThread(thread_state);
  COURIER_ASSIGN_OR_RETURN(courier::CallResult result, std::move(result_or));
  COURIER_ASSIGN_OR_RETURN(courier::SafePyObjectPtr py_object,
                           DeserializePyObject(result.result()));
  return py::reinterpret_steal<py::object>(py_object.release());
}

absl::StatusOr<PyClientCallCanceller> PyClient::AsyncPyCall(
    const std::string& method, const py::list& args, const py::dict& kwargs,
    PyObjectCallback result_cb, PyObjectCallback exception_cb,
    bool wait_for_ready, absl::Duration timeout, bool compress,
    bool chunk_tensors) {
  auto arguments = absl::make_unique<courier::CallArguments>();
  COURIER_RETURN_IF_ERROR(SerializePybindArgs(args, kwargs, arguments.get()));

  auto context = std::make_shared<CallContext>(
      timeout, /*wait_for_ready=*/wait_for_ready, /*compress=*/compress,
      /*interruptible=*/true, /*chunk_tensors=*/chunk_tensors);
  // Release the GIL as `AsynCallF` might block on `Client::Init()`.
  PyThreadState* thread_state = PyEval_SaveThread();
  AsyncCallF(
      context.get(), method, std::move(arguments),
      [exception_cb = std::move(exception_cb), result_cb = std::move(result_cb),
       context](const absl::StatusOr<courier::CallResult>& result_or) {
        py::gil_scoped_acquire gil;
        if (!result_or.ok()) {
          exception_cb(
              py::cast(py::google::DoNotThrowStatus(result_or.status())));
          return;
        }
        const courier::CallResult& result = result_or.value();
        absl::StatusOr<courier::SafePyObjectPtr> py_result =
            DeserializePyObject(result.result());
        if (!py_result.ok()) {
          exception_cb(
              py::cast(py::google::DoNotThrowStatus(py_result.status())));
          return;
        }
        result_cb(
            py::reinterpret_steal<py::object>(py_result.value().release()));
      });

  PyEval_RestoreThread(thread_state);
  return PyClientCallCanceller([context] { context->Cancel(); });
}

namespace {

PYBIND11_MODULE(py_client, m) {
  ImportNumpy();
  py::google::ImportStatusModule();

  py::class_<PyClientCallCanceller>(m, "PyClientCallCanceller")
      .def("Cancel", &PyClientCallCanceller::Cancel);

  py::class_<PyClient, std::shared_ptr<PyClient>>(m, "PyClient")
      .def(py::init<const std::string&>())
      .def("PyCall", &PyClient::PyCall)
      .def("AsyncPyCall", &PyClient::AsyncPyCall)
      .def("ListMethods", &PyClient::ListMethods,
           py::call_guard<py::gil_scoped_release>())
      .def("Shutdown", &PyClient::Shutdown,
           py::call_guard<py::gil_scoped_release>());
}

}  // namespace
}  // namespace courier
