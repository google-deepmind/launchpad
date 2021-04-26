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

#include "courier/router.h"

#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>

#include "pybind11_abseil/absl_casters.h"
#include "pybind11_abseil/status_casters.h"

namespace courier {
namespace {

namespace py = pybind11;

struct RouterUnique : public Router {
  using Router::Router;
};

template <typename T>
struct unique_ptr_nogil_deleter {
  void operator()(T *ptr) {
    pybind11::gil_scoped_release nogil;
    delete ptr;
  }
};

PYBIND11_MODULE(router, m) {
  py::google::ImportStatusModule();

  py::class_<Router, std::unique_ptr<Router, unique_ptr_nogil_deleter<Router>>>(
      m, "Router")
      .def(py::init<>())
      .def("Bind", &Router::Bind)
      .def("Unbind", &Router::Unbind, py::call_guard<py::gil_scoped_release>());
}

}  // namespace
}  // namespace courier
