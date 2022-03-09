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

#include "courier/serialization/pybind_serialize.h"

#include "absl/status/status.h"
#include "courier/platform/status_macros.h"
#include "courier/serialization/py_serialize.h"
#include <pybind11/pybind11.h>

namespace courier {

namespace py = pybind11;

absl::Status SerializePybindArgs(const py::list& args, const py::dict& kwargs,
                                 CallArguments* serialized) {
  for (py::handle arg : args) {
    PyObject* object = arg.ptr();
    COURIER_RETURN_IF_ERROR(SerializePyObject(object, serialized->add_args()));
  }

  for (const auto& kwarg : kwargs) {
    auto ins = serialized->mutable_kwargs()->insert(
        {kwarg.first.cast<std::string>(), courier::SerializedObject()});
    COURIER_RET_CHECK(ins.second)
        << "Duplicate kwargs key: " << kwarg.first.cast<std::string>();
    PyObject* object = kwarg.second.ptr();
    COURIER_RETURN_IF_ERROR(SerializePyObject(object, &ins.first->second));
  }

  return absl::OkStatus();
}

}  // namespace courier
