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

#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>

#include "courier/platform/status_macros.h"
#include "courier/serialization/py_serialize.h"
#include "courier/serialization/tensor_conversion.h"
#include "pybind11_abseil/absl_casters.h"
#include "pybind11_abseil/status_casters.h"

namespace courier {
namespace {

namespace py = pybind11;

absl::StatusOr<py::bytes> SerializeToString(const py::handle& handle) {
  PyObject* object = handle.ptr();
  COURIER_ASSIGN_OR_RETURN(auto result, SerializePyObjectToString(object));
  return py::bytes(result);
}

absl::StatusOr<py::object> DeserializeFromString(const std::string& str) {
  COURIER_ASSIGN_OR_RETURN(auto result, DeserializePyObjectFromString(str));
  py::object object = py::reinterpret_steal<py::object>(result);
  return object;
}

absl::StatusOr<SerializedObject> SerializeToProto(const py::handle& handle) {
  PyObject* object = handle.ptr();
  return SerializePyObject(object);
}

absl::StatusOr<py::object> DeserializeFromProto(
    const SerializedObject& buffer) {
  COURIER_ASSIGN_OR_RETURN(auto tensor_lookup, CreateTensorLookup(buffer));
  COURIER_ASSIGN_OR_RETURN(auto result,
                           DeserializePyObjectUnsafe(buffer, tensor_lookup));
  return py::reinterpret_steal<py::object>(result);
}


PYBIND11_MODULE(pybind, m) {
  ImportNumpy();

  py::google::ImportStatusModule();

  m.def("SerializeToString", &SerializeToString,
        "Serializes Object to a string");
  m.def("DeserializeFromString", &DeserializeFromString,
        "Deserializes object from a string");
  m.def("SerializeToProto", &SerializeToProto, "Serializes object to a proto");
  m.def("DeserializeFromProto", &DeserializeFromProto,
        "Deserializes object from a proto");
}

}  // namespace
}  // namespace courier
