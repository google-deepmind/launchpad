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

#ifndef COURIER_SERIALIZATION_PY_SERIALIZE_H_
#define COURIER_SERIALIZATION_PY_SERIALIZE_H_

#include <string>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "courier/serialization/tensor_conversion.h"
#include "courier/serialization/pyobject_ptr.h"
#include "courier/serialization/serialization.pb.h"

namespace courier {

using SafePyObjectPtr = courier::PyObjectPtr;

absl::Status SerializePyObject(PyObject* object, SerializedObject* buffer);

absl::StatusOr<SerializedObject> SerializePyObject(PyObject* object);

absl::StatusOr<PyObject*> DeserializePyObjectUnsafe(
    const SerializedObject& buffer, TensorLookup& tensor_lookup);

absl::StatusOr<SafePyObjectPtr> DeserializePyObject(
    const SerializedObject& buffer, TensorLookup& tensor_lookup);

absl::StatusOr<SafePyObjectPtr> DeserializePyObject(
    const SerializedObject& buffer);

// Convenience method for serializing a PyObject to a string.
absl::StatusOr<std::string> SerializePyObjectToString(PyObject* object);

// Convenience method for deserializing a string to a PyObject.
absl::StatusOr<PyObject*> DeserializePyObjectFromString(const std::string& str);

void ImportNumpy();

}  // namespace courier

#endif  // COURIER_SERIALIZATION_PY_SERIALIZE_H_
