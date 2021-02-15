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

#ifndef COURIER_TF_SERIALIZE_H_
#define COURIER_TF_SERIALIZE_H_

#include "absl/status/status.h"
#include "courier/serialization/serialization.pb.h"
#include "courier/serialization/serialize.h"
#include "tensorflow/core/framework/allocator.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor.pb.h"

namespace courier {

absl::Status DeserializeTensor(const SerializedObject& buffer,
                               tensorflow::Tensor* tensor_value,
                               tensorflow::Allocator* allocator);
absl::Status DeserializeTensor(const SerializedObject& buffer,
                               tensorflow::TensorProto* tensor_value,
                               tensorflow::Allocator* allocator);

// Template specialization for serializing a Tensor.
template <>
struct Serializer<tensorflow::Tensor> {
  static absl::Status Write(const tensorflow::Tensor& value,
                            SerializedObject* buffer) {
    value.AsProtoTensorContent(buffer->mutable_tensor_value());
    return absl::OkStatus();
  }
};

// Template specialization for deserializing a Tensor.
template <>
struct Deserializer<tensorflow::Tensor> {
  static absl::Status Read(const SerializedObject& buffer,
                           tensorflow::Tensor* value) {
    return courier::DeserializeTensor(buffer, value,
                                      tensorflow::cpu_allocator());
  }
};

// Template specialization for deserializing a Tensor to a raw TensorProto.
template <>
struct Deserializer<tensorflow::TensorProto> {
  static absl::Status Read(const SerializedObject& buffer,
                           tensorflow::TensorProto* value) {
    return courier::DeserializeTensor(buffer, value,
                                      tensorflow::cpu_allocator());
  }
};

}  // namespace courier

#endif  // COURIER_TF_SERIALIZE_H_
