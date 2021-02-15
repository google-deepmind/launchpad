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

#include "courier/tf_serialize.h"

#include <string>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "courier/serialization/serialization.pb.h"
#include "tensorflow/core/framework/allocator.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor.pb.h"
#include "tensorflow/core/framework/tensor_shape.h"
#include "tensorflow/core/platform/tstring.h"

namespace courier {

absl::Status DeserializeTensor(const courier::SerializedObject& buffer,
                               tensorflow::TensorProto* tensor_value,
                               tensorflow::Allocator* allocator) {
  if (!buffer.has_tensor_value()) {
    return absl::InternalError("TensorProto did not exist");
  }
  // Note that the tensor_content member is a Cord, so this is essentially
  // zero-copy.
  *tensor_value = buffer.tensor_value();
  return absl::OkStatus();
}

absl::Status DeserializeTensor(const courier::SerializedObject& buffer,
                               tensorflow::Tensor* tensor_value,
                               tensorflow::Allocator* allocator) {
  if (buffer.has_tensor_value()) {
    if (!tensor_value->FromProto(allocator, buffer.tensor_value())) {
      return absl::InternalError("Failed to parse TensorProto.");
    }
    return absl::OkStatus();
  }

  tensorflow::TensorShape shape;

  switch (buffer.payload_case()) {
    case courier::SerializedObject::kBoolValue: {
      *tensor_value = tensorflow::Tensor(allocator, tensorflow::DT_BOOL, shape);
      auto flat_tensor = tensor_value->flat<bool>();
      flat_tensor(0) = buffer.bool_value();
      break;
    }
    case courier::SerializedObject::kDoubleValue: {
      *tensor_value =
          tensorflow::Tensor(allocator, tensorflow::DT_FLOAT, shape);
      auto flat_tensor = tensor_value->flat<float>();
      flat_tensor(0) = static_cast<float>(buffer.double_value());
      break;
    }
    case courier::SerializedObject::kIntValue: {
      *tensor_value =
          tensorflow::Tensor(allocator, tensorflow::DT_INT32, shape);
      auto flat_tensor = tensor_value->flat<int>();
      flat_tensor(0) = static_cast<int>(buffer.int_value());
      break;
    }
    case courier::SerializedObject::kStringValue: {
      *tensor_value =
          tensorflow::Tensor(allocator, tensorflow::DT_STRING, shape);
      auto flat_tensor = tensor_value->flat<tensorflow::tstring>();
      flat_tensor(0) = buffer.string_value();
      break;
    }
    case courier::SerializedObject::kUnicodeValue: {
      *tensor_value =
          tensorflow::Tensor(allocator, tensorflow::DT_STRING, shape);
      auto flat_tensor = tensor_value->flat<tensorflow::tstring>();
      flat_tensor(0) = buffer.unicode_value();
      break;
    }
    default:
      return absl::InvalidArgumentError(
          absl::StrCat("Input value could not be parsed as a Tensor: ",
                       buffer.DebugString()));
  }
  return absl::OkStatus();
}

}  // namespace courier
