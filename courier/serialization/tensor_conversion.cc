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

#include "courier/serialization/tensor_conversion.h"

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/synchronization/mutex.h"
#include "courier/platform/default/status_macros.h"
#include "courier/platform/status_macros.h"
#include "courier/serialization/serialization.pb.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor.pb.h"

namespace courier {
namespace {

class TensorLookupBuilder {
 public:
  TensorLookupBuilder(int min_tensor_size)
      : min_tensor_size_(min_tensor_size), all_success_(true), pending_(0) {}

  void ConvertAndInsert(const tensorflow::TensorProto* proto) {
    TryAddTensor(proto);
  }

  absl::StatusOr<TensorLookup> Build() {
    absl::MutexLock lock(&mu_);
    auto done = [this]() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
      return !all_success_ || pending_ == 0;
    };
    mu_.Await(absl::Condition(&done));
    if (!all_success_) {
      return absl::InternalError("Failed to parse TensorProto.");
    }
    return std::move(lookup_);
  }

 private:
  void TryAddTensor(const tensorflow::TensorProto* proto) {
    tensorflow::Tensor tensor;
    bool success = tensor.FromProto(*proto);
    {
      absl::MutexLock lock(&mu_);
      all_success_ = all_success_ && success;
      pending_--;
      if (success) {
        lookup_[proto] = std::move(tensor);
      }
    }
  }

  int min_tensor_size_;

  absl::Mutex mu_;
  TensorLookup lookup_ ABSL_GUARDED_BY(mu_);
  bool all_success_ ABSL_GUARDED_BY(mu_);
  int pending_ ABSL_GUARDED_BY(mu_);
};

absl::Status CreateTensorLookup(const SerializedObject& buffer,
                                TensorLookupBuilder* builder) {
  switch (buffer.payload_case()) {
    case SerializedObject::kNoneValue:
    case SerializedObject::kIntValue:
    case SerializedObject::kDoubleValue:
    case SerializedObject::kBoolValue:
    case SerializedObject::kStringValue:
    case SerializedObject::kUnicodeValue:
    case SerializedObject::kTypeValue:
    case SerializedObject::PAYLOAD_NOT_SET:
      return absl::OkStatus();

    case SerializedObject::kJaxTensorValue: {
      builder->ConvertAndInsert(&buffer.jax_tensor_value());
      return absl::OkStatus();
    }

    case SerializedObject::kTensorValue: {
      builder->ConvertAndInsert(&buffer.tensor_value());
      return absl::OkStatus();
    }

    case SerializedObject::kDictValue: {
      const SerializedDict& dict = buffer.dict_value();
      for (int i = 0; i < dict.keys_size(); ++i) {
        COURIER_RETURN_IF_ERROR(CreateTensorLookup(dict.values(i), builder));
      }
      return absl::OkStatus();
    }

    case SerializedObject::kListValue: {
      const SerializedList& list = buffer.list_value();
      for (int i = 0; i < list.items_size(); ++i) {
        COURIER_RETURN_IF_ERROR(CreateTensorLookup(list.items(i), builder));
      }
      return absl::OkStatus();
    }

    case SerializedObject::kReducedObjectValue: {
      COURIER_RETURN_IF_ERROR(
          CreateTensorLookup(buffer.reduced_object_value().args(), builder));

      // Maybe deserialize and set state.
      if (buffer.reduced_object_value().has_state() &&
          buffer.reduced_object_value().state().payload_case() !=
              SerializedObject::kNoneValue) {
        COURIER_RETURN_IF_ERROR(
            CreateTensorLookup(buffer.reduced_object_value().state(), builder));
      }

      // Maybe deserialize and set items.
      if (buffer.reduced_object_value().has_items() &&
          buffer.reduced_object_value().items().payload_case() !=
              SerializedObject::kNoneValue) {
        COURIER_RETURN_IF_ERROR(
            CreateTensorLookup(buffer.reduced_object_value().items(), builder));
      }

      // Maybe deserialize and set key/value pairs.
      if (buffer.reduced_object_value().has_kvpairs() &&
          buffer.reduced_object_value().kvpairs().payload_case() !=
              SerializedObject::kNoneValue) {
        COURIER_RETURN_IF_ERROR(CreateTensorLookup(
            buffer.reduced_object_value().kvpairs(), builder));
      }
      return absl::OkStatus();
    }
  }
}

}  // namespace

absl::StatusOr<TensorLookup> CreateTensorLookup(const SerializedObject& buffer,
                                                size_t min_tensor_size) {
  TensorLookupBuilder builder(min_tensor_size);
  COURIER_RETURN_IF_ERROR(CreateTensorLookup(buffer, &builder));
  return builder.Build();
}

absl::StatusOr<TensorLookup> CreateTensorLookup(
    const CallArguments& call_arguments, size_t min_tensor_size) {
  TensorLookupBuilder builder(min_tensor_size);

  for (const auto& buffer : call_arguments.args()) {
    COURIER_RETURN_IF_ERROR(CreateTensorLookup(buffer, &builder));
  }

  for (const auto& kwargs : call_arguments.kwargs()) {
    COURIER_RETURN_IF_ERROR(CreateTensorLookup(kwargs.second, &builder));
  }

  return builder.Build();
}

}  // namespace courier
