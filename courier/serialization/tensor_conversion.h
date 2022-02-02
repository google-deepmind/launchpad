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

#ifndef THIRD_PARTY_PY_COURIER_SERIALIZATION_TENSOR_CONVERSION_H_
#define THIRD_PARTY_PY_COURIER_SERIALIZATION_TENSOR_CONVERSION_H_

#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "courier/serialization/serialization.pb.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor.pb.h"

namespace courier {

using TensorLookup =
    ::absl::flat_hash_map<const tensorflow::TensorProto*, tensorflow::Tensor>;

// The minimum size required for the tensor to be deserialized in a background.
// Smaller tensors will be deserialized on the calling thread.
// We apply this constraint as the lookup may be built in parallel
// by more than one thread. The latency of the context switch is therefore
// comparable (if not worse) than simply unpacking the tensor on the calling
// thread.
//
// The value of 128kB was estimated from a simple benchmark running on one
// machine. The system seems to be robust against changes of this value. Values
// within the [10kb, 1M] all had similar performance characteristic so further
// tuning is unlikely to provide massive improvements.
constexpr auto kDefaultMinTensorSizeBytes = 128 * 1024;

// Unpack `tensorflow::TensorProto` into `tensorflow::Tensor`. The conversion
// often require the entire content of the tensor buffer to be copied which can
// be slow for large tensors. This process does however not require the GIL to
// be held so we can reduce contention by creating the tensors before the GIL
// is acquired.
absl::StatusOr<TensorLookup> CreateTensorLookup(
    const SerializedObject& buffer,
    size_t min_tensor_size = kDefaultMinTensorSizeBytes);
absl::StatusOr<TensorLookup> CreateTensorLookup(
    const CallArguments& call_arguments,
    size_t min_tensor_size = kDefaultMinTensorSizeBytes);

}  // namespace courier

#endif  // THIRD_PARTY_PY_COURIER_SERIALIZATION_TENSOR_CONVERSION_H_
