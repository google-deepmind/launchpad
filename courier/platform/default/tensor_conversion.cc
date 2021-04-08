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

#include "courier/platform/tensor_conversion.h"

#include "absl/status/statusor.h"
#include "courier/serialization/serialization.pb.h"

namespace courier {

absl::StatusOr<TensorLookup> CreateTensorLookup(const SerializedObject& buffer,
                                                size_t min_tensor_size) {
  // Tensors (aka ndarrays) are packed using __reduce__ in the OSS version so
  // there is no value in building the lookup.
  return TensorLookup();
}

absl::StatusOr<TensorLookup> CreateTensorLookup(
    const CallArguments& call_arguments, size_t min_tensor_size) {
  // Tensors (aka ndarrays) are packed using __reduce__ in the OSS version so
  // there is no value in building the lookup.
  return TensorLookup();
}

}  // namespace courier
