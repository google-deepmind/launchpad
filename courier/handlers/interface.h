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

#ifndef COURIER_HANDLERS_INTERFACE_H_
#define COURIER_HANDLERS_INTERFACE_H_

#include <functional>
#include <string>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "courier/serialization/serialization.pb.h"

namespace courier {

// Interface for a handler. The Call function is executed when an endpoint is
// called.
//
// A handler will be called concurrently and thus needs to be thread-safe. A
// handler must only be destroyed once all calls have returned.
class HandlerInterface {
 public:
  virtual ~HandlerInterface() = default;

  // `endpoint` is the method name that was called on the server.
  virtual absl::StatusOr<CallResult> Call(absl::string_view endpoint,
                                          const CallArguments& arguments) = 0;
};

}  // namespace courier

#endif  // COURIER_HANDLERS_INTERFACE_H_
