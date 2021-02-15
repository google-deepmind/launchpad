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

#ifndef COURIER_ADDRESS_INTERCEPTOR_H_
#define COURIER_ADDRESS_INTERCEPTOR_H_

#include <string>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"

namespace courier {

// Interceptor for Courier's address resolution. `RegisterName()` calls.
// There is one global interceptor singleton, and the interceptor can be either
// disabled or enabled. It starts disabled and can only transition to enabled.
//
// `SetRedirect()` and will *NOT* register with real BNS if the interceptor was
// enabled. `Client::TryInit()` will wait for the interceptor's redirect if the
// interceptor was enabled.

class AddressInterceptor {
 public:
  // This constructor is for internal use only; users should use the singleton
  // function below.
  AddressInterceptor() = default;

  // Enables this interceptor. Only the first call has an effect. Thread-safe.
  void Enable();

  // Has no effect and returns if the interceptor has not been enabled.
  // Otherwise, writes the `address` into the redirect map. If the entry already
  // exists then the entry is overwritten. Thread-safe.
  bool SetRedirect(absl::string_view address, std::string redirect_to);

  // Has no effect and returns if the interceptor has not been enabled or the
  // address does not contain '/courier/'. Otherwise, gets the entry
  // corresponding to `address` from the redirect map. This function will block
  // until the entry exists and periodically print a warning in the console.
  // Thread-safe.
  bool GetRedirect(absl::string_view address, std::string* redirect_to) const;

 private:
  bool enabled_ ABSL_GUARDED_BY(mu_) = false;
  absl::flat_hash_map<std::string, std::string> redirects_ ABSL_GUARDED_BY(mu_);
  mutable absl::Mutex mu_;
};

// The global address interceptor shared by all Courier servers and clients in
// a process.
AddressInterceptor& InterceptorSingleton();

}  // namespace courier

#endif  // COURIER_ADDRESS_INTERCEPTOR_H_
