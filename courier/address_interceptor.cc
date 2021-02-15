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

#include "courier/address_interceptor.h"

#include <string>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "absl/strings/match.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "courier/platform/logging.h"


namespace courier {

void AddressInterceptor::Enable() {
  absl::WriterMutexLock lock(&mu_);
  enabled_ = true;
}

bool AddressInterceptor::SetRedirect(absl::string_view address,
                                     std::string redirect_to) {
  absl::WriterMutexLock lock(&mu_);
  if (!enabled_) return false;
  redirects_[address] = std::move(redirect_to);
  COURIER_LOG(COURIER_INFO)
      << "Courier address interceptor registered: " << address;
  return true;
}

bool AddressInterceptor::GetRedirect(absl::string_view address,
                                     std::string* redirect_to) const {
  if (!absl::StrContains(address, "/courier/")) {
    return false;
  }

  {
    absl::ReaderMutexLock lock(&mu_);
    if (!enabled_) return false;
  }
  while (true) {
    {
      absl::ReaderMutexLock lock(&mu_);
      auto it = redirects_.find(address);
      if (it != redirects_.end()) {
        *redirect_to = it->second;
        return true;
      }
    }
    COURIER_LOG(COURIER_WARNING)
        << "Courier address interceptor could not find: " << address;
    absl::SleepFor(absl::Seconds(5));
  }
  return true;
}

AddressInterceptor& InterceptorSingleton() {
  static AddressInterceptor singleton;
  return singleton;
}

}  // namespace courier
