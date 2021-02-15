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

#include "courier/router.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/synchronization/mutex.h"
#include "courier/handlers/interface.h"
#include "courier/serialization/serialization.pb.h"
#include "tensorflow/core/profiler/lib/traceme.h"

namespace courier {

absl::Status Router::Bind(absl::string_view method,
                          std::shared_ptr<HandlerInterface> method_handler) {
  if (method.empty()) {
    return absl::InvalidArgumentError("Bind method name must be non-empty");
  }
  if (method_handler == nullptr) {
    return absl::InvalidArgumentError("Bind method handler must be non-null.");
  }

  absl::WriterMutexLock lock(&mu_);
  handlers_[std::string(method)] =
      absl::make_unique<CallCountingHandler>(std::move(method_handler));
  return absl::OkStatus();
}

void Router::Unbind(absl::string_view method) {
  absl::WriterMutexLock lock(&mu_);
  handlers_.erase(std::string(method));
}

absl::StatusOr<courier::CallResult> Router::Call(
    absl::string_view method_name, const courier::CallArguments& arguments) {
  tensorflow::profiler::TraceMe trace_me(method_name);
  CallCountingHandler* handler = nullptr;
  {
    absl::ReaderMutexLock lock(&mu_);
    auto func_it = handlers_.find(std::string(method_name));
    if (func_it == handlers_.end()) {
      func_it = handlers_.find("*");
    }
    if (func_it == handlers_.end()) {
      return absl::Status(absl::StatusCode::kNotFound,
                          absl::StrCat("method ", method_name, " not found"));
    }
    handler = func_it->second.get();
    absl::MutexLock handler_lock(&handler->mu_);
    handler->inflight_calls_++;
  }
  auto result = handler->handler_->Call(method_name, arguments);
  absl::MutexLock handler_lock(&handler->mu_);
  handler->inflight_calls_--;
  return result;
}

std::vector<std::string> Router::Names() {
  absl::ReaderMutexLock lock(&mu_);
  std::vector<std::string> names;
  names.reserve(handlers_.size());
  for (const auto& item : handlers_) {
    names.push_back(item.first);
  }
  return names;
}

Router::CallCountingHandler::CallCountingHandler(
    std::shared_ptr<HandlerInterface> handler)
    : handler_(std::move(handler)) {}

Router::CallCountingHandler::~CallCountingHandler() {
  absl::MutexLock lock(&mu_);
  mu_.Await(
      absl::Condition(this, &Router::CallCountingHandler::NoInflightCalls));
}

}  // namespace courier
