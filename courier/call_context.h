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

#ifndef COURIER_CALL_CONTEXT_H_
#define COURIER_CALL_CONTEXT_H_

#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "grpcpp/grpcpp.h"

namespace courier {

// Container for the `grpc::ClientContext` context. To be used in combination
// with `Client` when making calls. `Reset()` must be called before a
// `CallContext` is used across different calls.
class CallContext {
 public:
  // `timeout` and `wait_for_ready` are used to set the respective attributes on
  // the `grpc::ClientContext` context. If `interruptible` is set, registers
  // interrupt signal handler to initiate cancellation on CTRL-C.
  explicit CallContext(absl::Duration timeout = absl::ZeroDuration(),
                       bool wait_for_ready = true, bool compress = false,
                       bool interruptible = false, bool chunk_tensors = false);

  // Removes the signal handler and destroys the context. Should only be called
  // once the grpc::ClientContext call has completed.
  ~CallContext();

  // Does not block. No-op if the call has already been cancelled. See
  // `grpc::ClientContext::TryCancel` for detailed semantics. Thread-safe.
  void Cancel();

  // ONLY FOR INTERNAL USE.
  // Destroys the existing context and creates a new one. This is used for
  // implementing retry. If `Cancel()` is called before `Reset()`, then the new
  // context will also be cancelled. Thread-safe.
  void Reset();

  // ONLY FOR INTERNAL USE.
  grpc::ClientContext* context();

  bool wait_for_ready() const { return wait_for_ready_; }
  bool chunk_tensors() const { return chunk_tensors_; }

 private:
  std::unique_ptr<grpc::ClientContext> NewContext() const;

  // Used to set `deadline` when creating a new context.
  const absl::Time deadline_;

  // Used to set `fail_fast` when creating a new context.
  const bool wait_for_ready_;

  // Used to set compression attributes when creating a new context.
  const bool compress_;

  // Feature unsupported at the moment.
  const bool chunk_tensors_;


  // The GRPC client context.
  bool cancelled_ ABSL_GUARDED_BY(mu_);
  std::unique_ptr<grpc::ClientContext> context_ ABSL_GUARDED_BY(mu_);
  absl::Mutex mu_;
};

}  // namespace courier

#endif  // COURIER_CALL_CONTEXT_H_
