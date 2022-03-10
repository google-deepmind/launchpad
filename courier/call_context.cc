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

#include "courier/call_context.h"

#include "absl/memory/memory.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "courier/platform/logging.h"

namespace courier {

CallContext::CallContext(absl::Duration timeout, bool wait_for_ready,
                         bool compress, bool interruptible, bool chunk_tensors)
    : deadline_(timeout == absl::ZeroDuration() ? absl::InfiniteFuture()
                                                : absl::Now() + timeout),
      wait_for_ready_(wait_for_ready),
      compress_(compress),
      chunk_tensors_(chunk_tensors),
      cancelled_(false),
      context_(NewContext()) {
}

CallContext::~CallContext() {
}

void CallContext::Cancel() {
  absl::WriterMutexLock lock(&mu_);
  cancelled_ = true;
  context_->TryCancel();
}

void CallContext::Reset() {
  absl::WriterMutexLock lock(&mu_);
  context_ = NewContext();
  if (cancelled_) context_->TryCancel();
}

grpc::ClientContext* CallContext::context() {
  absl::ReaderMutexLock lock(&mu_);
  return context_.get();
}

std::unique_ptr<grpc::ClientContext> CallContext::NewContext() const {
  auto context = absl::make_unique<grpc::ClientContext>();

  context->set_compression_algorithm(
      compress_ ? grpc_compression_algorithm::GRPC_COMPRESS_GZIP
                : grpc_compression_algorithm::GRPC_COMPRESS_NONE);

  context->set_wait_for_ready(wait_for_ready_);
  context->set_deadline(absl::ToChronoTime(deadline_));

  return context;
}

}  // namespace courier
