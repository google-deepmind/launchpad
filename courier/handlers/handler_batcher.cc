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

#include "courier/handlers/handler_batcher.h"

#include <queue>

#include "absl/status/status.h"
#include "absl/synchronization/notification.h"
#include "courier/platform/status_macros.h"
#include "courier/serialization/batching.h"
#include "tensorflow/core/profiler/lib/traceme.h"

namespace courier {
namespace {

struct Batch {
 public:
  Batch(int batch_size) : enqueued_for_execution_(false) {
    requests_.reserve(batch_size);
  }

  bool enqueued_for_execution_;
  absl::Notification ready_for_execution_;
  absl::Notification executed_;
  std::vector<const courier::CallArguments*> requests_;
  std::vector<courier::CallResult> results_;
  absl::Status result_status_;
};


// Design: the internal `Batch` is the currently being built one. The first
// thread which will start the batch will be responsible for executing it
// if the timeout expires, while the thread filling the last item of the batch
// will be responsible for executing it in that case.
class BatchedPyCallHandler : public HandlerInterface {
 public:
  BatchedPyCallHandler(absl::string_view endpoint,
                       std::shared_ptr<HandlerInterface> handler,
                       int batch_size, int max_parallelism,
                       absl::Duration timeout, bool pad_batch)
      : endpoint_(endpoint), handler_(std::move(handler)), inflight_batches_(0),
        batch_size_(batch_size), max_parallelism_(max_parallelism),
        timeout_(timeout), pad_batch_(pad_batch) {
    current_batch_ = std::make_shared<Batch>(batch_size_);
  }

  bool EnqueueForExecution(Batch* batch) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    batch->enqueued_for_execution_ = true;
    if (++inflight_batches_ > max_parallelism_) {
      queued_batches_.push(batch);
      return false;
    } else {
      batch->ready_for_execution_.Notify();
      return true;
    }
  }

  absl::StatusOr<courier::CallResult> Call(
      absl::string_view endpoint,
      const courier::CallArguments& arguments) override {
    if (endpoint != endpoint_) {
      return absl::InvalidArgumentError("Mismatched endpoint name.");
    }
    std::shared_ptr<Batch> my_batch;
    int request_idx;
    bool execute_batch = false;
    {
      absl::MutexLock lock(&mu_);
      my_batch = current_batch_;
      request_idx = my_batch->requests_.size();
      my_batch->requests_.push_back(&arguments);
      if (request_idx == batch_size_ - 1) {
        current_batch_ = std::make_shared<Batch>(batch_size_);
        // The batch is already enqueued iff the timeout was exceeded.
        if (!my_batch->enqueued_for_execution_) {
          EnqueueForExecution(my_batch.get());
          execute_batch = true;
        }
      }
    }
    if (request_idx == 0 && !execute_batch) {
      // Caller adding the first element to the batch is responsible for
      // handling the timeout.
      if (!my_batch->executed_.WaitForNotificationWithTimeout(timeout_)) {
        absl::MutexLock lock(&mu_);
        // The batch is already enqueued iff it is full.
        if (!my_batch->enqueued_for_execution_) {
          if (EnqueueForExecution(my_batch.get())) {
            // Batch is to be executed immediately, don't allow any more
            // additions. Otherwise it is pending execution and we keep
            // adding to it until it is executed.
            current_batch_ = std::make_shared<Batch>(batch_size_);
          }
          execute_batch = true;
        }
      }
    }
    if (execute_batch) {
      my_batch->ready_for_execution_.WaitForNotification();
      my_batch->result_status_ = ExecuteBatch(endpoint, my_batch.get());
      my_batch->executed_.Notify();
      {
        absl::MutexLock lock(&mu_);
        inflight_batches_--;
        if (!queued_batches_.empty()) {
          auto to_run = queued_batches_.front();
          to_run->ready_for_execution_.Notify();
          if (to_run == current_batch_.get()) {
            // This batch is not full. It was enqueued for execution
            // because of timeout was exceeded. But it was pending
            // execution because maximum number of inflight batches
            // was exceeded. We kept adding to it until it is executed.
            current_batch_ = std::make_shared<Batch>(batch_size_);
          }
          queued_batches_.pop();
        }
      }
    }
    my_batch->executed_.WaitForNotification();
    if (my_batch->result_status_.ok()) {
      return my_batch->results_[request_idx];
    } else {
      return my_batch->result_status_;
    }
  }

 private:
  absl::Status ExecuteBatch(absl::string_view endpoint, Batch* batch) {
    tensorflow::profiler::TraceMe trace_me("ExecuteBatch");
    if (pad_batch_) {
      for (int x = batch->requests_.size(); x < batch_size_; x++) {
        batch->requests_.push_back(batch->requests_.front());
      }
    }
    const int batch_size = batch->requests_.size();
    const int arg_cnt = batch->requests_[0]->args_size();
    // Verify that all requests in a batch have the same number of args.
    for (const auto& request : batch->requests_) {
      if (request->args_size() != arg_cnt) {
        return absl::InternalError(
            "Requests in a batch do not have the same number of arguments.");
      }
    }

    // Deserialize args.
    CallArguments batched_call;
    for (int i = 0; i < arg_cnt; i++) {
      std::vector<const SerializedObject*> objects;
      objects.reserve(batch_size);
      for (int x = 0; x < batch_size; x++) {
        objects.push_back(
            const_cast<SerializedObject*>(&batch->requests_[x]->args(i)));
      }
      COURIER_RETURN_IF_ERROR(
          BatchSerializedObjects(objects, batched_call.add_args()));
    }

    absl::flat_hash_map<std::string,
                        std::vector<const SerializedObject*>> batched_kwargs;
    for (int i = 0; i < batch_size; i++) {
      for (auto &arg : batch->requests_[i]->kwargs()) {
        batched_kwargs[arg.first].push_back(&arg.second);
      }
    }
    for (auto& i : batched_kwargs) {
      if (i.second.size() != batch_size) {
        return absl::InternalError(
            "Mismatching keys in the kwargs to batch.");
      }
      courier::SerializedObject batched;
      COURIER_RETURN_IF_ERROR(
          BatchSerializedObjects(i.second, &batched));
      (*batched_call.mutable_kwargs())[i.first] = batched;
    }
    CallResult result;
    {
      tensorflow::profiler::TraceMe trace_me("BatchedHandler");
      COURIER_ASSIGN_OR_RETURN(result,
                               handler_->Call(endpoint, batched_call));
    }
    std::vector<SerializedObject*> objects;
    batch->results_.resize(batch_size);
    objects.reserve(batch_size);
    for (auto& result : batch->results_) {
      objects.push_back(result.mutable_result());
    }
    COURIER_RETURN_IF_ERROR(UnbatchSerializedObject(result.result(), objects));
    return absl::OkStatus();
  }

  absl::Mutex mu_;
  const std::string endpoint_;
  std::shared_ptr<HandlerInterface> handler_;
  std::shared_ptr<Batch> current_batch_ ABSL_GUARDED_BY(mu_);
  std::queue<Batch*> queued_batches_ ABSL_GUARDED_BY(mu_);
  int inflight_batches_ ABSL_GUARDED_BY(mu_);
  const int batch_size_;
  const int max_parallelism_;
  const absl::Duration timeout_;
  const bool pad_batch_;
};

}  // namespace

std::shared_ptr<HandlerInterface> BuildBatchedHandlerWrapper(
    absl::string_view endpoint, std::shared_ptr<HandlerInterface> py_func,
    int batch_size, int max_parallelism, absl::Duration timeout,
    bool pad_batch) {
  return std::make_shared<BatchedPyCallHandler>(endpoint, std::move(py_func),
                                                batch_size, max_parallelism,
                                                timeout, pad_batch);
}

}  // namespace courier
