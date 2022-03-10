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

#ifndef COURIER_CLIENT_H_
#define COURIER_CLIENT_H_

#include <functional>
#include <memory>
#include <string>
#include <thread>  // NOLINT
#include <tuple>
#include <utility>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "grpcpp/grpcpp.h"
#include "courier/call_context.h"
#include "courier/courier_service.grpc.pb.h"
#include "courier/courier_service.pb.h"
#include "courier/platform/client_monitor.h"
#include "courier/platform/status_macros.h"
#include "courier/serialization/serialization.pb.h"
#include "courier/serialization/serialize.h"

namespace courier {

// Client implements the client-side of the Courier RPC setup. It is used
// to call methods on a server. All member functions are thread-safe.
//
// Please note that the calls do not have any timeout or retry policy.
//
class Client {
 public:
  // Creates a new Client that will connect to a Server.
  explicit Client(absl::string_view server_address);

  ~Client();

  // Calls a method on the server. The caller retains ownership of `context`.
  // Blocks until the RPC call has finished, so the caller is not expected to
  // call `Wait()` on the `context`. If `CallContext::wait_for_ready` is true,
  // then `Unavailable` errors are automatically retried.
  absl::StatusOr<courier::CallResult> CallF(
      CallContext* context, absl::string_view method_name,
      std::unique_ptr<courier::CallArguments> arguments);

  // Calls a method on the server asynchronously. The caller retains ownership
  // of `context` which must not be deleted before `callback` is invoked. If
  // `CallContext::wait_for_ready` is true, then `Unavailable` errors are
  // automatically retried.
  void AsyncCallF(
      CallContext* context, absl::string_view method_name,
      std::unique_ptr<courier::CallArguments> arguments,
      std::function<void(absl::StatusOr<courier::CallResult>)> callback);

  // Calls a method on the server. The variadic arguments will be serialized
  // and the result from calling the method on the server will be deserialized
  // to the expected type. If `CallContext::wait_for_ready` is true, then
  // `Unavailable` errors are automatically retried.
  template <typename R, typename... Args>
  absl::StatusOr<R> Call(CallContext* context, absl::string_view method,
                         const Args&... args) {
    auto arguments = absl::make_unique<courier::CallArguments>();
    COURIER_RETURN_IF_ERROR(SerializeToRepeatedObject(
        std::forward_as_tuple(args...), arguments->mutable_args()));
    COURIER_ASSIGN_OR_RETURN(courier::CallResult result_proto,
                             CallF(context, method, std::move(arguments)));
    R result;
    COURIER_RETURN_IF_ERROR(
        DeserializeFromObject(result_proto.result(), &result));
    return result;
  }

  // Lists the methods available on the server.
  absl::StatusOr<std::vector<std::string>> ListMethods();

  // Not thread safe. Called by the destructor if not called explicitly.
  void Shutdown();

 private:
  friend class AsyncRequest;
  // Initializes the stub RPC client. This has to be called outside of the
  // constructor as it might block and we expect the constructor to be non
  // blocking. If the singleton `AddressInterceptor` was enabled, then we
  // consult the interceptor for a potential redirect before using the
  // `server_address` to create the stub.
  absl::Status TryInit(CallContext* context) ABSL_LOCKS_EXCLUDED(init_mu_);


  // Run by dedicated thread it polls on the competion queue.
  void cq_polling();

  grpc::CompletionQueue cq_;
  std::thread cq_thread_;
  bool shutdown_ = false;

  // Ensures initialization is only done once.
  absl::Mutex init_mu_;

  // Stored for logging.
  const std::string server_address_;

  // The RPC client channel and stub.
  std::shared_ptr<grpc::ChannelInterface> channel_;
  std::unique_ptr</* grpc_gen:: */CourierService::Stub> stub_;
};

class AsyncRequest {
 public:
  AsyncRequest(Client* client, CallContext* context,
               MonitoredCallScope* monitor, absl::string_view method_name,
               std::unique_ptr<CallArguments> arguments,
               std::function<void(absl::StatusOr<CallResult>)> callback);

  void Run();

  void Done(const ::grpc::Status& grpc_status);

 private:
  friend class Client;
  Client* client_;
  const std::function<void(absl::StatusOr<courier::CallResult>)> callback_;
  CallContext* context_;
  courier::CallRequest request_;
  courier::CallResponse response_;
  courier::MonitoredCallScope* monitor_;
  grpc::Status status_;
};
}  // namespace courier

#endif  // COURIER_CLIENT_H_
