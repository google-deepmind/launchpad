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

#ifndef COURIER_COURIER_SERVICE_IMPL_H_
#define COURIER_COURIER_SERVICE_IMPL_H_

#include <memory>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"
#include "grpcpp/server_context.h"
#include "courier/courier_service.grpc.pb.h"
#include "courier/courier_service.pb.h"
#include "courier/router.h"

namespace courier {

// CourierServiceImpl implements an RPC server for the CourierService.
//
// Method handlers are invoked via CourierService.Call. Calls can be executed
// concurrently with respect to each other but are serialized with respect to
// Router.Bind and Router.Unbind.
//
// Note: Method handlers must not transitively call Bind/Unbind, which would
// deadlock.
class CourierServiceImpl final : public CourierService::Service {
 public:
  CourierServiceImpl(Router* router);

  // Calls the method handler registered under the name request->method()
  // and returns the result of the call over RPC. If no method is registered
  // under the requested name, a NOT_FOUND error is returned.
  //
  // This function blocks until the execution has completed.
  // calls of the binding functions have completed.
  grpc::Status Call(::grpc::ServerContext* context, const CallRequest* request,
                    CallResponse* reply) override;

  // Returns a list of the names of all registered method handlers over RPC.
  // The returned list is advisory only. Presence on the list does not imply
  // that a call under that name will succeed, nor does absence from the list
  // imply that a call will fail.
  //
  // This function blocks until concurrent calls of the binding functions have
  // completed.
  grpc::Status ListMethods(::grpc::ServerContext* context,
                           const ListMethodsRequest* request,
                           ListMethodsResponse* reply) override;

 private:
  // Method handlers which are executed on incoming Call requests.
  Router* router_;
};

}  // namespace courier

#endif  // COURIER_COURIER_SERVICE_IMPL_H_
