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

#include "courier/platform/default/courier_service_impl.h"

#include <algorithm>
#include <iterator>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "courier/courier_service.pb.h"
#include "courier/platform/logging.h"
#include "courier/platform/status_macros.h"
#include "courier/router.h"
#include "courier/serialization/serialization.pb.h"

namespace courier {

inline grpc::Status ToGrpcStatus(const absl::Status& s) {
  if (s.ok()) return grpc::Status::OK;

  return grpc::Status(static_cast<grpc::StatusCode>(s.code()),
                      std::string(s.message()));
}

CourierServiceImpl::CourierServiceImpl(Router* router) : router_(router) {
  COURIER_CHECK(router_ != nullptr);
}

grpc::Status CourierServiceImpl::Call(::grpc::ServerContext* context,
                                      const CallRequest* request,
                                      CallResponse* reply) {
  absl::StatusOr<courier::CallResult> result =
      router_->Call(request->method(), request->arguments());
  if (result.ok()) {
    *reply->mutable_result() = std::move(result).value();
    return grpc::Status();
  } else {
    return ToGrpcStatus(result.status());
  }
}

grpc::Status CourierServiceImpl::ListMethods(::grpc::ServerContext* context,
                                             const ListMethodsRequest* request,
                                             ListMethodsResponse* reply) {
  const auto& names = router_->Names();
  *reply->mutable_methods() =
      google::protobuf::RepeatedPtrField<std::string>(names.begin(), names.end());
  return grpc::Status();
}

}  // namespace courier
