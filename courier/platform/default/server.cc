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

#include "courier/server.h"

#include <cstdlib>
#include <memory>
#include <utility>

#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "courier/platform/default/courier_service_impl.h"
#include "courier/platform/logging.h"
#include "courier/platform/status_macros.h"
#include "courier/router.h"
#include "grpcpp/client_context.h"
#include "grpcpp/security/server_credentials.h"
#include "grpcpp/server_builder.h"
#include "courier/platform/grpc_utils.h"

namespace courier {

class ServerImpl : public Server {
 public:
  ServerImpl(Router* router, int port) : port_(port), service_(router) {}

  ~ServerImpl() { Stop().IgnoreError(); }

  absl::Status Start() {
    grpc::ServerBuilder builder;
    builder.AddListeningPort(absl::StrCat("[::]:", port_),
                             MakeServerCredentials());
    builder.RegisterService(&service_);
    builder.SetMaxMessageSize(std::numeric_limits<int32_t>::max());
    builder.AddChannelArgument(GRPC_ARG_ALLOW_REUSEPORT, 0);
    grpc_server_ = builder.BuildAndStart();
    if (!grpc_server_) {
      return absl::Status(absl::StatusCode::kUnknown,
                          "Failed to start Courier gRPC server.");
    }
    LOG(INFO) << "Courier server on port " << port_ << " started successfully.";
    return absl::OkStatus();
  }

  absl::Status Stop() override {
    if (grpc_server_) {
      grpc_server_->Shutdown();
      LOG(INFO) << "Courier server on port " << port_ << " shutting down.";
    }
    return absl::OkStatus();
  }

  absl::Status Join() override {
    COURIER_CHECK(grpc_server_ != nullptr);
    grpc_server_->Wait();
    return absl::OkStatus();
  }

  void SetIsHealthy(bool is_healthy) override {
    COURIER_CHECK(grpc_server_ != nullptr);
    grpc_server_->GetHealthCheckService()->SetServingStatus(is_healthy);
  }

 private:
  // Port used for hosting the gRPC server.
  int port_;

  // gRPC service implementation.
  CourierServiceImpl service_;

  // gRPC server.
  std::unique_ptr<grpc::Server> grpc_server_;
};

absl::StatusOr<std::unique_ptr<Server>> Server::BuildAndStart(
    Router* router, int port, int thread_pool_size) {
  auto server = std::make_unique<ServerImpl>(router, port);
  COURIER_RETURN_IF_ERROR(server->Start());
  return absl::WrapUnique<Server>(server.release());
}
}  // namespace courier
