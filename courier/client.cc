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

#include "courier/client.h"

#include <cstdint>
#include <functional>
#include <iterator>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "grpcpp/channel.h"
#include "grpcpp/client_context.h"
#include "grpcpp/create_channel.h"
#include "absl/base/optimization.h"
#include "absl/functional/bind_front.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "courier/address_interceptor.h"
#include "courier/call_context.h"
#include "courier/courier_service.pb.h"
#include "courier/platform/grpc_utils.h"
#include "courier/platform/logging.h"
#include "courier/platform/status_macros.h"
#include "courier/serialization/serialization.pb.h"


constexpr char kStreamRemovedMessage[] = "Stream removed";

inline bool IsStreamRemovedError(const ::grpc::Status& s) {
  return !s.ok() && s.error_code() == ::grpc::StatusCode::UNKNOWN &&
         s.error_message() == kStreamRemovedMessage;
}

inline absl::Status FromGrpcStatus(const grpc::Status& s) {
  if (s.ok()) return absl::OkStatus();

  // Convert "UNKNOWN" stream removed errors into unavailable, to allow
  // for retry upstream.
  if (IsStreamRemovedError(s)) {
    return absl::UnavailableError(s.error_message());
  }
  return absl::Status(static_cast<absl::StatusCode>(s.error_code()),
                      s.error_message());
}

namespace courier {

bool IsRetryable(const absl::Status& status) {
  return absl::IsUnavailable(status);
}

AsyncRequest::AsyncRequest(
    Client* client, CallContext* context, MonitoredCallScope* monitor,
    absl::string_view method_name, std::unique_ptr<CallArguments> arguments,
    std::function<void(absl::StatusOr<CallResult>)> callback)
    : client_(client),
      callback_(callback),
      context_(context),
      monitor_(monitor) {
  request_.set_method(std::string(method_name));
  request_.set_allocated_arguments(arguments.release());
}

void AsyncRequest::Run() {
  std::unique_ptr<grpc::ClientAsyncResponseReader<CallResponse>> rpc(
      client_->stub_->PrepareAsyncCall(context_->context(), request_,
                                       &client_->cq_));
  rpc->StartCall();
  rpc->Finish(&response_, &status_, (void*)this);
}

void AsyncRequest::Done(const ::grpc::Status& grpc_status) {
  absl::Status status = FromGrpcStatus(grpc_status);
  if (IsRetryable(status) && context_->wait_for_ready()) {
    context_->Reset();
    Run();
  } else {
    delete monitor_;
    if (status.ok()) {
      callback_(std::move(*response_.mutable_result()));
    } else {
      callback_(status);
    }
    delete this;
  }
}

void Client::cq_polling() {
  void* tag;
  bool ok = false;
  while (cq_.Next(&tag, &ok)) {
    AsyncRequest* request = static_cast<AsyncRequest*>(tag);
    COURIER_CHECK(ok);
    request->Done(request->status_);
  }
}

Client::Client(absl::string_view server_address)
    :
      cq_thread_(&Client::cq_polling, this),
      server_address_(server_address) {
  ClientCreation();
}

Client::~Client() { Shutdown(); }

void Client::Shutdown() {
  if (!shutdown_) {
    shutdown_ = true;
    cq_.Shutdown();
    cq_thread_.join();
  }
}

absl::StatusOr<courier::CallResult> Client::CallF(
    CallContext* context, absl::string_view method_name,
    std::unique_ptr<courier::CallArguments> arguments) {
  COURIER_RETURN_IF_ERROR(TryInit(context));

  CallRequest request;
  request.set_method(std::string(method_name));
  request.set_allocated_arguments(arguments.release());
  COURIER_CHECK(stub_);
  CallResponse response;

  auto monitor =
      BuildCallMonitor(channel_.get(), request.method(), server_address_);
  while (true) {
    absl::Status status =
        FromGrpcStatus(stub_->Call(context->context(), request, &response));

    if (!IsRetryable(status) || !context->wait_for_ready()) {
      COURIER_RETURN_IF_ERROR(status);
      break;
    }
    context->Reset();
  }
  return std::move(response).result();
}


void Client::AsyncCallF(
    CallContext* context, absl::string_view method_name,
    std::unique_ptr<courier::CallArguments> arguments,
    std::function<void(absl::StatusOr<courier::CallResult>)> callback) {
  absl::Status status = TryInit(context);
  if (!status.ok()) {
    callback(status);
    return;
  }

  COURIER_CHECK(stub_);
  auto monitor = BuildCallMonitor(channel_.get(), std::string(method_name),
                                  server_address_);
  // Request deletes itself upon completion.
  AsyncRequest* request =
      new AsyncRequest(this, context, monitor.release(), method_name,
                       std::move(arguments), callback);
  request->Run();
}

absl::StatusOr<std::vector<std::string>> Client::ListMethods() {
  CallContext context;
  COURIER_RETURN_IF_ERROR(TryInit(&context));
  COURIER_CHECK(stub_);
  ListMethodsRequest request;
  ListMethodsResponse response;
  COURIER_RETURN_IF_ERROR(FromGrpcStatus(
      stub_->ListMethods(context.context(), request, &response)));
  return std::vector<std::string>(
      std::make_move_iterator(response.mutable_methods()->begin()),
      std::make_move_iterator(response.mutable_methods()->end()));
}

absl::Status Client::TryInit(CallContext* context) {
  {
    absl::ReaderMutexLock lock(&init_mu_);
    if (stub_) return absl::OkStatus();
  }
  absl::WriterMutexLock lock(&init_mu_);
  if (stub_) return absl::OkStatus();

  std::string address;
  if (!InterceptorSingleton().GetRedirect(server_address_, &address)) {
    address = server_address_;
  }

  grpc::ChannelArguments channel_args;
  channel_args.SetInt(GRPC_ARG_MAX_RECEIVE_MESSAGE_LENGTH, -1);
  channel_args.SetInt(GRPC_ARG_MAX_SEND_MESSAGE_LENGTH, -1);
  channel_args.SetInt(GRPC_ARG_MAX_METADATA_SIZE, 16 * 1024 * 1024);


  channel_ =
      CreateCustomGrpcChannel(address, MakeChannelCredentials(), channel_args);
  stub_ = /* grpc_gen:: */CourierService::NewStub(channel_);

  return absl::OkStatus();
}

}  // namespace courier
