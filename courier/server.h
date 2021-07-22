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

#ifndef COURIER_PLATFORM_SERVER_H_
#define COURIER_PLATFORM_SERVER_H_

#include <functional>
#include <memory>
#include <string>
#include <tuple>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "courier/router.h"
#include "courier/tf_serialize.h"

namespace courier {

// Server provides an RPC server with method handlers which can be defined at
// runtime. Method handlers are registered with a name and can have arbitrary
// signatures. On the wire, arguments and return values are serialized with
// the proto schema defined in serialization.proto. The thread pool size
// determines how many method handlers can be executed concurrently.
//
// This class can either be used in its own right or from Python via Pybind11
// bindings (see courier/python/server.cc). The Pybind11 API defines
// the semantics of the server lifetime operations (Start, Stop, Join)
// and the method handler operations (Bind, Unbin, ListMethods, Call).
// For implementation details on the latter see courier_service.h.
//
// Summary: all operations are thread-safe, Start and Stop must only be called
// once, and Bind/Unbind serializes with Call. For best performance, don't call
// Bind/Unbind in the hot path. Uninterrupted concurrent calls to Call do not
// block one another.
//
class Server {
 public:
  virtual ~Server() = default;

  // Shuts down then server by requesting termination and joining (so the call
  // may block). Thread-safe.
  virtual absl::Status Stop() = 0;

  // Joins the server. This call blocks until the server has been terminated and
  // all pending requests have been served. A call to this function constitutes
  // a typical "main loop" of a server that does not do anything else. (If the
  // application already has its own main loop, joining may not be useful, and
  // Stop may be used when the application's business has concluded.)
  //
  // Join is thread-safe and may be called repeatedly.
  virtual absl::Status Join() = 0;

  // Updates the RPC server's health status. An unhealthy server still accepts
  // requests but prefers not to serve. This is particularly useful when
  // load-balancing requests over several servers. See
  // https://github.com/grpc/grpc/blob/master/doc/health-checking.md for more
  // information.
  virtual void SetIsHealthy(bool is_healthy) = 0;

  // Constructs Courier server.
  static absl::StatusOr<std::unique_ptr<Server>> BuildAndStart(
      Router* router, int port, int thread_pool_size = 16);
};

}  // namespace courier

#endif  // COURIER_PLATFORM_SERVER_H_
