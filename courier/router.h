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

#ifndef COURIER_ROUTER_H_
#define COURIER_ROUTER_H_

#include <functional>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/synchronization/notification.h"
#include "courier/handlers/interface.h"
#include "courier/serialization/serialization.pb.h"

namespace courier {

// Container for method handlers.
//
// The class allows method handlers to be registered by name at runtime. The
// Router.Call function looks up the requested method by name and
// executes the corresponding MethodHandler.Call function. Serialization of user
// data is left to a higher layer.
//
// Thread safety: All public functions (Call, Names, Bind and Unbind) are
// allowed to be called concurrently. The binding functions (Bind, Unbind) are
// serialized with respect to all functions. The handlers' call functions
// may be called concurrently with respect to one another and are allowed to
// transitively call Bind/Unbind.
//
// Note(tkoeppe): That is to say, the binding functions form a "bottleneck" for
// concurrent Call()s. This is a constraint that we may be able to relax by
// making the locking more granular, e.g. via per-row or sharded locking.
// However, such changes should be well motivated by benchmarks, since they are
// significantly more complex than a simple, single mutex.
class Router {
 public:
  // Binds a method handler to a method name. The Router.Call function
  // will execute the Call function of the corresponding MethodHandler.
  //
  // If a name has already been bound to a handler, the original handler is
  // replaced by the new handler. Note: `method_name` must not be empty and
  // `method_handler` must not be null.
  //
  // If a method handler with the same name was registered before, then this
  // function will not block until concurrent calls to that handler have
  // finished. Instead, the new handler will be installed and handle incoming
  // calls from then on out.
  absl::Status Bind(absl::string_view method,
                    std::shared_ptr<HandlerInterface> method_handler)
      ABSL_LOCKS_EXCLUDED(mu_);


  // Deletes the function handler registed under the given name, if any;
  // there is no effect if no handler is registered under that name.
  //
  // This function is serialized with respect to other calls of all public
  // member functions.
  void Unbind(absl::string_view method) ABSL_LOCKS_EXCLUDED(mu_);

  // Looks up the requested method handler. If no method is registered under the
  // requested name, a NOT_FOUND error is returned. This function blocks until
  // concurrent calls to the binding functions have completed.
  absl::StatusOr<std::shared_ptr<HandlerInterface>> Lookup(
      absl::string_view method_name) ABSL_LOCKS_EXCLUDED(mu_);

  // Returns a list of the names of all registered method handlers.
  // The returned list is advisory only. Presence on the list does not imply
  // that a call under that name will succeed, nor does absence from the list
  // imply that a call will fail.
  //
  // This function blocks until concurrent calls of the binding functions have
  // completed.
  std::vector<std::string> Names() ABSL_LOCKS_EXCLUDED(mu_);

 private:
  // Stores bound method names and their method handles.
  std::map<std::string, std::shared_ptr<HandlerInterface>> handlers_
      ABSL_GUARDED_BY(mu_);
  absl::Mutex mu_;
};

}  // namespace courier

#endif  // COURIER_ROUTER_H_
