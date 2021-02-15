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

#include "courier/handlers/py_call.h"

#include <pybind11/pybind11.h>

#include <memory>
#include <string>

#include "courier/platform/default/py_utils.h"
#include "absl/base/casts.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "courier/handlers/interface.h"
#include "courier/platform/logging.h"
#include "courier/platform/status_macros.h"
#include "courier/serialization/py_serialize.h"
#include "courier/serialization/serialization.pb.h"
          ;
      return result;
    } else {
      std::string error_prefix = "Python exception was raised on the server";
      absl::StatusCode status_code = PythonExceptionErrorCode();
      std::string exception;
      if (PythonUtils::FetchPendingException(&exception)) {
        std::string error_message =
            absl::StrCat(error_prefix, ":\n", exception);
        COURIER_LOG(COURIER_ERROR) << error_message;
        return absl::Status(status_code, error_message);
      }
      return absl::InternalError(absl::StrCat(
          error_prefix, " but the exception message could not be caught."));
    }
  }

 private:
  PyObject* py_func_;
};

}  // namespace

std::unique_ptr<HandlerInterface> BuildPyCallHandler(PyObject* py_func) {
  return absl::make_unique<PyCallHandler>(py_func);
}

}  // namespace courier
