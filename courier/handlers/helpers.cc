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

#include "courier/handlers/helpers.h"

#include "courier/platform/default/py_utils.h"
#include "courier/platform/logging.h"

absl::StatusCode PythonExceptionErrorCode() {
  if (!PyErr_Occurred()) {
    return absl::StatusCode::kUnknown;
  }
  if (PyErr_ExceptionMatches(PyExc_ValueError) ||
      PyErr_ExceptionMatches(PyExc_TypeError)) {
    return absl::StatusCode::kInvalidArgument;
  }
  if (PyErr_ExceptionMatches(PyExc_StopIteration)) {
    return absl::StatusCode::kOutOfRange;
  }
  if (PyErr_ExceptionMatches(PyExc_MemoryError)) {
    return absl::StatusCode::kResourceExhausted;
  }
  if (PyErr_ExceptionMatches(PyExc_NotImplementedError)) {
    return absl::StatusCode::kUnimplemented;
  }
  if (PyErr_ExceptionMatches(PyExc_KeyboardInterrupt)) {
    return absl::StatusCode::kAborted;
  }
  if (PyErr_ExceptionMatches(PyExc_SystemError) ||
      PyErr_ExceptionMatches(PyExc_SyntaxError)) {
    return absl::StatusCode::kInternal;
  }
  if (PyErr_ExceptionMatches(PyExc_LookupError)) {
    return absl::StatusCode::kNotFound;
  }
  return absl::StatusCode::kUnknown;
}

absl::Status ReturnPythonException() {
  std::string error_prefix = "Python exception was raised on the server";
  absl::StatusCode status_code = PythonExceptionErrorCode();
  std::string exception;
  if (PythonUtils::FetchPendingException(&exception)) {
    std::string error_message = absl::StrCat(error_prefix, ":\n", exception);
    COURIER_LOG(COURIER_ERROR) << error_message;
    return absl::Status(status_code, error_message);
  }
  return absl::InternalError(absl::StrCat(
      error_prefix, " but the exception message could not be caught.   "));
}
