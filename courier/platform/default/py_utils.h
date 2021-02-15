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

#ifndef THIRD_PARTY_PY_COURIER_PLATFORM_DEFAULT_PY_UTILS_H_
#define THIRD_PARTY_PY_COURIER_PLATFORM_DEFAULT_PY_UTILS_H_

#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>

namespace PythonUtils {

bool CPPString_FromPyString(PyObject* py_obj, std::string* output);
bool FetchPendingException(std::string* exception);

}  // namespace PythonUtils

#endif  // THIRD_PARTY_PY_COURIER_PLATFORM_DEFAULT_PY_UTILS_H_
