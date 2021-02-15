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

#ifndef COURIER_SERIALIZATION_PYOBJECT_PTR_H_
#define COURIER_SERIALIZATION_PYOBJECT_PTR_H_

#include "pybind11/pybind11.h"

#include <memory>

namespace courier {

struct DecrementsPyRefcount {
  void operator()(PyObject *ptr) const { Py_DECREF(ptr); }
};

// PyObjectPtr wraps an underlying Python object and decrements the
// reference count in the destructor.
//
// This class does not acquire the GIL in the destructor, so the GIL must be
// held when the destructor is called.
using PyObjectPtr = std::unique_ptr<PyObject, DecrementsPyRefcount>;

};  // namespace courier

#endif  // COURIER_SERIALIZATION_PYOBJECT_PTR_H_
