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

#include "courier/platform/default/py_utils.h"

namespace PythonUtils {

bool CPPString_FromPyString(PyObject* py_obj, std::string* output) {
  Py_ssize_t len = -1;
  if (PyUnicode_Check(py_obj)) {
    const char* buf = PyUnicode_AsUTF8AndSize(py_obj, &len);
    if (buf == nullptr) {
      return false;
    }
    output->assign(buf, len);
    return true;
  }
  if (PyBytes_Check(py_obj)) {
    char* buf = nullptr;
    int result = PyBytes_AsStringAndSize(py_obj, &buf, &len);
    if (result == -1) {
      return false;
    }
    output->assign(buf, len);
    return true;
  }
  return false;
}

bool FetchPendingException(std::string* exception) {
  bool status = true;
  Py_ssize_t listsize;
  PyObject *ptype, *pvalue, *ptraceback, *module, *formatter, *args, *results;
  PyErr_Fetch(&ptype, &pvalue, &ptraceback);
  if (ptype == nullptr) {
    status = false;
  }
  if (status) {
    PyErr_NormalizeException(&ptype, &pvalue, &ptraceback);
    if (pvalue == nullptr || ptraceback == nullptr) {
      if (pvalue == nullptr) {
        pvalue = Py_None;
      }
      if (ptraceback == nullptr) {
        ptraceback = Py_None;
      }
    }
  }

  if (status) {
    module = PyImport_ImportModule(const_cast<char*>("traceback"));
    if (module == nullptr) {
      status = false;
    }
  }

  if (status) {
    formatter =
        PyObject_GetAttrString(module, const_cast<char*>("format_exception"));
    if (formatter == nullptr) {
      status = false;
    }
  }

  if (status) {
    args = PyTuple_Pack(3, ptype, pvalue, ptraceback);
    if (args == nullptr) {
      status = false;
    }
  }

  if (status) {
    results = PyObject_CallObject(formatter, args);
    if (results == nullptr) {
      status = false;
    }
  }

  if (status) {
    listsize = PySequence_Size(results);
    if (listsize < 0) {
      status = false;
    }
  }

  if (status) {
    std::string traceline;
    for (Py_ssize_t i = 0; i < listsize; ++i) {
      PyObject* item = PySequence_GetItem(results, i);
      if (PythonUtils::CPPString_FromPyString(item, &traceline)) {
        exception->append(traceline);
      } else {
        status = false;
      }
      Py_XDECREF(item);
      if (!status) {
        break;
      }
    }
  }
  Py_XDECREF(ptype);
  Py_XDECREF(pvalue);
  Py_XDECREF(ptraceback);
  Py_XDECREF(module);
  Py_XDECREF(formatter);
  Py_XDECREF(args);
  Py_XDECREF(results);
  return status;
}

}  // namespace PythonUtils
