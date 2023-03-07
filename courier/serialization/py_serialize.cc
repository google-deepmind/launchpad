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

#include "courier/serialization/py_serialize.h"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <string>
#include <utility>

#include "courier/platform/default/py_utils.h"
#include "absl/container/flat_hash_map.h"
#include "absl/flags/flag.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/synchronization/mutex.h"
#include "courier/platform/status_macros.h"
#include "courier/serialization/serialization.pb.h"
#include "courier/serialization/tensor_conversion.h"

#include <numpy/ndarrayobject.h>
#include "reverb/conversions.h"

using std::isfinite;

#define PyInt_Check PyLong_Check
#define PyInt_AsLong PyLong_AsLong
#define PyInt_FromLong PyLong_FromLong
#define PyString_AsString(ob) \
  (PyUnicode_Check(ob) ? PyUnicode_AsUTF8(ob) : PyBytes_AS_STRING(ob))

namespace tensorflow {

absl::Status ToUtilStatus(const ::tensorflow::Status& s) {
  return s.ok() ? absl::OkStatus()
                : absl::InvalidArgumentError("ToUtilStatus failure.");
}

}  // namespace tensorflow

namespace util_task_python {

absl::Status StatusFromPyExcMaybeErrOccurred() {
  if (!PyErr_Occurred()) {
    return absl::OkStatus();
  }
  return absl::InvalidArgumentError("ToUtilStatus failure.");
}

}  // namespace util_task_python


ABSL_FLAG(bool, py_serialize_debug_check_finite, false,
          ("If enabled, serializing any float data containing Inf or NaN"
           " values is an error."));

namespace courier {
namespace {

template <typename T>
bool PyArrayIsFinite(PyArrayObject* object) {
  T* data = static_cast<T*>(PyArray_DATA(object));
  return std::all_of(data, data + PyArray_SIZE(object),
                     [](const T element) { return Py_IS_FINITE(element); });
}

struct State {
  absl::flat_hash_map<std::string, PyObject*> cache;
  absl::Mutex mu;
};

inline State& GetState() {
  // Will not leak as all PyObject* are destroyed on shutdown.
  static State state;
  return state;
}

absl::StatusOr<PyObject*> ImportClass(const std::string& module,
                                      const std::string& name,
                                      bool ignore_cache) {
  if (module.empty()) {
    return absl::InvalidArgumentError("Module cannot be empty.");
  }
  if (name.empty()) {
    return absl::InvalidArgumentError("Name cannot be empty.");
  }

  // We need to release the GIL before locking, otherwise we might deadlock.
  PyThreadState* gil_releaser = PyEval_SaveThread();
  // We only allow a single import at a time.
  State& state = GetState();
  absl::MutexLock lock(&state.mu);
  PyEval_RestoreThread(gil_releaser);  // Reacquires the GIL.
  std::string full_name = absl::StrCat(module, ".", name);
  if (!ignore_cache) {
    if (auto it = state.cache.find(full_name); it != state.cache.end()) {
      return it->second;
    }
  }

  SafePyObjectPtr py_module(PyImport_ImportModule(module.data()));
  if (!py_module) {
    COURIER_RETURN_IF_ERROR(
        util_task_python::StatusFromPyExcMaybeErrOccurred());
    return absl::InvalidArgumentError(
        absl::StrCat("Failed to import module: ", module));
  }
  PyObject* py_class = PyObject_GetAttrString(py_module.get(), name.data());
  if (py_class == nullptr) {
    COURIER_RETURN_IF_ERROR(
        util_task_python::StatusFromPyExcMaybeErrOccurred());
    return absl::InvalidArgumentError(
        absl::StrCat("Failed to import class: ", module, ".", name));
  }
  state.cache[full_name] = py_class;
  return py_class;
}

absl::Status PyClassModuleAndName(PyObject* py_class, std::string* class_module,
                                  std::string* class_name) {
  SafePyObjectPtr py_module(PyObject_GetAttrString(py_class, "__module__"));
  if (py_module == nullptr) {
    return absl::InvalidArgumentError(
        "Only importable classes can be serialized.");
  }
  COURIER_RET_CHECK(
      PythonUtils::CPPString_FromPyString(py_module.get(), class_module));
  if (*class_module == "__main__") {
    return absl::InvalidArgumentError(
        "Only importable classes can be serialized.");
  }

  // Fetch class name.
  SafePyObjectPtr py_name(PyObject_GetAttrString(py_class, "__name__"));
  COURIER_RET_CHECK_NE(py_name.get(), nullptr);
  COURIER_RET_CHECK(
      PythonUtils::CPPString_FromPyString(py_name.get(), class_name));

  // Verify that class is importable.
  COURIER_ASSIGN_OR_RETURN(
      PyObject * py_class_imported,
      ImportClass(*class_module, *class_name, /*ignore_cache=*/false));
  if (py_class_imported == py_class) {
    return absl::OkStatus();
  }
  // If `py_class` differs from the one from import, that may be because the
  // import hit the module cache, while the module has actually been reloaded.
  // To ensure this doesn't block serialization, we try bypassing the cache,
  // which causes it to be overwritten in case of success.
  COURIER_ASSIGN_OR_RETURN(
      PyObject * py_class_reimported,
      ImportClass(*class_module, *class_name, /*ignore_cache=*/true));
  if (py_class_reimported == py_class) {
    return absl::OkStatus();
  }
  return absl::InvalidArgumentError(absl::StrCat("Class ", *class_name,
                                                 " from module ", *class_module,
                                                 " is not importable."));
}

bool PyClassModuleStartsWith(PyObject* object, const std::string& cmp) {
  SafePyObjectPtr py_module(
      PyObject_GetAttrString(PyObject_Type(object), "__module__"));
  if (py_module == nullptr) {
    return false;
  }
  std::string class_module;
  if (!PythonUtils::CPPString_FromPyString(py_module.get(), &class_module)) {
    return false;
  }
  return absl::StartsWith(class_module, cmp);
}

absl::Status SerializeAsTensorProto(PyObject* object,
                                    tensorflow::TensorProto* proto) {
  PyArrayObject* array = reinterpret_cast<PyArrayObject*>(object);
  tensorflow::DataType dtype;
  {
    tensorflow::Tensor tensor;
    tensorflow::Status status = ::deepmind::reverb::pybind::NdArrayToTensor(object, &tensor);
    if (absl::StartsWith(status.error_message(), "Unsupported object type")) {
      return absl::InvalidArgumentError(
          "Cannot serialize array of objects. NumPy arrays of np.object can "
          "only be serialized if all elements are strings.");
    }
    COURIER_RETURN_IF_ERROR(tensorflow::ToUtilStatus(status));
    tensor.AsProtoTensorContent(proto);
    dtype = tensor.dtype();
  }

  if (absl::GetFlag(FLAGS_py_serialize_debug_check_finite)) {
    if (dtype == tensorflow::DataType::DT_FLOAT) {
      COURIER_RET_CHECK(PyArrayIsFinite<float>(array))
          << "Serializing numpy array containing non-finite float.";
    }
    if (dtype == tensorflow::DataType::DT_DOUBLE) {
      COURIER_RET_CHECK(PyArrayIsFinite<double>(array))
          << "Serializing numpy array containing non-finite double.";
    }
  }
  return absl::OkStatus();
}

// Serializes a numpy array of type object.
absl::StatusOr<SerializedNumpyObjectTensor> SerializeObjectArray(
    PyArrayObject* array) {
  SerializedNumpyObjectTensor result;

  // Store the shape information in the result proto.
  result.mutable_shape()->Reserve(PyArray_NDIM(array));
  for (int i = 0; i < PyArray_NDIM(array); ++i) {
    result.add_shape(PyArray_DIM(array, i));
  }

  // Iterate over the flattened array and serialize all objects individually.
  auto iter =
      MakeSafePyPtr<PyArrayIterObject>(PyArray_IterNew((PyObject*)array));
  COURIER_RET_CHECK(iter != nullptr);

  while (PyArray_ITER_NOTDONE(iter.get())) {
    auto item = MakeSafePyPtr(PyArray_ToScalar(iter->dataptr, iter->ao));
    COURIER_RET_CHECK(item != nullptr);
    COURIER_RETURN_IF_ERROR(
        SerializePyObject(item.get(), result.add_payload()));
    PyArray_ITER_NEXT(iter.get());
  }
  return result;
}

// De-serializes a numpy array of type object.
absl::StatusOr<PyArrayObject*> DeserializeObjectArray(
    const SerializedNumpyObjectTensor& serialized,
    TensorLookup& tensor_lookup) {
  // Allocate the output array.
  auto result = MakeSafePyPtr<PyArrayObject>(PyArray_SimpleNewFromDescr(
      serialized.shape_size(), const_cast<int64_t*>(serialized.shape().data()),
      PyArray_DescrFromType(NPY_OBJECT)));
  COURIER_RET_CHECK(result != nullptr);

  // Create iterators over the input and the output arrays.
  auto iter = MakeSafePyPtr<PyArrayIterObject>(
      PyArray_IterNew((PyObject*)result.get()));
  COURIER_RET_CHECK(iter != nullptr);

  auto elem_iter = serialized.payload().begin();
  while (PyArray_ITER_NOTDONE(iter.get())) {
    COURIER_RET_CHECK(elem_iter != serialized.payload().end())
        << "Invalid SerializedNumpyObject proto.";

    COURIER_ASSIGN_OR_RETURN(auto out_item,
                             DeserializePyObject(*elem_iter++, tensor_lookup));

    COURIER_RET_CHECK(
        PyArray_SETITEM(result.get(),
                        reinterpret_cast<char*>(PyArray_ITER_DATA(iter.get())),
                        out_item.get()) == 0);
    PyArray_ITER_NEXT(iter.get());
  }
  return result.release();
}

// In Python:
//
//   from jax.numpy import bfloat16
//   return bfloat16.dtype.num
//
absl::StatusOr<int> GetJaxBfloat16NumpyType() {
  COURIER_ASSIGN_OR_RETURN(
      PyObject * bfloat16_obj,
      ImportClass("jax.numpy", "bfloat16", /*ignore_cache=*/false));
  SafePyObjectPtr dtype(
      PyObject_GetAttrString(bfloat16_obj, const_cast<char*>("dtype")));
  COURIER_RET_CHECK(dtype);
  SafePyObjectPtr dtype_num(
      PyObject_GetAttrString(dtype.get(), const_cast<char*>("num")));
  COURIER_RET_CHECK(dtype_num && PyInt_Check(dtype_num.get()));
  return static_cast<int>(PyInt_AsLong(dtype_num.get()));
}

absl::Status SerializeNdArray(PyObject* object, SerializedObject* buffer) {

  // If it is not an array then we are trying to convert a scalar. Since JAX
  // objects are first converted to ndarrays using __array__ we know that any
  // object that PyArray_Check returns false must NOT originated from a jax
  // object and thus cannot be a jax bfloat16 value.
  if (!PyArray_Check(object)) {
    return SerializeAsTensorProto(object, buffer->mutable_tensor_value());
  }

  PyArrayObject* array = reinterpret_cast<PyArrayObject*>(object);
  int array_type = PyArray_TYPE(array);

  // Unicode arrays are converted to string tensors, which are first
  // deserialized to byte arrays and then cast back to string.
  if (array_type == NPY_UNICODE) {
    buffer->set_numpy_metadata(SerializedObject::UNICODE_TENSOR);
    return SerializeAsTensorProto(object, buffer->mutable_tensor_value());
  }

  // Entries of object arrays are stored in two fields. String representation
  // of the objects are stored in `tensor_value`. These are used when the object
  // is de-serialized for C++ or TensorFlow. A best-effort object serialization
  // is stored in `numpy_object_tensor_payload`. This is used when the array is
  // de-serialized for Python.
  if (array_type == NPY_OBJECT) {
    buffer->set_numpy_metadata(SerializedObject::OBJECT_TENSOR);
    COURIER_ASSIGN_OR_RETURN(*buffer->mutable_numpy_object_tensor(),
                             SerializeObjectArray(array));
    return SerializeAsTensorProto(object, buffer->mutable_tensor_value());
  }

  // Usage of user defined types (e.g) is low so we avoid fetching bfloat16
  // details from JAX and TF until a user defined type is used.
  if (!PyTypeNum_ISUSERDEF(array_type)) {
    return SerializeAsTensorProto(object, buffer->mutable_tensor_value());
  }

  return SerializeAsTensorProto(object, buffer->mutable_tensor_value());
}

// UTF-8 decodes all strings stored in an array of dtype byte_. This is
// necessary to correctly handle unicode arrays, which are serialized to byte
// arrays. Note that a simple cast using PyArray_CastToType does not work if the
// byte string contains non ASCII characters.
absl::StatusOr<PyArrayObject*> DeserializeByteArray(PyArrayObject* array) {
  // Allocate the output array. We cannot allocate the array as unicode array
  // because we don't know the string lengths. We'll cast the array later.
  auto result = MakeSafePyPtr<PyArrayObject>(
      PyArray_SimpleNewFromDescr(PyArray_NDIM(array), PyArray_DIMS(array),
                                 PyArray_DescrFromType(NPY_OBJECT)));
  COURIER_RET_CHECK(result != nullptr);

  // Create iterators over the input and the output arrays.
  auto in_iter =
      MakeSafePyPtr<PyArrayIterObject>(PyArray_IterNew((PyObject*)array));
  auto out_iter = MakeSafePyPtr<PyArrayIterObject>(
      PyArray_IterNew((PyObject*)result.get()));
  COURIER_RET_CHECK(in_iter != nullptr);
  COURIER_RET_CHECK(out_iter != nullptr);

  while (PyArray_ITER_NOTDONE(in_iter.get())) {
    // Note: We do not need to increment the reference count here since our use
    // of in_item is only temporary.
    auto in_item = PyArray_ToScalar(in_iter->dataptr, in_iter->ao);
    COURIER_RET_CHECK(in_item != nullptr);

    auto out_item =
        MakeSafePyPtr(PyUnicode_FromEncodedObject(in_item, nullptr, nullptr));
    COURIER_RET_CHECK(out_item != nullptr)
        << "Failed to convert bytes object to unicode.";

    COURIER_RET_CHECK(PyArray_SETITEM(result.get(),
                                      reinterpret_cast<char*>(
                                          PyArray_ITER_DATA(out_iter.get())),
                                      out_item.get()) == 0);

    PyArray_ITER_NEXT(in_iter.get());
    PyArray_ITER_NEXT(out_iter.get());
  }

  // Now cast the array to unicode.
  auto unicode_array = MakeSafePyPtr<PyArrayObject>(PyArray_CastToType(
      result.get(), PyArray_DescrFromType(NPY_UNICODE), /* fortran */ 0));

  COURIER_RET_CHECK(unicode_array != nullptr)
      << "Failed to cast array to unicode.";
  return unicode_array.release();
}

// Converts a Tensor to an ndarray using aliasing if possible.
absl::StatusOr<PyGenericPtr<PyArrayObject>> TensorToNdArray(
    std::unique_ptr<tensorflow::Tensor> tensor) {
  PyObject* ret;

  auto res = tensorflow::ToUtilStatus(
      ::deepmind::reverb::pybind::TensorToNdArray(*tensor, &ret));
  COURIER_RETURN_IF_ERROR(res);
  return MakeSafePyPtr<PyArrayObject>(ret);
}

absl::Status TensorFromTensorProto(
    const tensorflow::TensorProto* proto, TensorLookup& tensor_lookup,
    tensorflow::Tensor* result) {
  auto it = tensor_lookup.find(proto);
  if (it != tensor_lookup.end()) {
    *result = std::move(it->second);
  } else if (!result->FromProto(*proto)) {
    return absl::InternalError("Failed to parse TensorProto.");
  }
  return absl::OkStatus();
}

}  // namespace

absl::Status SerializePyObject(PyObject* object, SerializedObject* buffer) {
  CHECK(Py_IsInitialized()) << "The Python interpreter has not been "
                               "initialized using Py_Initialize()";
  if (PyBool_Check(object)) {
    buffer->set_bool_value(PyObject_IsTrue(object));
  } else if (PyArray_Check(object) || PyArray_CheckScalar(object)) {
    COURIER_RETURN_IF_ERROR(SerializeNdArray(object, buffer));
  } else if (PyInt_Check(object)) {
    buffer->set_int_value(PyInt_AsLong(object));
  } else if (PyLong_Check(object)) {
    buffer->set_int_value(PyLong_AsLong(object));
  } else if (PyFloat_Check(object)) {
    if (absl::GetFlag(FLAGS_py_serialize_debug_check_finite)) {
      COURIER_RET_CHECK(Py_IS_FINITE(PyFloat_AsDouble(object)))
          << "Serializing non-finite Python float!";
    }
    buffer->set_double_value(PyFloat_AsDouble(object));
  } else if (PyBytes_Check(object)) {
    std::string result;
    if (!PythonUtils::CPPString_FromPyString(object, &result)) {
      return absl::InternalError("Failed to serialize bytes string.");
    }
    buffer->set_string_value(std::move(result));
  } else if (PyUnicode_Check(object)) {
    std::string result;
    if (!PythonUtils::CPPString_FromPyString(object, &result)) {
      return absl::InternalError("Failed to serialize unicode string.");
    }
    buffer->set_unicode_value(std::move(result));
  } else if (object == Py_None) {
    buffer->set_none_value(true);
  } else if (PyList_CheckExact(object)) {
    SerializedList* list = buffer->mutable_list_value();
    Py_ssize_t size = PyList_Size(object);
    for (Py_ssize_t i = 0; i < size; ++i) {
      COURIER_RETURN_IF_ERROR(
          SerializePyObject(PyList_GetItem(object, i), list->add_items()));
    }
  } else if (PyTuple_CheckExact(object)) {
    SerializedList* list = buffer->mutable_list_value();
    list->set_is_tuple(true);
    Py_ssize_t size = PyTuple_Size(object);
    for (Py_ssize_t i = 0; i < size; ++i) {
      COURIER_RETURN_IF_ERROR(
          SerializePyObject(PyTuple_GetItem(object, i), list->add_items()));
    }
  } else if (PyDict_CheckExact(object)) {
    PyObject* key;
    PyObject* value;
    Py_ssize_t position = 0;
    SerializedDict* dict = buffer->mutable_dict_value();
    while (PyDict_Next(object, &position, &key, &value)) {
      COURIER_RETURN_IF_ERROR(SerializePyObject(key, dict->add_keys()));
      COURIER_RETURN_IF_ERROR(SerializePyObject(value, dict->add_values()));
    }
  } else if (PyDictKeys_Check(object)) {
    return absl::InvalidArgumentError(
        "Serializing Python dict keys is not supported.");
  } else if (PyDictItems_Check(object)) {
    return absl::InvalidArgumentError(
        "Serializing Python dict items is not supported.");
  } else if (PyDictValues_Check(object)) {
    return absl::InvalidArgumentError(
        "Serializing Python dict values is not supported.");
  } else if (PyType_Check(object) || PyFunction_Check(object) ||
             PyCFunction_Check(object)) {
    COURIER_RETURN_IF_ERROR(PyClassModuleAndName(
        object, buffer->mutable_type_value()->mutable_module(),
        buffer->mutable_type_value()->mutable_name()));
  } else if (PyObject_HasAttrString(object, "__reduce__") ||
             PyObject_HasAttrString(object, "__reduce_ex__")) {
    SafePyObjectPtr reduced;
    if (PyObject_HasAttrString(object, "__reduce_ex__")) {
      SafePyObjectPtr reduce_ex_call(
          PyObject_GetAttrString(object, "__reduce_ex__"));
      SafePyObjectPtr reduce_ex_args(PyTuple_New(1));
      // pickle.DEFAULT_PROTOCOL = 3
      // https://python.readthedocs.io/en/stable/library/pickle.html#pickle.DEFAULT_PROTOCOL
      PyTuple_SET_ITEM(reduce_ex_args.get(), 0, PyInt_FromLong(3));
      reduced = SafePyObjectPtr(
          PyObject_CallObject(reduce_ex_call.get(), reduce_ex_args.get()));
    } else {
      reduced = SafePyObjectPtr(PyObject_CallMethod(
          object, const_cast<char*>("__reduce__"), nullptr));
    }
    if (!reduced) {
      SafePyObjectPtr repr(PyObject_Repr(object));
      if (!repr) {
        return absl::InternalError("Error retrieving Python object");
      } else {
        return absl::InvalidArgumentError(
            absl::StrCat("Nothing returned from __reduce__ on ",
                         PyString_AsString(repr.get())));
      }
    }

    // Fetch class module and name.
    PyObject* py_class = PyTuple_GetItem(reduced.get(), 0);  // borrowed.
    COURIER_RETURN_IF_ERROR(PyClassModuleAndName(
        py_class,
        buffer->mutable_reduced_object_value()->mutable_class_module(),
        buffer->mutable_reduced_object_value()->mutable_class_name()));

    // Serialize arguments.
    PyObject* py_args = PyTuple_GetItem(reduced.get(), 1);  // borrowed.
    COURIER_RET_CHECK(PyTuple_CheckExact(py_args));
    COURIER_RETURN_IF_ERROR(SerializePyObject(
        py_args, buffer->mutable_reduced_object_value()->mutable_args()));

    // Maybe serialize state.
    if (PyTuple_Size(reduced.get()) >= 3) {
      PyObject* py_state = PyTuple_GetItem(reduced.get(), 2);
      COURIER_RETURN_IF_ERROR(SerializePyObject(
          py_state, buffer->mutable_reduced_object_value()->mutable_state()));
    }

    // Maybe save items contained by this object.
    if (PyTuple_Size(reduced.get()) >= 4) {
      PyObject* py_items = PyTuple_GetItem(reduced.get(), 3);
      COURIER_RET_CHECK_NE(py_items, nullptr);
      if (py_items != Py_None) {
        // This is an iterator we add items from one at a time.
        SafePyObjectPtr iterator(PyObject_GetIter(py_items));
        SafePyObjectPtr item;
        while ((item = SafePyObjectPtr(PyIter_Next(iterator.get())))) {
          COURIER_RETURN_IF_ERROR(SerializePyObject(
              item.get(), buffer->mutable_reduced_object_value()
                              ->mutable_items()
                              ->mutable_list_value()
                              ->add_items()));
        }
      }
    }

    // Maybe save keys and values contained by this object.
    if (PyTuple_Size(reduced.get()) == 5) {
      PyObject* py_kv_pairs = PyTuple_GetItem(reduced.get(), 4);
      COURIER_RET_CHECK_NE(py_kv_pairs, nullptr);
      if (py_kv_pairs != Py_None) {
        // This is an iterator but we must turn it into a list first.
        SafePyObjectPtr iterator(PyObject_GetIter(py_kv_pairs));
        SafePyObjectPtr item;
        while ((item = SafePyObjectPtr(PyIter_Next(iterator.get())))) {
          COURIER_RETURN_IF_ERROR(SerializePyObject(
              item.get(), buffer->mutable_reduced_object_value()
                              ->mutable_kvpairs()
                              ->mutable_list_value()
                              ->add_items()));
        }
      }
    }
  } else {
    SafePyObjectPtr repr(PyObject_Repr(object));
    if (repr == nullptr) {
      return absl::InternalError("Error retrieving Python object");
    } else {
      return absl::InvalidArgumentError(absl::StrCat(
          "Object not serializable: ", PyString_AsString(repr.get())));
    }
  }
  return util_task_python::StatusFromPyExcMaybeErrOccurred();
}

absl::StatusOr<SerializedObject> SerializePyObject(PyObject* object) {
  SerializedObject buffer;
  COURIER_RETURN_IF_ERROR(SerializePyObject(object, &buffer));
  return buffer;
}

absl::StatusOr<PyObject*> DeserializePyObjectUnsafe(
    const SerializedObject& buffer, TensorLookup& tensor_lookup) {
  CHECK(Py_IsInitialized()) << "The Python interpreter has not been "
                               "initialized using Py_Initialize()";
  switch (buffer.payload_case()) {
    case SerializedObject::kNoneValue:
      Py_RETURN_NONE;
    case SerializedObject::kIntValue:
      return PyInt_FromLong(buffer.int_value());
    case SerializedObject::kDoubleValue:
      return PyFloat_FromDouble(buffer.double_value());
    case SerializedObject::kBoolValue:
      return PyBool_FromLong(buffer.bool_value());
    case SerializedObject::kStringValue: {
      PyObject* py_str = PyBytes_FromStringAndSize(
          buffer.string_value().data(), buffer.string_value().size());
      COURIER_RET_CHECK(py_str)
          << "Failed to build python string from proto string.";
      return py_str;
    }
    case SerializedObject::kUnicodeValue: {
      PyObject* py_str = PyUnicode_FromStringAndSize(
          buffer.unicode_value().data(), buffer.unicode_value().size());
      COURIER_RET_CHECK(py_str)
          << "Failed to build python string from proto string.";
      return py_str;
    }
    case SerializedObject::kDictValue: {
      const SerializedDict& dict = buffer.dict_value();
      if (dict.keys_size() != dict.values_size()) {
        return absl::InternalError("Dict keys/values size mismatch.");
      }
      PyObject* py_dict = PyDict_New();
      for (int i = 0; i < dict.keys_size(); ++i) {
        COURIER_ASSIGN_OR_RETURN(
            SafePyObjectPtr py_key,
            DeserializePyObject(dict.keys(i), tensor_lookup));
        COURIER_ASSIGN_OR_RETURN(
            SafePyObjectPtr py_value,
            DeserializePyObject(dict.values(i), tensor_lookup));
        PyDict_SetItem(py_dict, py_key.get(), py_value.get());
      }
      return py_dict;
    }
    case SerializedObject::kListValue: {
      const SerializedList& list = buffer.list_value();
      if (list.is_tuple()) {
        PyObject* py_tuple = PyTuple_New(list.items_size());
        for (int i = 0; i < list.items_size(); ++i) {
          COURIER_ASSIGN_OR_RETURN(
              SafePyObjectPtr py_item,
              DeserializePyObject(list.items(i), tensor_lookup));
          PyTuple_SET_ITEM(py_tuple, i, py_item.release());
        }
        return py_tuple;
      } else {
        PyObject* py_list = PyList_New(list.items_size());
        for (int i = 0; i < list.items_size(); ++i) {
          COURIER_ASSIGN_OR_RETURN(
              SafePyObjectPtr py_item,
              DeserializePyObject(list.items(i), tensor_lookup));
          PyList_SET_ITEM(py_list, i, py_item.release());
        }
        return py_list;
      }
    }
    case SerializedObject::kTensorValue: {
      auto tensor = absl::make_unique<tensorflow::Tensor>();
      COURIER_RETURN_IF_ERROR(TensorFromTensorProto(
          &buffer.tensor_value(), tensor_lookup, tensor.get()));

      const auto dtype = tensor->dtype();

      COURIER_ASSIGN_OR_RETURN(auto ret_safe,
                               TensorToNdArray(std::move(tensor)));

      // PyArray_Return turns rank 0 arrays into numpy scalars
      return PyArray_Return(ret_safe.release());
    }
    case SerializedObject::kJaxTensorValue: {
      auto tensor = absl::make_unique<tensorflow::Tensor>();
      COURIER_RETURN_IF_ERROR(TensorFromTensorProto(
          &buffer.jax_tensor_value(), tensor_lookup, tensor.get()));

      COURIER_ASSIGN_OR_RETURN(auto ret_safe,
                               TensorToNdArray(std::move(tensor)));
      // PyArray_Return turns rank 0 arrays into numpy scalars
      return PyArray_Return(
          reinterpret_cast<PyArrayObject*>(ret_safe.release()));
    }
    case SerializedObject::kTypeValue: {
      COURIER_ASSIGN_OR_RETURN(PyObject * py_class,
                               ImportClass(buffer.type_value().module(),
                                           buffer.type_value().name(),
                                           /*ignore_cache=*/false));
      // The caller of this function assumes ownership of the PyObject*. So we
      // need to increment the reference of the cached object.
      Py_INCREF(py_class);
      return py_class;
    }
    case SerializedObject::kReducedObjectValue: {
      COURIER_ASSIGN_OR_RETURN(
          PyObject * py_class,
          ImportClass(buffer.reduced_object_value().class_module(),
                      buffer.reduced_object_value().class_name(),
                      /*ignore_cache=*/false));

      // Deserialize args.
      COURIER_ASSIGN_OR_RETURN(
          SafePyObjectPtr py_args,
          DeserializePyObject(buffer.reduced_object_value().args(),
                              tensor_lookup));

      // Make instance.
      PyObject* py_object = PyObject_CallObject(py_class, py_args.get());

      // Maybe deserialize and set state.
      if (buffer.reduced_object_value().has_state() &&
          buffer.reduced_object_value().state().payload_case() !=
              SerializedObject::kNoneValue) {
        COURIER_ASSIGN_OR_RETURN(
            SafePyObjectPtr py_state,
            DeserializePyObject(buffer.reduced_object_value().state(),
                                tensor_lookup));
        if (PyObject_HasAttrString(py_object, "__setstate__")) {
          SafePyObjectPtr py_setstate_call(
              PyObject_GetAttrString(py_object, "__setstate__"));
          SafePyObjectPtr py_state_args(PyTuple_Pack(1, py_state.get()));
          SafePyObjectPtr py_result(
              PyObject_CallObject(py_setstate_call.get(), py_state_args.get()));
          if (py_result == nullptr) {
            COURIER_RETURN_IF_ERROR(
                util_task_python::StatusFromPyExcMaybeErrOccurred());
            return absl::InternalError("__setstate__ call failed");
          }
        } else {
          // If the object has no __setstate__ method then, the value must be a
          // dictionary and it will be added to the object’s __dict__ attribute.
          // See https://docs.python.org/3/library/pickle.html#object.__reduce__
          SafePyObjectPtr pyobject_dict(
              PyObject_GetAttrString(py_object, "__dict__"));
          COURIER_RET_CHECK(pyobject_dict);
          COURIER_RET_CHECK(PyDict_Check(pyobject_dict.get()));
          COURIER_RET_CHECK(PyDict_Check(py_state.get()));
          COURIER_RET_CHECK_EQ(
              PyDict_Update(pyobject_dict.get(), py_state.get()), 0);
        }
      }

      // Maybe deserialize and set items.
      if (buffer.reduced_object_value().has_items() &&
          buffer.reduced_object_value().items().payload_case() !=
              SerializedObject::kNoneValue) {
        COURIER_ASSIGN_OR_RETURN(
            SafePyObjectPtr py_items,
            DeserializePyObject(buffer.reduced_object_value().items(),
                                tensor_lookup));

        // Call extend on the object.
        SafePyObjectPtr extend_fn(
            Py_BuildValue("s", const_cast<char*>("extend")));
        SafePyObjectPtr py_result(PyObject_CallMethodObjArgs(
            py_object, extend_fn.get(), py_items.get(), NULL));
        if (py_result == nullptr) {
          COURIER_RETURN_IF_ERROR(
              util_task_python::StatusFromPyExcMaybeErrOccurred());
          return absl::InternalError("extend call failed");
        }
      }

      // Maybe deserialize and set key/value pairs.
      if (buffer.reduced_object_value().has_kvpairs() &&
          buffer.reduced_object_value().kvpairs().payload_case() !=
              SerializedObject::kNoneValue) {
        COURIER_ASSIGN_OR_RETURN(
            SafePyObjectPtr py_kvpairs,
            DeserializePyObject(buffer.reduced_object_value().kvpairs(),
                                tensor_lookup));

        // Set each k/v pair on the item.
        for (int i = 0; i < PyList_Size(py_kvpairs.get()); ++i) {
          PyObject* py_kv = PyList_GetItem(py_kvpairs.get(), i);
          COURIER_RET_CHECK_EQ(
              PyObject_SetItem(py_object, PyTuple_GetItem(py_kv, 0),
                               PyTuple_GetItem(py_kv, 1)),
              0);
        }
      }
      return py_object;
    }
    case SerializedObject::PAYLOAD_NOT_SET:
      return absl::InternalError(
          "No value set. The buffer is likely corrupted.");
  }
}

absl::StatusOr<SafePyObjectPtr> DeserializePyObject(
    const SerializedObject& buffer, TensorLookup& tensor_lookup) {
  COURIER_ASSIGN_OR_RETURN(PyObject * obj,
                           DeserializePyObjectUnsafe(buffer, tensor_lookup));
  return SafePyObjectPtr(obj);
}
absl::StatusOr<SafePyObjectPtr> DeserializePyObject(
    const SerializedObject& buffer) {
  TensorLookup empty_lookup;
  COURIER_ASSIGN_OR_RETURN(PyObject * obj,
                           DeserializePyObjectUnsafe(buffer, empty_lookup));
  return SafePyObjectPtr(obj);
}

absl::StatusOr<std::string> SerializePyObjectToString(PyObject* object) {
  SerializedObject buffer;
  COURIER_RETURN_IF_ERROR(SerializePyObject(object, &buffer));
  return buffer.SerializeAsString();
}

absl::StatusOr<PyObject*> DeserializePyObjectFromString(
    const std::string& str) {
  SerializedObject buffer;
  buffer.ParseFromString(str);
  COURIER_ASSIGN_OR_RETURN(auto tensor_lookup, CreateTensorLookup(buffer));
  COURIER_ASSIGN_OR_RETURN(SafePyObjectPtr obj,
                           DeserializePyObject(buffer, tensor_lookup));
  return obj.release();
}

void ImportNumpy() {
  // import_array1 has to be called from each module using Numpy Arrays.
  deepmind::reverb::pybind::ImportNumpy();
  import_array1();
}

}  // namespace courier
