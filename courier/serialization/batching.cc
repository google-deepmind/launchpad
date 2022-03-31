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

#include "courier/serialization/batching.h"
#include "absl/status/status.h"
#include "tensorflow/core/framework/types.pb.h"

#ifdef TENSORFLOW_PROTOBUF_USES_CORD
#include "strings/cord_varint.h"
#endif  // defined(TENSORFLOW_PROTOBUF_USES_CORD)
#include "absl/container/flat_hash_map.h"
#include "courier/platform/status_macros.h"
#include "courier/serialization/serialization.pb.h"
#include "tensorflow/core/framework/tensor_shape.pb.h"

namespace courier {

absl::Status BatchTensors(std::vector<const tensorflow::TensorProto*>& tensors,
                          tensorflow::TensorProto* result) {
  for (auto tensor : tensors) {
    if (tensors[0]->tensor_shape().dim_size() !=
        tensor->tensor_shape().dim_size()) {
      return absl::InvalidArgumentError(
          "Rank of Tensors to be batched do not match.");
    }
    if (tensors[0]->dtype() != tensor->dtype()) {
      return absl::InvalidArgumentError(
          "Types of Tensors to be batched do not match.");
    }
    for (int y = 0; y < tensor->tensor_shape().dim_size(); y++) {
      if (tensors[0]->tensor_shape().dim(y).size() !=
          tensor->tensor_shape().dim(y).size()) {
        return absl::InvalidArgumentError(
            "Shapes of Tensors to be batched do not match.");
      }
    }
  }
  int64_t size = 1;
  result->set_dtype(tensors[0]->dtype());
  auto* shape = result->mutable_tensor_shape();
  shape->add_dim()->set_size(tensors.size());
  for (auto& dim : tensors[0]->tensor_shape().dim()) {
    const int64_t dim_size = dim.size();
    size *= dim_size;
    shape->add_dim()->set_size(dim_size);
  }
  if (tensors[0]->dtype() == tensorflow::DT_STRING) {
#ifdef TENSORFLOW_PROTOBUF_USES_CORD
    std::vector<int> data_pos;
    data_pos.reserve(tensors.size());
    for (auto tensor : tensors) {
      CordReader reader(tensor->tensor_content());
      for (int i = 0; i < size; i++) {
        uint64_t val;
        if (!strings::CordReaderReadVarint(&reader, &val)) {
          return absl::InternalError("Corrupted Tensor payload.");
        }
      }
      data_pos.push_back(reader.Position());
    }
    for (int x = 0; x < tensors.size(); x++) {
      result->mutable_tensor_content()->Append(
          tensors[x]->tensor_content().Subcord(0, data_pos[x]));
    }
    for (int x = 0; x < tensors.size(); x++) {
      auto& cord = tensors[x]->tensor_content();
      result->mutable_tensor_content()->Append(
          cord.Subcord(data_pos[x], cord.size() - data_pos[x]));
    }
#else
    return absl::InvalidArgumentError(
        "Batching of string tensors is not supported.");
#endif  // defined(TENSORFLOW_PROTOBUF_USES_CORD)
  } else {
    for (auto tensor : tensors) {
#ifdef TENSORFLOW_PROTOBUF_USES_CORD
      result->mutable_tensor_content()->Append(tensor->tensor_content());
#else
      result->mutable_tensor_content()->append(tensor->tensor_content());
#endif  // defined(TENSORFLOW_PROTOBUF_USES_CORD)
    }
  }
  return absl::OkStatus();
}

absl::Status UnbatchDmStruct(
    const SerializedObject& object,
    const std::vector<SerializedObject*>& results) {
  for (auto o : results) {
    o->mutable_list_value()->set_is_tuple(true);
    *o->mutable_list_value()->add_items() = object.list_value().items(0);
    o->mutable_list_value()->add_items();
  }
  for (auto& src : object.list_value().items(1).list_value().items()) {
    std::vector<SerializedObject*> children;
    children.reserve(results.size());
    for (auto parent : results) {
      auto dst = parent->mutable_list_value()->mutable_items(1)->
          mutable_list_value()->add_items()->mutable_list_value();
      dst->set_is_tuple(true);
      *dst->add_items() = src.list_value().items(0);
      children.push_back(dst->add_items());
    }
    COURIER_RETURN_IF_ERROR(
        UnbatchSerializedObject(src.list_value().items(1), children));
  }
  return absl::OkStatus();
}

absl::Status UnbatchNewObject(
    const SerializedObject& object,
    const std::vector<SerializedObject*>& results) {
  for (auto o : results) {
    o->mutable_list_value()->set_is_tuple(true);
    *o->mutable_list_value()->add_items() = object.list_value().items(0);
  }
  for (int x = 1; x < object.list_value().items_size(); x++) {
    std::vector<SerializedObject*> children;
    children.reserve(results.size());
    for (auto parent : results) {
      children.push_back(parent->mutable_list_value()->add_items());
    }
    COURIER_RETURN_IF_ERROR(UnbatchSerializedObject(
        object.list_value().items(x), children));
  }
  return absl::OkStatus();
}

absl::Status UnbatchTensor(
    const tensorflow::TensorProto& tensor,
    const std::vector<tensorflow::TensorProto*>& results) {
  if (results.size() != tensor.tensor_shape().dim(0).size()) {
    return absl::InvalidArgumentError(
        "Invalid dimension of the Tensor to unbatch.");
  }
  ::tensorflow::TensorShapeProto shape;
  int64_t size = 1;
  for (int x = 1; x < tensor.tensor_shape().dim_size(); x++) {
    shape.add_dim()->CopyFrom(tensor.tensor_shape().dim(x));
    size *= tensor.tensor_shape().dim(x).size();
  }
  for (auto result : results) {
    result->mutable_tensor_shape()->CopyFrom(shape);
    result->set_dtype(tensor.dtype());
  }
  if (tensor.dtype() == tensorflow::DT_STRING) {
#ifdef TENSORFLOW_PROTOBUF_USES_CORD
    std::vector<size_t> length_pos;
    std::vector<size_t> data_pos;
    length_pos.reserve(results.size() + 1);
    data_pos.reserve(results.size() + 1);
    length_pos.push_back(0);
    data_pos.push_back(0);
    CordReader reader(tensor.tensor_content());
    for (int x = 0; x < results.size(); x++) {
      size_t data_size = 0;
      for (int y = 0; y < size; y++) {
        size_t val = 0;
        if (!strings::CordReaderReadVarint(&reader, &val)) {
          return absl::InternalError("Corrupted Tensor payload.");
        }
        data_size += val;
      }
      length_pos.push_back(reader.Position());
      data_pos.push_back(data_pos.back() + data_size);
    }
    for (int x = 0; x < results.size(); x++) {
      results[x]->mutable_tensor_content()->Append(
          tensor.tensor_content().Subcord(length_pos[x],
                                          length_pos[x + 1] - length_pos[x]));
      results[x]->mutable_tensor_content()->Append(
          tensor.tensor_content().Subcord(length_pos.back() + data_pos[x],
                                          data_pos[x + 1] - data_pos[x]));
    }
#else
    return absl::InvalidArgumentError(
        "Unbatching of string tensors is not supported.");
#endif  // defined(TENSORFLOW_PROTOBUF_USES_CORD)
  } else {
#ifdef TENSORFLOW_PROTOBUF_USES_CORD
    size_t pos = 0;
    auto &cord = tensor.tensor_content();
    size_t increment = cord.size() / results.size();
    auto iter = cord.Chars().begin();
    for (auto result : results) {
      result->mutable_tensor_content()->Append(
          cord.AdvanceAndRead(&iter, increment));
      pos += increment;
    }
#else
    size_t pos = 0;
    const auto &str = tensor.tensor_content();
    size_t increment = str.size() / results.size();
    for (auto result : results) {
      *result->mutable_tensor_content() = str.substr(pos, pos + increment);
      pos += increment;
    }
#endif  // defined(TENSORFLOW_PROTOBUF_USES_CORD)
  }
  return absl::OkStatus();
}

absl::Status UnbatchSerializedObject(
    const SerializedObject& object,
    const std::vector<SerializedObject*>& results) {
  switch (object.payload_case()) {
    case SerializedObject::kNoneValue:
    case SerializedObject::kIntValue:
    case SerializedObject::kDoubleValue:
    case SerializedObject::kBoolValue:
    case SerializedObject::kStringValue:
    case SerializedObject::kTypeValue:
    case SerializedObject::kUnicodeValue:
      return absl::InvalidArgumentError("Can't unbatch basic type.");
    case SerializedObject::kListValue: {
      if (object.list_value().is_tuple()) {
        for (auto result : results) {
          result->mutable_list_value()->set_is_tuple(true);
        }
        for (auto& element : object.list_value().items()) {
          std::vector<SerializedObject*> children;
          children.reserve(results.size());
          for (auto child : results) {
            children.push_back(child->mutable_list_value()->add_items());
          }
          COURIER_RETURN_IF_ERROR(UnbatchSerializedObject(element, children));
        }
        return absl::OkStatus();
      } else {
        if (object.list_value().items_size() != results.size()) {
          return absl::InvalidArgumentError(
              "List to unbatch has invalid length.");
        }
        for (int x = 0; x < results.size(); x++) {
          results[x]->CopyFrom(object.list_value().items(x));
        }
        return absl::OkStatus();
      }
    }
    case SerializedObject::kDictValue: {
      if (object.dict_value().keys_size() !=
          object.dict_value().values_size()) {
        return absl::InternalError("Dict keys/values size mismatch.");
      }
      // Initialize dict in case it is empty.
      for (auto result : results) {
        result->mutable_dict_value();
      }
      for (int x = 0; x < object.dict_value().keys_size(); x++) {
        std::vector<SerializedObject*> children;
        children.reserve(results.size());
        for (auto child : results) {
          *child->mutable_dict_value()->add_keys() =
              object.dict_value().keys(x);
          children.push_back(child->mutable_dict_value()->add_values());
        }
        COURIER_RETURN_IF_ERROR(
            UnbatchSerializedObject(object.dict_value().values(x), children));
      }
      return absl::OkStatus();
    }
    case SerializedObject::kReducedObjectValue: {
      auto& reduced_object = object.reduced_object_value();
      std::vector<SerializedObject*> children;
      children.reserve(results.size());
      for (auto c : results) {
        auto to_set = c->mutable_reduced_object_value();
        to_set->set_class_module(reduced_object.class_module());
        to_set->set_class_name(reduced_object.class_name());
        children.push_back(to_set->mutable_args());
      }
      if (reduced_object.class_module() ==
          "tensorflow.python.framework.ops" &&
          reduced_object.class_name() == "convert_to_tensor") {
        return UnbatchSerializedObject(reduced_object.args(), children);
      } else if (reduced_object.class_module() == "copyreg" &&
                 reduced_object.class_name() == "__newobj__" &&
                 reduced_object.has_state() &&
                 reduced_object.state().payload_case() ==
                 SerializedObject::PayloadCase::kNoneValue) {
        for (auto c : results) {
          *c->mutable_reduced_object_value()->mutable_state() =
              reduced_object.state();
        }
        return UnbatchNewObject(reduced_object.args(), children);
      }
      return absl::InternalError(
          "Unbatching provided object is not currently supported.");
    }
    case SerializedObject::kTensorValue: {
      std::vector<tensorflow::TensorProto*> children;
      children.reserve(results.size());
      for (auto result : results) {
        result->set_numpy_metadata(object.numpy_metadata());
        children.push_back(result->mutable_tensor_value());
      }
      if (object.has_numpy_object_tensor()) {
        return absl::InvalidArgumentError(
            "Unbatching of numpy object tensors is not supported currently.");
      }
      return UnbatchTensor(object.tensor_value(), children);
    }
    case SerializedObject::kJaxTensorValue: {
      std::vector<tensorflow::TensorProto*> children;
      children.reserve(results.size());
      for (auto result : results) {
        children.push_back(result->mutable_tensor_value());
      }
      return UnbatchTensor(object.jax_tensor_value(), children);
    }
    case SerializedObject::PAYLOAD_NOT_SET:
      return absl::InternalError(
          "No value set. The buffer is likely corrupted.");
  }
}

void BatchSerializedObjectsAsList(
    const std::vector<const SerializedObject*>& objects,
    SerializedObject* result) {
  // Wrap all objects into a list.
  auto list = result->mutable_list_value();
  for (auto object : objects) {
    list->add_items()->CopyFrom(*object);
  }
}

absl::Status BatchDmStruct(
    const std::vector<const SerializedObject*>& objects,
    SerializedObject* result) {
  auto base = objects[0];
  for (const auto& obj : objects) {
    if (!obj->has_list_value() ||
        obj->list_value().items_size() != 2 ||
        obj->list_value().items(0).payload_case() !=
        SerializedObject::PayloadCase::kNoneValue ||
        base->list_value().items(0).none_value() !=
            obj->list_value().items(0).none_value() ||
        base->list_value().items(1).list_value().items_size() !=
            obj->list_value().items(1).list_value().items_size()) {
      return absl::InternalError("Provided DM structs can not be batched.");
    }
  }
  auto list = result->mutable_list_value();
  list->set_is_tuple(true);
  list->add_items()->set_none_value(base->list_value().items(0).none_value());
  list = list->add_items()->mutable_list_value();
  for (int x = 0; x < base->list_value().items(1).list_value().items_size();
      x++) {
    auto batched = list->add_items()->mutable_list_value();
    batched->set_is_tuple(true);
    *batched->add_items() =
        base->list_value().items(1).list_value().items(x).list_value().items(0);
    std::vector<const SerializedObject*> children;
    children.reserve(objects.size());
    for (const auto& obj : objects) {
      children.push_back(&obj->list_value().items(1).list_value().items(x).
                         list_value().items(1));
    }
    COURIER_RETURN_IF_ERROR(
        BatchSerializedObjects(children, batched->add_items()));
  }
  return absl::OkStatus();
}

absl::Status BatchNewObject(
    const std::vector<const SerializedObject*>& objects,
    SerializedObject* result) {
  auto base = objects[0];
  for (const auto& obj : objects) {
    if (!obj->has_list_value() ||
        !obj->list_value().items(0).has_type_value() ||
        base->list_value().items(0).type_value().module() !=
        obj->list_value().items(0).type_value().module() ||
        base->list_value().items(0).type_value().name() !=
        obj->list_value().items(0).type_value().name() ||
        base->list_value().items_size() != obj->list_value().items_size()) {
      return absl::InternalError("Provided Python objects can not be batched.");
    }
  }
  auto list = result->mutable_list_value();
  list->set_is_tuple(true);
  *list->add_items()->mutable_type_value() =
      base->list_value().items(0).type_value();
  for (int x = 1; x < base->list_value().items_size(); x++) {
    std::vector<const SerializedObject*> children;
    children.reserve(objects.size());
    for (const auto& obj : objects) {
      children.push_back(&obj->list_value().items(x));
    }
    COURIER_RETURN_IF_ERROR(
        BatchSerializedObjects(children, list->add_items()));
  }
  return absl::OkStatus();
}

::tensorflow::DataType BasicObjectTypeTFType(const SerializedObject& object) {
  switch (object.payload_case()) {
    case SerializedObject::kIntValue:
      return tensorflow::DT_INT64;
    case SerializedObject::kDoubleValue:
      return tensorflow::DT_DOUBLE;
    case SerializedObject::kBoolValue:
      return tensorflow::DT_BOOL;
    default:
      return object.tensor_value().dtype();
  }
}

// Batches simple type objects and single-value Tensors of the matching type.
absl::Status BatchSimpleTypes(
    const std::vector<const SerializedObject*>& objects,
    SerializedObject* result) {
  auto tensor = result->mutable_tensor_value();
  auto type = BasicObjectTypeTFType(*objects[0]);
  tensor->set_dtype(type);
  tensor->mutable_tensor_shape()->add_dim()->set_size(objects.size());
  for (auto& o : objects) {
    if (type != BasicObjectTypeTFType(*o)) {
      return absl::InternalError(
        "Mismatching types of objects to be batched across requests.");
    }
    switch (o->payload_case()) {
      case SerializedObject::kIntValue:
        tensor->add_int64_val(o->int_value());
        break;
      case SerializedObject::kDoubleValue:
        tensor->add_double_val(o->double_value());
        break;
      case SerializedObject::kBoolValue:
        tensor->add_bool_val(o->bool_value());
        break;
      default:
        tensorflow::Tensor parsed;
        if (!parsed.FromProto(o->tensor_value())) {
          return absl::InternalError("Failed to parse TensorProto.");
        }
        if (type == tensorflow::DT_INT64) {
          tensor->add_int64_val(*parsed.flat<int64_t>().data());
        }
        if (type == tensorflow::DT_DOUBLE) {
          tensor->add_double_val(*parsed.flat<double>().data());
        }
        if (type == tensorflow::DT_BOOL) {
          tensor->add_bool_val(*parsed.flat<bool>().data());
        }
    }
  }
  return absl::OkStatus();
}

absl::Status BatchSerializedObjects(
    const std::vector<const SerializedObject*>& objects,
    SerializedObject* result) {
  const SerializedObject& front = *objects[0];
  if (front.payload_case() == SerializedObject::kIntValue ||
    front.payload_case() == SerializedObject::kDoubleValue ||
    front.payload_case() == SerializedObject::kBoolValue ||
    (front.payload_case() == SerializedObject::kTensorValue &&
     front.tensor_value().tensor_shape().dim_size() == 0 && (
              front.tensor_value().dtype() == tensorflow::DT_INT64 ||
              front.tensor_value().dtype() == tensorflow::DT_BOOL ||
              front.tensor_value().dtype() == tensorflow::DT_DOUBLE))) {
    return BatchSimpleTypes(objects, result);
  }
  for (auto object : objects) {
    if (front.payload_case() != object->payload_case()) {
      return absl::InternalError(
          "Mismatching types of objects to be batched across requests.");
    }
  }
  switch (front.payload_case()) {
    case SerializedObject::kListValue: {
      const SerializedList& list = front.list_value();
      // All elements should be either lists or tuples.
      for (auto object : objects) {
        if (object->list_value().is_tuple() != list.is_tuple()) {
          return absl::InternalError(
              "Mismatching types of objects to be batched across requests.");
        }
      }
      if (list.is_tuple()) {
        auto result_tuple = result->mutable_list_value();
        for (int x = 1; x < objects.size(); ++x) {
          if (list.items_size() != objects[x]->list_value().items_size()) {
            return absl::InternalError(
                "Mismatching lengths of lists across requests to be batched.");
          }
        }
        result_tuple->set_is_tuple(true);
        for (int i = 0; i < list.items_size(); ++i) {
          auto element = result_tuple->add_items();
          std::vector<const SerializedObject*> items;
          items.reserve(objects.size());
          for (int x = 0; x < objects.size(); x++) {
            items.push_back(&objects[x]->list_value().items(i));
          }
          COURIER_RETURN_IF_ERROR(BatchSerializedObjects(items, element));
        }
        return absl::OkStatus();
      } else {
        BatchSerializedObjectsAsList(objects, result);
        return absl::OkStatus();
      }
    }
    case SerializedObject::kStringValue: {
      auto tensor = result->mutable_tensor_value();
      for (auto o : objects) {
        tensor->add_string_val(o->string_value());
      }
      tensor->set_dtype(tensorflow::DT_STRING);
      tensor->mutable_tensor_shape()->add_dim()->set_size(objects.size());
      return absl::OkStatus();
    }
    case SerializedObject::kNoneValue:
    case SerializedObject::kTypeValue:
    case SerializedObject::kUnicodeValue: {
      BatchSerializedObjectsAsList(objects, result);
      return absl::OkStatus();
    }
    case SerializedObject::kReducedObjectValue: {
      auto& o_base = front.reduced_object_value();
      for (auto object : objects) {
        auto& o_cmp = object->reduced_object_value();
        if (o_base.class_module() != o_cmp.class_module() ||
            o_base.class_name() != o_cmp.class_name() ||
            o_cmp.has_items() || o_cmp.has_kvpairs()) {
          BatchSerializedObjectsAsList(objects, result);
          return absl::OkStatus();
        }
      }
      std::vector<const SerializedObject*> children;
      children.reserve(objects.size());
      for (auto& o : objects) {
        children.push_back(&o->reduced_object_value().args());
      }
      auto reduced_object = result->mutable_reduced_object_value();
      auto args = reduced_object->mutable_args();
      reduced_object->set_class_module(o_base.class_module());
      reduced_object->set_class_name(o_base.class_name());
      if (o_base.class_module() ==
          "tensorflow.python.framework.ops" &&
          o_base.class_name() == "convert_to_tensor") {
        return BatchSerializedObjects(children, args);
      } else if (o_base.class_module() == "copyreg" &&
                 o_base.class_name() == "__newobj__" &&
                 o_base.has_state() &&
                 o_base.state().payload_case() ==
                 SerializedObject::PayloadCase::kNoneValue) {
        *reduced_object->mutable_state() = o_base.state();
        return BatchNewObject(children, args);
      }
      result->clear_reduced_object_value();
      BatchSerializedObjectsAsList(objects, result);
      return absl::OkStatus();
    }
    case SerializedObject::kDictValue: {
      absl::flat_hash_map<std::string, std::vector<const SerializedObject*>>
          batched_dict;
      for (auto object : objects) {
        const SerializedDict& dict = object->dict_value();
        if (dict.keys_size() != dict.values_size()) {
          return absl::InternalError("Dict keys/values size mismatch.");
        }
        for (int i = 0; i < dict.keys_size(); ++i) {
          batched_dict[dict.keys(i).ShortDebugString()].push_back(
              &dict.values(i));
        }
      }
      for (auto& i : batched_dict) {
        if (i.second.size() != objects.size()) {
          return absl::InternalError(
              "Mismatching keys in the dictionaries to batch.");
        }
      }

      auto dict = result->mutable_dict_value();
      for (auto& i : front.dict_value().keys()) {
        *dict->add_keys() = i;
        auto value = dict->add_values();
        COURIER_RETURN_IF_ERROR(
            BatchSerializedObjects(batched_dict[i.ShortDebugString()], value));
      }
      return absl::OkStatus();
    }
    case SerializedObject::kTensorValue: {
      auto tensor = result->mutable_tensor_value();
      std::vector<const tensorflow::TensorProto*> tensors;
      for (auto b : objects) {
        if (b->numpy_metadata() != front.numpy_metadata()) {
          return absl::InvalidArgumentError(
              "Incompatible numpy metadata across requests to batch.");
        }
        if (b->has_numpy_object_tensor()) {
          return absl::InvalidArgumentError(
              "Batching of numpy object tensors is not supported currently.");
        }
        tensors.push_back(&b->tensor_value());
      }
      result->set_numpy_metadata(front.numpy_metadata());
      return BatchTensors(tensors, tensor);
    }
    case SerializedObject::kJaxTensorValue: {
      auto tensor = result->mutable_jax_tensor_value();
      std::vector<const tensorflow::TensorProto*> tensors;
      tensors.reserve(objects.size());
      for (auto b : objects) {
        tensors.push_back(&b->jax_tensor_value());
      }
      return BatchTensors(tensors, tensor);
    }
    default:
      return absl::InternalError(
          "No value set. The object is likely corrupted.");
  }
}

}  // namespace courier
