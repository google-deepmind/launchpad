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

#ifndef COURIER_SERIALIZATION_SERIALIZE_H_
#define COURIER_SERIALIZATION_SERIALIZE_H_

// Two main functions are provided by this header file:
//
// string SerializeToObject(T value);
//
// T DeserializeFromObject(const string& serialized);
//
// Example Usage:
//
// using MyDataStructure = absl::flat_hash_map<string, std::vector<float>>;
//
// MyDataStructure mds = ... ;
//
// auto s = SerializeToObject(mds);
//
// MyDataStructure mds2;
// DeserializeFromObject(s, &mds);
//
//
// These functions implemented via template specializations of the
// two classes Serializer<> and Deserializer<>.
//
// Implementations are currently available for basic types as well as
// string, vector, map and protocol buffers.
//
// Additional specializations can be provided for other custom types.

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/types/span.h"
#include "courier/platform/status_macros.h"
#include "courier/serialization/serialization.pb.h"

namespace courier {

template <class Tuple, class F>
constexpr F for_each(Tuple&& t, F&& f) {
  return for_each_impl(
      std::forward<Tuple>(t), std::forward<F>(f),
      std::make_index_sequence<
          std::tuple_size<std::remove_reference_t<Tuple>>::value>{});
}

template <class Tuple, class F, std::size_t... I>
constexpr F for_each_impl(Tuple&& t, F&& f, std::index_sequence<I...>) {
  return (void)std::initializer_list<int>{
             (std::forward<F>(f)(std::get<I>(std::forward<Tuple>(t))), 0)...},
         f;
}

// Basic Serializer for numbers.
// Specialization for other types are provided below.
template <typename T>
struct Serializer {
  static_assert(std::is_arithmetic<T>::value,
                "Serializer<T>: T must be a numeric type.");
  static absl::Status Write(const T& value, courier::SerializedObject* buffer) {
    if (std::is_floating_point<T>::value) {
      buffer->set_double_value(value);
    } else {
      buffer->set_int_value(value);
    }
    return absl::OkStatus();
  }
};

// Basic Deserializer for numbers.
// Specialization for other types are provided below.
template <typename T>
struct Deserializer {
  static_assert(std::is_arithmetic<T>::value,
                "Deserializer<T>: T must be a numeric type.");
  static absl::Status Read(const courier::SerializedObject& buffer, T* value) {
    if (value == nullptr) {
      return absl::InvalidArgumentError("Cannot deserialize to nullptr.");
    }
    if (std::is_floating_point<T>::value) {
      if (buffer.payload_case() != courier::SerializedObject::kDoubleValue) {
        return absl::InvalidArgumentError("Expected buffer to be float.");
      }
      *value = static_cast<T>(buffer.double_value());
    } else {
      if (buffer.payload_case() != courier::SerializedObject::kIntValue) {
        return absl::InvalidArgumentError("Expected buffer to be integer.");
      }
      *value = static_cast<T>(buffer.int_value());
    }
    return absl::OkStatus();
  }
};

// Template specializations below

template <>
struct Serializer<bool> {
  static absl::Status Write(const bool value,
                            courier::SerializedObject* buffer) {
    buffer->set_bool_value(value);
    return absl::OkStatus();
  }
};

template <>
struct Deserializer<bool> {
  static absl::Status Read(const courier::SerializedObject& buffer,
                           bool* value) {
    if (buffer.payload_case() != courier::SerializedObject::kBoolValue) {
      return absl::InvalidArgumentError("Expected buffer to be a bool.");
    }
    *value = buffer.bool_value();
    return absl::OkStatus();
  }
};

// Strings: we serialize both string and const char*
// but we only deserialize into string.
// We could easily add support for Cords if needed.
template <>
struct Serializer<std::string> {
  static absl::Status Write(const std::string& value,
                            courier::SerializedObject* buffer) {
    buffer->set_string_value(value);
    return absl::OkStatus();
  }
};

template <>
struct Serializer<const char*> {
  static absl::Status Write(const char* value,
                            courier::SerializedObject* buffer) {
    buffer->set_string_value(value);
    return absl::OkStatus();
  }
};

template <>
struct Deserializer<std::string> {
  static absl::Status Read(const courier::SerializedObject& buffer,
                           std::string* value) {
    if (buffer.payload_case() != courier::SerializedObject::kStringValue) {
      return absl::InvalidArgumentError("Expected buffer to be a string.");
    }
    *value = buffer.string_value();
    return absl::OkStatus();
  }
};

// Vectors (the type of the values can be any type we can serialize).

template <typename T>
struct Serializer<std::vector<T>> {
  static absl::Status Write(const std::vector<T>& value,
                            courier::SerializedObject* buffer) {
    return Serializer<absl::Span<const T>>::Write(absl::MakeConstSpan(value),
                                                  buffer);
  }
};

template <typename T>
struct Deserializer<std::vector<T>> {
  static absl::Status Read(const courier::SerializedObject& buffer,
                           std::vector<T>* result_vector) {
    if (!buffer.has_list_value()) {
      return absl::InvalidArgumentError("Expected buffer to be a list.");
    }
    const courier::SerializedList& list = buffer.list_value();
    for (const courier::SerializedObject& item : list.items()) {
      T value;
      COURIER_RETURN_IF_ERROR(Deserializer<T>::Read(item, &value));
      result_vector->push_back(std::move(value));
    }
    return absl::OkStatus();
  }
};

// absl::Span.

template <typename T>
struct Serializer<absl::Span<const T>> {
  static absl::Status Write(absl::Span<const T> value,
                            courier::SerializedObject* buffer) {
    courier::SerializedList* list = buffer->mutable_list_value();
    for (const T& item : value) {
      COURIER_RETURN_IF_ERROR(Serializer<T>::Write(item, list->add_items()));
    }
    return absl::OkStatus();
  }
};

// Unordered maps (the key and value types must be serializable).

template <typename K, typename V>
struct Serializer<absl::flat_hash_map<K, V>> {
  static absl::Status Write(const absl::flat_hash_map<K, V>& value,
                            courier::SerializedObject* buffer) {
    courier::SerializedDict* dict = buffer->mutable_dict_value();
    for (const auto& item : value) {
      COURIER_RETURN_IF_ERROR(
          Serializer<K>::Write(item.first, dict->add_keys()));
      COURIER_RETURN_IF_ERROR(
          Serializer<V>::Write(item.second, dict->add_values()));
    }
    return absl::OkStatus();
  }
};

template <typename K, typename V>
struct Deserializer<absl::flat_hash_map<K, V>> {
  static absl::Status Read(const courier::SerializedObject& buffer,
                           absl::flat_hash_map<K, V>* result_map) {
    if (!buffer.has_dict_value()) {
      return absl::InvalidArgumentError("Expected buffer to be a dict.");
    }
    const courier::SerializedDict& dict = buffer.dict_value();
    if (dict.keys_size() != dict.values_size()) {
      return absl::InternalError("Dict keys/values do not match in size.");
    }
    for (int i = 0; i < dict.keys_size(); ++i) {
      K key;
      COURIER_RETURN_IF_ERROR(Deserializer<K>::Read(dict.keys(i), &key));
      V value;
      COURIER_RETURN_IF_ERROR(Deserializer<V>::Read(dict.values(i), &value));
      (*result_map)[std::move(key)] = std::move(value);
    }
    return absl::OkStatus();
  }
};

// Tuples

// Serializer that takes a tuple and serializes it to a
// courier::SerializedObject proto.
template <typename... Args>
class Serializer<std::tuple<Args...>> {
 public:
  static absl::Status Write(const std::tuple<Args...>& value,
                            courier::SerializedObject* buffer) {
    return WriteRepeated(value, buffer->mutable_list_value()->mutable_items());
  }

  static absl::Status WriteRepeated(
      const std::tuple<Args...>& value,
      google::protobuf::RepeatedPtrField<courier::SerializedObject>* buffer) {
    absl::Status status;
    for_each(value, TupleSerializer(buffer, &status));
    return status;
  }

 private:
  // Internally used to serialize each element of the tuple and write it to
  // a courier::SerializedObject proto. This is used by for_each.
  struct TupleSerializer {
    explicit TupleSerializer(
        google::protobuf::RepeatedPtrField<courier::SerializedObject>* buffer,
        absl::Status* status)
        : buffer_ptr(buffer), status_ptr(status) {}
    template <class T>
    void operator()(const T& elem) const {
      status_ptr->Update(Serializer<T>::Write(elem, buffer_ptr->Add()));
    }
    google::protobuf::RepeatedPtrField<courier::SerializedObject>* buffer_ptr;
    absl::Status* status_ptr;
  };
};

// Deserializer that deserializes a repeated courier::SerializedObject proto
// to a tuple.
template <typename... Args>
class Deserializer<std::tuple<Args...>> {
 public:
  static absl::Status ReadRepeated(
      const google::protobuf::RepeatedPtrField<courier::SerializedObject>& items,
      std::tuple<Args...>* result_tuple) {
    if (sizeof...(Args) != items.size()) {
      return absl::InvalidArgumentError(
          absl::StrCat("Expected vector to have ", sizeof...(Args),
                       " elements, found ", items.size(), " instead."));
    }

    absl::Status status;
    for_each(*result_tuple, TupleDeserializer(items, &status));
    return status;
  }

  static absl::Status Read(const courier::SerializedObject& buffer,
                           std::tuple<Args...>* result_tuple) {
    if (!buffer.has_list_value()) {
      return absl::InvalidArgumentError("Expected buffer to be a list.");
    }
    return ReadRepeated(buffer.list_value().items(), result_tuple);
  }

 private:
  struct TupleDeserializer {
    explicit TupleDeserializer(
        const google::protobuf::RepeatedPtrField<courier::SerializedObject>& items,
        absl::Status* status)
        : items_view(items), status_ptr(status) {}
    template <class T>
    void operator()(T& elem) {
      status_ptr->Update(Deserializer<T>::Read(items_view.Get(pos), &elem));
      pos++;
    }
    const google::protobuf::RepeatedPtrField<courier::SerializedObject>& items_view;
    absl::Status* status_ptr;
    int pos = 0;
  };
};

// Main serialize and deserialize functions.

template <typename T>
absl::Status SerializeToObject(const T& value,
                               courier::SerializedObject* buffer) {
  return Serializer<T>::Write(value, buffer);
}

template <typename T>
absl::Status DeserializeFromObject(const courier::SerializedObject& buffer,
                                   T* value) {
  return Deserializer<T>::Read(buffer, value);
}

template <typename T>
absl::Status SerializeToRepeatedObject(
    const T& value,
    google::protobuf::RepeatedPtrField<courier::SerializedObject>* buffer) {
  return Serializer<T>::WriteRepeated(value, buffer);
}

template <typename T>
absl::Status DeserializeFromRepeatedObject(
    const google::protobuf::RepeatedPtrField<courier::SerializedObject>& items,
    T* result) {
  return Deserializer<T>::ReadRepeated(items, result);
}

}  // namespace courier

#endif  // COURIER_SERIALIZATION_SERIALIZE_H_
