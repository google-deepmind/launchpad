load("@pybind11_bazel//:build_defs.bzl", "pybind_extension", "pybind_library")

licenses(["notice"])

package(default_visibility = ["//visibility:public"])

pybind_extension(
    name = "pybind",
    srcs = ["pybind.cc"],
    copts = [
        "-fno-strict-aliasing",
    ],
    deps = [
        ":batching",
        ":py_serialize",
        ":tensor_conversion",
        "//courier/platform:status_macros",
        "@pybind11_abseil//pybind11_abseil:absl_casters",
        "@pybind11_abseil//pybind11_abseil:status_casters",
    ],
)

proto_library(
    name = "serialization_proto",
    srcs = ["serialization.proto"],
    deps = [
        "@local_config_tf//:tensor_proto",
    ],
)

cc_proto_library(
    name = "serialization_cc_proto",
    deps = ["serialization_proto"],
)

cc_library(
    name = "pyobject_ptr",
    hdrs = ["pyobject_ptr.h"],
    deps = [
        "@pybind11",  # buildcleaner: keep
    ],
)

cc_library(
    name = "py_serialize",
    srcs = ["py_serialize.cc"],
    hdrs = ["py_serialize.h"],
    deps = [
        ":pyobject_ptr",
        ":serialization_cc_proto",
        ":tensor_conversion",
        "//courier/platform:status_macros",
        "//courier/platform/default:py_utils",
        "//reverb:conversions",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@local_config_tf//:libtensorflow_framework",
        "@local_config_tf//:tf_header_lib",
        "@pybind11",  # build_cleaner: keep
    ],
)

cc_library(
    name = "batching",
    srcs = ["batching.cc"],
    hdrs = ["batching.h"],
    deps = [
        # I am concerned that this is causing serialization proto to be registered more than once.
        ":serialization_cc_proto",
        ":tensor_conversion",
        "//courier/platform:status_macros",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
    ],
)

cc_library(
    name = "serialize",
    hdrs = [
        "serialize.h",
    ],
    deps = [
        # I am concerned that this is causing serialization proto to be registered more than once.
        ":serialization_cc_proto",
        "//courier/platform:status_macros",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        # Apparently I am dependent on this but LaunchPad apparently where not?
        "@local_config_tf//:tf_header_lib",
    ],
)

cc_library(
    name = "tensor_conversion",
    srcs = ["tensor_conversion.cc"],
    hdrs = ["tensor_conversion.h"],
    deps = [
        # # I am concerned that this is causing serialization proto to be registered more than once.
        ":serialization_cc_proto",
        "//courier/platform:status_macros",
        "//courier/platform/default:status_macros",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/synchronization",
        # Apparently tensor_conversion is dependent on the TensorFlow header files.
        "@local_config_tf//:tf_header_lib",
    ],
)

pybind_library(
    name = "pybind_serialize",
    srcs = ["pybind_serialize.cc"],
    hdrs = ["pybind_serialize.h"],
    deps = [
        ":py_serialize",
        # # I am concerned that this is causing serialization proto to be registered more than once.
        # ":serialization_cc_proto",
        "//courier/platform:status_macros",
        "@com_google_absl//absl/status",
    ],
)
