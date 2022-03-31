load("//launchpad:build_defs.bzl", "lp_cc_library", "lp_cc_proto_library", "lp_library", "lp_pybind_extension", "lp_pybind_library")

licenses(["notice"])

package(default_visibility = ["//visibility:public"])

lp_pybind_extension(
    name = "pybind",
    srcs = ["pybind.cc"],
    deps = [
        ":batching",
        ":py_serialize",
        ":tensor_conversion",
        "//courier/platform:status_macros",
        "@pybind11_abseil//pybind11_abseil:absl_casters",
        "@pybind11_abseil//pybind11_abseil:status_casters",
    ],
)

lp_cc_proto_library(
    name = "serialization_cc_proto",
    srcs = ["serialization.proto"],
)

lp_cc_library(
    name = "pyobject_ptr",
    hdrs = ["pyobject_ptr.h"],
    deps = [
        "@pybind11",  # buildcleaner: keep
    ],
)

lp_cc_library(
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
        "@pybind11",  # build_cleaner: keep
        "@tensorflow_includes//:includes",
        "@tensorflow_solib//:framework_lib",
    ],
)

lp_cc_library(
    name = "batching",
    srcs = ["batching.cc"],
    hdrs = ["batching.h"],
    deps = [
        ":serialization_cc_proto",
        ":tensor_conversion",
        "//courier/platform:status_macros",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
    ],
)

lp_cc_library(
    name = "serialize",
    hdrs = ["serialize.h"],
    deps = [
        ":serialization_cc_proto",
        "//courier/platform:status_macros",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
    ],
)

lp_cc_library(
    name = "tensor_conversion",
    srcs = ["tensor_conversion.cc"],
    hdrs = ["tensor_conversion.h"],
    deps = [
        ":serialization_cc_proto",
        "//courier/platform:status_macros",
        "//courier/platform/default:status_macros",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/synchronization",
    ],
)

lp_pybind_library(
    name = "pybind_serialize",
    srcs = ["pybind_serialize.cc"],
    hdrs = ["pybind_serialize.h"],
    deps = [
        ":py_serialize",
        ":serialization_cc_proto",
        "//courier/platform:status_macros",
        "@com_google_absl//absl/status",
    ],
)
