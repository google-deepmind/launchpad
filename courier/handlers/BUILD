load("//launchpad:build_defs.bzl", "lp_cc_library")

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

lp_cc_library(
    name = "interface",
    hdrs = ["interface.h"],
    deps = [
        "//courier/serialization:serialization_cc_proto",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
    ],
)

lp_cc_library(
    name = "py_call",
    srcs = ["py_call.cc"],
    hdrs = ["py_call.h"],
    features = ["-use_header_modules"],
    deps = [
        ":interface",
        "//courier/platform:logging",
        "//courier/platform:status_macros",
        "//courier/platform:tensor_conversion",
        "//courier/platform/default:py_utils",
        "//courier/serialization:py_serialize",
        "//courier/serialization:serialization_cc_proto",
        "@com_google_absl//absl/base",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@pybind11",
    ],
)
