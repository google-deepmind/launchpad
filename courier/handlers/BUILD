package(default_visibility = ["//visibility:public"])

licenses(["notice"])

cc_library(
    name = "interface",
    hdrs = ["interface.h"],
    deps = [
        "//courier/serialization:serialization_cc_proto",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "py_call",
    srcs = ["py_call.cc"],
    hdrs = ["py_call.h"],
    features = ["-use_header_modules"],
    deps = [
        ":helpers",
        ":interface",
        "//courier/platform:logging",
        "//courier/platform:status_macros",
        "//courier/serialization:py_serialize",
        "//courier/serialization:serialization_cc_proto",
        "//courier/serialization:tensor_conversion",
        "@com_google_absl//absl/base",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@pybind11",
    ],
)

cc_library(
    name = "handler_batcher",
    srcs = ["handler_batcher.cc"],
    hdrs = ["handler_batcher.h"],
    features = ["-use_header_modules"],
    deps = [
        ":interface",
        "//courier/platform:status_macros",
        "//courier/serialization:batching",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/synchronization",
        "@local_config_tf//:tf_header_lib",
    ],
)

cc_library(
    name = "helpers",
    srcs = ["helpers.cc"],
    hdrs = ["helpers.h"],
    deps = [
        "//courier/platform:logging",
        "//courier/platform/default:py_utils",
        "@com_google_absl//absl/status",
    ],
)
