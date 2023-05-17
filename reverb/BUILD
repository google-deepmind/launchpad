package(default_visibility = ["//visibility:public"])

licenses(["notice"])

exports_files(["LICENSE"])

cc_library(
    name = "conversions",
    srcs = ["conversions.cc"],
    hdrs = ["conversions.h"],
    deps = [
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@local_config_python//:python_headers",
        "@local_config_tf//:libtensorflow_framework",
        "@local_config_tf//:numpy_headers",
        "@local_config_tf//:tf_header_lib",
    ],
)
