# Platform-specific code for Courier

load("//launchpad:build_defs.bzl", "lp_cc_library")

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

exports_files(["LICENSE"])

lp_cc_library(
    name = "client_monitor",
    srcs = ["client_monitor.cc"],
    deps = [
        "//courier/platform:client_monitor_hdr",
        "@com_github_grpc_grpc//:grpc++",
    ],
)

lp_cc_library(
    name = "py_utils",
    srcs = ["py_utils.cc"],
    hdrs = ["py_utils.h"],
    deps = [
        "@pybind11",
    ],
)

lp_cc_library(
    name = "status_macros",
    srcs = ["status_builder.cc"],
    hdrs = [
        "source_location.h",
        "status_builder.h",
        "status_macros.h",
        "status_matchers.h",
    ],
    deps = [
        "@com_google_absl//absl/status",
    ],
)

lp_cc_library(
    name = "logging",
    srcs = ["logging.cc"],
    hdrs = ["logging.h"],
)

lp_cc_library(
    name = "grpc_utils",
    srcs = ["grpc_utils.cc"],
    deps = [
        "//courier/platform:grpc_utils_hdr",
        "@com_github_grpc_grpc//:grpc++",
    ],
    alwayslink = 1,
)

lp_cc_library(
    name = "server",
    srcs = ["server.cc"],
    deps = [
        ":courier_service_impl",
        "//courier:router",
        "//courier:server_hdr",
        "//courier/platform:grpc_utils",
        "//courier/platform:logging",
        "//courier/platform:status_macros",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
    ],
)

lp_cc_library(
    name = "courier_service_impl",
    srcs = ["courier_service_impl.cc"],
    hdrs = ["courier_service_impl.h"],
    deps = [
        "//courier:courier_service_cc_grpc_proto",
        "//courier:courier_service_cc_proto",
        "//courier:router",
        "//courier/platform:logging",
        "//courier/platform:status_macros",
        "//courier/serialization:serialization_cc_proto",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/functional:bind_front",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
    ],
)
