load("//launchpad:build_defs.bzl", "lp_cc_grpc_library", "lp_cc_library", "lp_cc_proto_library")

package(
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])

exports_files(["LICENSE"])

# Full version of Courier, including TensorFlow support.
py_library(
    name = "courier",
    srcs_version = "PY3",
    deps = [":courier_no_tf"],
)

# Light-weight Courier version without TensorFlow support.
py_library(
    name = "courier_no_tf",
    srcs = ["__init__.py"],
    srcs_version = "PY3",
    deps = [
        "//courier/python:client",
        "//courier/python:py_server_no_tf",
    ],
)

lp_cc_proto_library(
    name = "courier_service_cc_proto",
    srcs = ["courier_service.proto"],
    deps = ["//courier/serialization:serialization_cc_proto"],
)

lp_cc_grpc_library(
    name = "courier_service_cc_grpc_proto",
    srcs = ["courier_service.proto"],
    deps = [":courier_service_cc_proto"],
)

lp_cc_library(
    name = "router",
    srcs = ["router.cc"],
    hdrs = ["router.h"],
    deps = [
        "//courier/handlers:interface",
        "//courier/serialization:serialization_cc_proto",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@tensorflow_includes//:includes",
    ],
)

lp_cc_library(
    name = "server",
    hdrs = ["server.h"],
    deps = [
        ":router",
        ":tf_serialize",
        "//courier/platform/default:server",  # buildcleaner: keep
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
    ],
)

lp_cc_library(
    name = "server_hdr",
    hdrs = ["server.h"],
    deps = [
        ":router",
        ":tf_serialize",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
    ],
)

lp_cc_library(
    name = "client",
    srcs = [
        "call_context.cc",
        "client.cc",
    ],
    hdrs = [
        "call_context.h",
        "client.h",
    ],
    deps = [
        ":address_interceptor",
        ":courier_service_cc_grpc_proto",
        ":courier_service_cc_proto",
        "//courier/platform:client_monitor",
        "//courier/platform:grpc_utils",
        "//courier/platform:logging",
        "//courier/platform:status_macros",
        "//courier/serialization:serialization_cc_proto",
        "//courier/serialization:serialize",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/functional:bind_front",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
    ],
)

lp_cc_library(
    name = "tf_serialize",
    srcs = ["tf_serialize.cc"],
    hdrs = ["tf_serialize.h"],
    deps = [
        "//courier/serialization:serialization_cc_proto",
        "//courier/serialization:serialize",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
    ],
)

lp_cc_library(
    name = "address_interceptor",
    srcs = ["address_interceptor.cc"],
    hdrs = ["address_interceptor.h"],
    deps = [
        "//courier/platform:logging",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
    ],
)
