load("@com_github_grpc_grpc//bazel:cc_grpc_library.bzl", "cc_grpc_library")

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

proto_library(
    name = "courier_service_proto",
    srcs = ["courier_service.proto"],
    deps = ["//courier/serialization:serialization_proto"],
)

cc_proto_library(
    name = "courier_service_cc_proto_static",
    deps = ["courier_service_proto"],
)

cc_shared_library(
    name = "courier_service_cc_proto_shared",
    shared_lib_name = "libcourier_service_cc_proto.dylib",
    user_link_flags = ["-Wl,-install_name,@rpath/libcourier_service_cc_proto.dylib"],
    deps = [
        ":courier_service_cc_proto_static",
    ],
)

cc_library(
    name = "courier_service_cc_proto",
    srcs = [":courier_service_cc_proto_shared"],
    # We only want the courier_service.pb.h file, not the static library.
    hdrs = [":courier_service_cc_proto_static"],
    deps = [
        "//courier/serialization:serialization_cc_proto",
    ],
)

# This rule does publish the grpc.pb.h file in the hdrs.
cc_grpc_library(
    name = "courier_service_cc_grpc_proto_static",
    srcs = [":courier_service_proto"],
    grpc_only = True,
    deps = [":courier_service_cc_proto"],
)

cc_shared_library(
    name = "courier_service_cc_grpc_proto_shared",
    # dynamic_deps = [":courier_service_cc_proto_shared"],
    shared_lib_name = "libcourier_service_cc_grpc_proto.dylib",
    user_link_flags = ["-Wl,-install_name,@rpath/libcourier_service_cc_grpc_proto.dylib"],
    deps = [":courier_service_cc_grpc_proto_static"],
)

cc_library(
    name = "courier_service_cc_grpc_proto",
    srcs = [":courier_service_cc_grpc_proto_shared"],
    deps = [
        "//courier:courier_service_cc_grpc_proto_static",
    ],
)

cc_library(
    name = "router",
    srcs = [":router_shared"],
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
    ],
)

cc_shared_library(
    name = "router_shared",
    shared_lib_name = "librouter.dylib",
    user_link_flags = ["-Wl,-install_name,@rpath/librouter.dylib"],
    deps = [":router_static"],
)

cc_library(
    name = "router_static",
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
    ],
)

cc_library(
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

cc_library(
    name = "server_hdr",
    hdrs = ["server.h"],
    deps = [
        ":router",
        ":tf_serialize",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_library(
    name = "client",
    srcs = [":client_shared"],
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

cc_shared_library(
    name = "client_shared",
    shared_lib_name = "libclient.dylib",
    user_link_flags = ["-Wl,-install_name,@rpath/libclient.dylib"],
    deps = [":client_static"],
)

cc_library(
    name = "client_static",
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

cc_library(
    name = "tf_serialize",
    srcs = [":tf_serialize_shared"],
    hdrs = ["tf_serialize.h"],
    deps = [
        "//courier/serialization:serialization_cc_proto",
        "//courier/serialization:serialize",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
    ],
)

cc_shared_library(
    name = "tf_serialize_shared",
    shared_lib_name = "libtf_serialize.dylib",
    user_link_flags = ["-Wl,-install_name,@rpath/libtf_serialize.dylib"],
    deps = [":tf_serialize_static"],
)

cc_library(
    name = "tf_serialize_static",
    srcs = ["tf_serialize.cc"],
    hdrs = ["tf_serialize.h"],
    deps = [
        "//courier/serialization:serialization_cc_proto",
        "//courier/serialization:serialize",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "address_interceptor",
    srcs = [":address_interceptor_shared"],
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

cc_shared_library(
    name = "address_interceptor_shared",
    shared_lib_name = "libaddress_interceptor.dylib",
    user_link_flags = ["-Wl,-install_name,@rpath/libaddress_interceptor.dylib"],
    deps = [":address_interceptor_static"],
)

cc_library(
    name = "address_interceptor_static",
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
