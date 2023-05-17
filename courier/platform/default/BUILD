# Platform-specific code for Courier
package(default_visibility = ["//visibility:public"])

licenses(["notice"])

exports_files(["LICENSE"])

cc_library(
    name = "client_monitor",
    srcs = [":client_monitor_shared"],
    # hdrs = ["//courier/platform:client_monitor_hdr"],
    deps = [
        "//courier/platform:client_monitor_hdr",
        "@com_github_grpc_grpc//:grpc++",
    ],
)

cc_shared_library(
    name = "client_monitor_shared",
    shared_lib_name = "libclient_monitor.dylib",
    user_link_flags = [
        "-Wl,-install_name,@rpath/libclient_monitor.dylib",
    ],
    deps = [":client_monitor_static"],
)

cc_library(
    name = "client_monitor_static",
    srcs = ["client_monitor.cc"],
    deps = [
        "//courier/platform:client_monitor_hdr",
        "@com_github_grpc_grpc//:grpc++",
    ],
)

cc_library(
    name = "py_utils",
    srcs = [":py_utils_shared"],
    hdrs = ["py_utils.h"],
    deps = ["@pybind11"],
)

cc_shared_library(
    name = "py_utils_shared",
    shared_lib_name = "libpy_utils.dylib",
    user_link_flags = [
        "-Wl,-install_name,@rpath/libpy_utils.dylib",
    ],
    deps = [":py_utils_static"],
)

cc_library(
    name = "py_utils_static",
    srcs = ["py_utils.cc"],
    hdrs = ["py_utils.h"],
    deps = ["@pybind11"],
)

cc_library(
    name = "status_macros",
    srcs = [":status_macros_shared"],
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

cc_shared_library(
    name = "status_macros_shared",
    shared_lib_name = "libstatus_macros.dylib",
    user_link_flags = [
        "-Wl,-install_name,@rpath/libstatus_macros.dylib",
    ],
    deps = [":status_macros_static"],
)

cc_library(
    name = "status_macros_static",
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

cc_library(
    name = "logging",
    srcs = [":logging_shared"],
    hdrs = ["logging.h"],
)

cc_shared_library(
    name = "logging_shared",
    shared_lib_name = "liblogging.dylib",
    user_link_flags = [
        "-Wl,-install_name,@rpath/liblogging.dylib",
    ],
    deps = [":logging_static"],
)

cc_library(
    name = "logging_static",
    srcs = ["logging.cc"],
    hdrs = ["logging.h"],
)

cc_library(
    name = "grpc_utils_static",
    srcs = ["grpc_utils.cc"],
    linkstatic = True,
    deps = [
        "//courier/platform:grpc_utils_hdr",
        "@com_github_grpc_grpc//:envoy_admin_upb",
        "@com_github_grpc_grpc//:grpc++",
    ],
    alwayslink = True,
)

cc_binary(
    name = "libgrpc_utils.dylib",
    # copts = ["-llibpkg.upbdefs.a"],
    linkopts = [
        "-Wl,-install_name,@rpath/libgrpc_utils.dylib",
    ],
    linkshared = True,
    linkstatic = False,
    deps = [
        ":grpc_utils_static",
        "//courier/platform:grpc_utils_hdr",
        "@com_github_grpc_grpc//:envoy_admin_upb",
        "@com_github_grpc_grpc//:grpc++",
    ],
)

cc_library(
    name = "grpc_utils",
    srcs = [":libgrpc_utils.dylib"],
    deps = [
        "//courier/platform:grpc_utils_hdr",
        "@com_github_grpc_grpc//:grpc++",
    ],
)

cc_library(
    name = "server",
    srcs = [":server_shared"],
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

cc_shared_library(
    name = "server_shared",
    shared_lib_name = "libserver.dylib",
    user_link_flags = [
        "-Wl,-install_name,@rpath/libserver.dylib",
    ],
    deps = [":server_static"],
)

cc_library(
    name = "server_static",
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

cc_library(
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
        "@local_config_tf//:tf_header_lib",
    ],
)

cc_shared_library(
    name = "courier_service_impl_shared",
    shared_lib_name = "libcourier_service_impl.dylib",
    user_link_flags = [
        "-Wl,-install_name,@rpath/libcourier_service_impl.dylib",
    ],
    deps = [":courier_service_impl_static"],
)

cc_library(
    name = "courier_service_impl_static",
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
        "@local_config_tf//:tf_header_lib",
    ],
)
