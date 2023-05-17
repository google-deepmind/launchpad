# Platform-specific code for reverb
package(default_visibility = ["//visibility:public"])

licenses(["notice"])

cc_library(
    name = "status_macros",
    hdrs = ["status_macros.h"],
    deps = ["//courier/platform/default:status_macros"],
)

cc_library(
    name = "grpc_utils_hdr",
    hdrs = ["grpc_utils.h"],
    deps = [
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "grpc_utils",
    hdrs = ["grpc_utils.h"],
    deps = [
        "//courier/platform/default:grpc_utils",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "client_monitor_hdr",
    hdrs = ["client_monitor.h"],
    deps = ["@com_github_grpc_grpc//:grpc++"],
)

cc_library(
    name = "client_monitor",
    hdrs = ["client_monitor.h"],
    deps = [
        "//courier/platform/default:client_monitor",
        "@com_github_grpc_grpc//:grpc++",
    ],
)

cc_library(
    name = "logging",
    hdrs = ["logging.h"],
    deps = ["//courier/platform/default:logging"],
)
