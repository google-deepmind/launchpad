load("//launchpad:build_defs.bzl", "lp_library", "lp_test")

licenses(["notice"])

package(default_visibility = ["//visibility:public"])

lp_library(
    name = "addressing",
    srcs = ["addressing.py"],
    srcs_version = "PY3",
    deps = ["//launchpad:address"],
)

lp_library(
    name = "flags_utils",
    srcs = ["flags_utils.py"],
    srcs_version = "PY3",
)

lp_library(
    name = "node",
    srcs = ["node.py"],
    srcs_version = "PY3",
    deps = [
        ":addressing",
        ":local_multi_processing",
        "//launchpad:context",
        "//launchpad/nodes:base",
        "//launchpad/nodes:dereference",
    ],
)

lp_library(
    name = "local_multi_processing",
    srcs = ["local_multi_processing.py"],
    srcs_version = "PY3",
    deps = [":flags_utils"],
)
