load("//launchpad:build_defs.bzl", "lp_library", "lp_test")

licenses(["notice"])

package(default_visibility = ["//visibility:public"])

lp_library(
    name = "node",
    srcs = ["node.py"],
    srcs_version = "PY3",
    deps = [
        ":courier_utils",
        "//courier",
        "//launchpad:address",
        "//launchpad/nodes:base",
        "//launchpad/nodes/python:node",
    ],
)

lp_library(
    name = "courier_utils",
    srcs = ["courier_utils.py"],
    srcs_version = "PY3",
    deps = ["//courier"],
)

lp_test(
    name = "node_test",
    srcs = ["node_test.py"],
    python_version = "PY3",
    srcs_version = "PY3",
    deps = [
        ":node",
        "//courier",
        "//launchpad/launch/test_multi_threading:address_builder",
    ],
)
