load(
    "//launchpad:build_defs.bzl",
    "lp_library",
    "lp_test",
)

licenses(["notice"])

package(default_visibility = ["//visibility:public"])

lp_library(
    name = "address_builder",
    srcs = ["address_builder.py"],
    srcs_version = "PY3",
    deps = [
        "//launchpad:address",
        "//launchpad/nodes:base",
    ],
)

lp_library(
    name = "launch",
    srcs = ["launch.py"],
    srcs_version = "PY3",
    deps = ["//launchpad:context"],
)

lp_test(
    name = "launch_test",
    srcs = ["launch_test.py"],
    python_version = "PY3",
    srcs_version = "PY3",
    deps = [
        ":launch",
        "//launchpad:program",
        "//launchpad/nodes/python:node",
    ],
)
