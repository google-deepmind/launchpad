# Description: Launchpad is a library that simplifies writing distributed
# programs and seamlessly launching them on a range of supported platforms.

load("//launchpad:build_defs.bzl", "lp_library", "lp_test")

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

exports_files(["LICENSE"])

lp_library(
    name = "program",
    srcs = ["program.py"],
    srcs_version = "PY3",
    deps = ["//launchpad/nodes:base"],
)

lp_library(
    name = "context",
    srcs = ["context.py"],
    srcs_version = "PY3",
)

lp_library(
    name = "address",
    srcs = ["address.py"],
    srcs_version = "PY3",
)
