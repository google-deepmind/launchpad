load("//launchpad:build_defs.bzl", "lp_library", "lp_test")

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

lp_library(
    name = "base",
    srcs = ["base.py"],
    srcs_version = "PY3",
    deps = [
        ":dereference",
        "//launchpad:address",
        "//launchpad:context",
    ],
)

lp_library(
    name = "dereference",
    srcs = ["dereference.py"],
    srcs_version = "PY3",
)
