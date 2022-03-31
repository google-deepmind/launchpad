load("//launchpad:build_defs.bzl", "lp_cc_library")

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

exports_files(["LICENSE"])

lp_cc_library(
    name = "conversions",
    srcs = ["conversions.cc"],
    hdrs = ["conversions.h"],
    deps = [
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@tensorflow_includes//:includes",
        "@tensorflow_solib//:framework_lib",
    ],
)
