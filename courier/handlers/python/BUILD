load("//launchpad:build_defs.bzl", "lp_pybind_extension")

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

lp_pybind_extension(
    name = "pybind",
    srcs = ["pybind.cc"],
    deps = [
        "//courier/handlers:handler_batcher",
        "//courier/handlers:interface",
        "//courier/handlers:py_call",
        "//courier/platform:status_macros",
        "//courier/serialization:py_serialize",
        "//courier/serialization:pybind_serialize",
        "//courier/serialization:serialization_cc_proto",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
        "@pybind11_abseil//pybind11_abseil:absl_casters",
        "@pybind11_abseil//pybind11_abseil:status_casters",
    ],
)
