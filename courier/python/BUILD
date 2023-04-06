load("//launchpad:build_defs.bzl", "lp_library", "lp_pybind_extension", "lp_test")

licenses(["notice"])

package(default_visibility = ["//visibility:public"])

lp_pybind_extension(
    name = "router",
    srcs = ["router.cc"],
    deps = [
        "//courier:router",
        "@pybind11",
        "@pybind11_abseil//pybind11_abseil:absl_casters",
        "@pybind11_abseil//pybind11_abseil:status_casters",
    ],
)

lp_pybind_extension(
    name = "server",
    srcs = ["server.cc"],
    deps = [
        "//courier:router",
        "//courier:server",
        "@pybind11",
        "@pybind11_abseil//pybind11_abseil:absl_casters",
        "@pybind11_abseil//pybind11_abseil:status_casters",
    ],
)

lp_pybind_extension(
    name = "py_client",
    srcs = [
        "py_client.cc",
        "py_client.h",
    ],
    deps = [
        "//courier:client",
        "//courier/platform:logging",
        "//courier/platform:status_macros",
        "//courier/serialization:py_serialize",
        "//courier/serialization:pybind_serialize",
        "//courier/serialization:serialization_cc_proto",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
        "@pybind11_abseil//pybind11_abseil:absl_casters",
        "@pybind11_abseil//pybind11_abseil:status_casters",
    ],
)

lp_library(
    name = "client",
    srcs = ["client.py"],
    srcs_version = "PY3",
    deps = [":py_client"],
)

lp_library(
    name = "py_server_no_tf",
    srcs = ["py_server.py"],
    srcs_version = "PY3",
    tags = [
        "ignore_for_dep=third_party.py.tensorflow.compat.v1",
    ],
    deps = [
        ":router",
        ":server",
        "//courier/handlers/python:pybind",
    ],
)

lp_library(
    name = "py_server",
    srcs_version = "PY3",
    deps = [":py_server_no_tf"],
)
