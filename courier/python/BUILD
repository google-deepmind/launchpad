load("@pybind11_bazel//:build_defs.bzl", "pybind_extension")

licenses(["notice"])

package(default_visibility = ["//visibility:public"])

pybind_extension(
    name = "router",
    srcs = ["router.cc"],
    copts = [
        "-fno-strict-aliasing",
    ],
    linkopts = [
        "-Wl,-install_name,@rpath/router.so",
        "-Wl,-rpath,@loader_path/..",
        "-Wl,-rpath,@loader_path/../platform/default",
    ],
    deps = [
        "//courier:router",
        "@pybind11_abseil//pybind11_abseil:absl_casters",
        "@pybind11_abseil//pybind11_abseil:status_casters",
    ],
)

pybind_extension(
    name = "server",
    srcs = ["server.cc"],
    copts = [
        "-fno-strict-aliasing",
    ],
    linkopts = [
        "-Wl,-install_name,@rpath/server.so",
        "-Wl,-rpath,@loader_path/..",
        "-Wl,-rpath,@loader_path/../platform/default",
    ],
    deps = [
        "//courier:router",
        "//courier:server",
        "@pybind11_abseil//pybind11_abseil:absl_casters",
        "@pybind11_abseil//pybind11_abseil:status_casters",
    ],
)

pybind_extension(
    name = "py_client",
    srcs = [
        "py_client.cc",
        "py_client.h",
    ],
    copts = [
        "-fno-strict-aliasing",
    ],
    linkopts = [
        "-Wl,-install_name,@rpath/py_client.so",
        "-Wl,-rpath,@loader_path/..",
        "-Wl,-rpath,@loader_path/../platform/default",
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
        "@upb",
    ],
)

py_library(
    name = "client",
    srcs = ["client.py"],
    data = [":py_client.so"],
    srcs_version = "PY3",
)

py_library(
    name = "py_server_no_tf",
    srcs = ["py_server.py"],
    data = [
        ":router.so",
        ":server.so",
        "//courier/handlers/python:pybind.so",
    ],
    srcs_version = "PY3",
    tags = [
        "ignore_for_dep=third_party.py.tensorflow.compat.v1",
    ],
)

py_library(
    name = "py_server",
    srcs_version = "PY3",
    deps = [":py_server_no_tf"],
)
