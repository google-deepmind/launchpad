workspace(name = "launchpad")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")
load("//tf:tf_configure.bzl", "tf_configure")

http_archive(
    name = "pybind11_abseil",
    sha256 = "be5da399b4f62615fdc2a236674638480118f6030d7b16645c6d3f0e208a7f8f",
    strip_prefix = "pybind11_abseil-1bb411eb1b13440d5af61660e70e8c5b5b2998a1",
    urls = ["https://github.com/pybind/pybind11_abseil/archive/1bb411eb1b13440d5af61660e70e8c5b5b2998a1.zip"],
)

http_archive(
    name = "pybind11_bazel",
    sha256 = "8f546c03bdd55d0e88cb491ddfbabe5aeb087f87de2fbf441391d70483affe39",
    strip_prefix = "pybind11_bazel-26973c0ff320cb4b39e45bc3e4297b82bc3a6c09",
    urls = ["https://github.com/pybind/pybind11_bazel/archive/26973c0ff320cb4b39e45bc3e4297b82bc3a6c09.tar.gz"],
)

# We still require the pybind library.
http_archive(
    name = "pybind11",
    build_file = "@pybind11_bazel//:pybind11.BUILD",
    sha256 = "eacf582fa8f696227988d08cfc46121770823839fe9e301a20fbce67e7cd70ec",
    strip_prefix = "pybind11-2.10.0",
    urls = ["https://github.com/pybind/pybind11/archive/v2.10.0.tar.gz"],
)

http_archive(
    name = "absl_py",
    sha256 = "a7c51b2a0aa6357a9cbb2d9437e8cd787200531867dc02565218930b6a32166e",
    strip_prefix = "abseil-py-pypi-v1.0.0",
    urls = [
        "https://github.com/abseil/abseil-py/archive/refs/tags/v1.0.0.tar.gz",
    ],
)

load("@pybind11_bazel//:python_configure.bzl", "python_configure")

python_configure(name = "local_config_python")

git_repository(
    name = "com_google_snappy",
    commit = "c9f9edf6d75bb065fa47468bf035e051a57bec7c",
    remote = "https://github.com/google/snappy",
)

http_archive(
    name = "com_github_grpc_grpc",
    strip_prefix = "grpc-1.54.1",
    urls = ["https://github.com/grpc/grpc/archive/refs/tags/v1.54.1.tar.gz"],
)

ABSL_COMMIT = "273292d1cfc0a94a65082ee350509af1d113344d"

ABSL_SHA256 = "94aef187f688665dc299d09286bfa0d22c4ecb86a80b156dff6aabadc5a5c26d"

http_archive(
    name = "com_google_absl",
    sha256 = ABSL_SHA256,
    strip_prefix = "abseil-cpp-{commit}".format(commit = ABSL_COMMIT),
    urls = ["https://github.com/abseil/abseil-cpp/archive/{commit}.tar.gz".format(commit = ABSL_COMMIT)],
)

load("@com_github_grpc_grpc//bazel:grpc_deps.bzl", "grpc_deps")

grpc_deps()

load("@com_github_grpc_grpc//bazel:grpc_extra_deps.bzl", "grpc_extra_deps")

grpc_extra_deps()

tf_configure(name = "local_config_tf")
