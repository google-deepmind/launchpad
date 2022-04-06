workspace(name = "launchpad")

# To change to a version of protoc compatible with tensorflow:
#  1. Convert the required header version to a version string, e.g.:
#     3011004 => "3.11.4"
#  2. Calculate the sha256 of the binary:
#     PROTOC_VERSION="3.11.4"
#     curl -L "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-x86_64.zip" | sha256sum
#  3. Update the two variables below.
#
PROTOC_VERSION = "3.9.0"
PROTOC_SHA256 = "15e395b648a1a6dda8fd66868824a396e9d3e89bc2c8648e3b9ab9801bea5d55"
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

load(
    "//launchpad:repo.bzl",
    "absl_deps",
    "cc_tf_configure",
    "googletest_deps",
    "github_grpc_deps",
    "protoc_deps",
    "python_deps",
)

googletest_deps()

absl_deps()
http_archive(
  name = "pybind11_abseil",
  strip_prefix = "pybind11_abseil-1bb411eb1b13440d5af61660e70e8c5b5b2998a1",
  sha256 = "be5da399b4f62615fdc2a236674638480118f6030d7b16645c6d3f0e208a7f8f",
  urls = ["https://github.com/pybind/pybind11_abseil/archive/1bb411eb1b13440d5af61660e70e8c5b5b2998a1.zip"],
)

http_archive(
  name = "pybind11_bazel",
  strip_prefix = "pybind11_bazel-26973c0ff320cb4b39e45bc3e4297b82bc3a6c09",
  sha256 = "8f546c03bdd55d0e88cb491ddfbabe5aeb087f87de2fbf441391d70483affe39",
  urls = ["https://github.com/pybind/pybind11_bazel/archive/26973c0ff320cb4b39e45bc3e4297b82bc3a6c09.tar.gz"],
)

## Begin GRPC related deps
github_grpc_deps()
load("@com_github_grpc_grpc//bazel:grpc_deps.bzl", "grpc_deps")

grpc_deps()

load("@com_github_grpc_grpc//bazel:grpc_extra_deps.bzl", "grpc_extra_deps")

grpc_extra_deps()


load("@upb//bazel:workspace_deps.bzl", "upb_deps")

upb_deps()

load(
    "@build_bazel_rules_apple//apple:repositories.bzl",
    "apple_rules_dependencies",
)

apple_rules_dependencies()

load(
    "@build_bazel_apple_support//lib:repositories.bzl",
    "apple_support_dependencies",
)

apple_support_dependencies()
## End GRPC related deps

cc_tf_configure()

python_deps()

protoc_deps(version = PROTOC_VERSION, sha256 = PROTOC_SHA256)
