"""Launchpad build rules."""

load("@com_github_grpc_grpc//bazel:python_rules.bzl", "py_grpc_library")

def lp_copts():
    return ["-Wno-sign-compare"]

def lp_cc_library(
        name,
        srcs = [],
        hdrs = [],
        deps = [],
        testonly = 0,
        **kwargs):
    if testonly:
        new_deps = [
            "@com_google_googletest//:gtest",
            "@tensorflow_includes//:includes",
            "@tensorflow_solib//:framework_lib",
        ]
    else:
        new_deps = []
    native.cc_library(
        name = name,
        srcs = srcs,
        hdrs = hdrs,
        copts = lp_copts(),
        testonly = testonly,
        deps = depset(deps + new_deps),
        **kwargs
    )

def lp_kernel_library(name, srcs = [], deps = [], **kwargs):
    deps = deps + lp_tf_deps()
    lp_cc_library(
        name = name,
        srcs = srcs,
        deps = deps,
        alwayslink = 1,
        **kwargs
    )

def _normalize_proto(x):
    if x.endswith("_proto"):
        x = x.rstrip("_proto")
    if x.endswith("_cc"):
        x = x.rstrip("_cc")
    if x.endswith("_pb2"):
        x = x.rstrip("_pb2")
    return x

def _strip_proto_suffix(x):
    # Workaround for bug that str.rstrip(".END") takes off more than just ".END"
    if x.endswith(".proto"):
        x = x[:-6]
    return x

def lp_cc_proto_library(name, srcs = [], deps = [], **kwargs):
    """Build a proto cc_library.

    This rule does three things:

    1) Create a filegroup with name `name` that contains `srcs`
       and any sources from deps named "x_proto" or "x_cc_proto".

    2) Uses protoc to compile srcs to .h/.cc files, allowing any
       tensorflow imports.

    3) Creates a cc_library with name `name` building the resulting .h/.cc
       files.

    Args:
      name: The name, should end with "_cc_proto".
      srcs: The .proto files.
      deps: Any lp_cc_proto_library targets.
      **kwargs: Any additional args for the cc_library rule.
    """
    gen_srcs = [_strip_proto_suffix(x) + ".pb.cc" for x in srcs]
    gen_hdrs = [_strip_proto_suffix(x) + ".pb.h" for x in srcs]
    src_paths = ["$(location {})".format(x) for x in srcs]
    dep_srcs = []
    for x in deps:
        if x.endswith("_proto"):
            dep_srcs.append(_normalize_proto(x))
    native.filegroup(
        name = _normalize_proto(name),
        srcs = srcs + dep_srcs,
        **kwargs
    )
    native.genrule(
        name = name + "_gen",
        srcs = srcs,
        outs = gen_srcs + gen_hdrs,
        tools = dep_srcs + [
            "@protobuf_protoc//:protoc_bin",
            "@tensorflow_includes//:protos",
        ],
        cmd = """
        OUTDIR=$$(echo $(RULEDIR) | sed -e 's#courier.*##')
        $(location @protobuf_protoc//:protoc_bin) \
          --proto_path=external/tensorflow_includes/tensorflow_includes/ \
          --proto_path=. \
          --cpp_out=$$OUTDIR {}""".format(
            " ".join(src_paths),
        ),
    )

    native.cc_library(
        name = "{}_static".format(name),
        srcs = gen_srcs,
        hdrs = gen_hdrs,
        deps = depset(deps + lp_tf_deps()),
        alwayslink = 1,
        **kwargs
    )
    native.cc_binary(
        name = "lib{}.so".format(name),
        deps = ["{}_static".format(name)],
        linkshared = 1,
        **kwargs
    )
    native.cc_library(
        name = name,
        hdrs = gen_hdrs,
        srcs = ["lib{}.so".format(name)],
        deps = depset(deps + lp_tf_deps()),
        alwayslink = 1,
        **kwargs
    )

def lp_py_proto_library(name, srcs = [], deps = [], **kwargs):
    """Build a proto py_library.

    This rule does three things:

    1) Create a filegroup with name `name` that contains `srcs`
       and any sources from deps named "x_proto" or "x_py_proto".

    2) Uses protoc to compile srcs to _pb2.py files, allowing any
       tensorflow imports.

    3) Creates a py_library with name `name` building the resulting .py
       files.

    Args:
      name: The name, should end with "_cc_proto".
      srcs: The .proto files.
      deps: Any lp_cc_proto_library targets.
      **kwargs: Any additional args for the cc_library rule.
    """
    gen_srcs = [_strip_proto_suffix(x) + "_pb2.py" for x in srcs]
    src_paths = ["$(location {})".format(x) for x in srcs]
    proto_deps = []
    py_deps = []
    for x in deps:
        if x.endswith("_proto"):
            proto_deps.append(_normalize_proto(x))
        else:
            py_deps.append(x)
    native.filegroup(
        name = _normalize_proto(name),
        srcs = srcs + proto_deps,
        **kwargs
    )
    native.genrule(
        name = name + "_gen",
        srcs = srcs,
        outs = gen_srcs,
        tools = proto_deps + [
            "@protobuf_protoc//:protoc_bin",
            "@tensorflow_includes//:protos",
        ],
        cmd = """
        OUTDIR=$$(echo $(RULEDIR) | sed -e 's#courier.*##')
        $(location @protobuf_protoc//:protoc_bin) \
          --proto_path=external/tensorflow_includes/tensorflow_includes/ \
          --proto_path=. \
          --python_out=$$OUTDIR {}""".format(
            " ".join(src_paths),
        ),
    )
    native.py_library(
        name = name,
        srcs = gen_srcs,
        deps = py_deps,
        data = proto_deps,
        **kwargs
    )

def lp_cc_grpc_library(
        name,
        srcs = [],
        deps = [],
        generate_mocks = False,
        **kwargs):
    """Build a grpc cc_library.

    This rule does two things:

    1) Uses protoc + grpc plugin to compile srcs to .h/.cc files, allowing any
       tensorflow imports.  Also creates mock headers if requested.

    2) Creates a cc_library with name `name` building the resulting .h/.cc
       files.

    Args:
      name: The name, should end with "_cc_grpc_proto".
      srcs: The .proto files.
      deps: lp_cc_proto_library targets.  Must include src + "_cc_proto",
        the cc_proto library, for each src in srcs.
      generate_mocks: If true, creates mock headers for each source.
      **kwargs: Any additional args for the cc_library rule.
    """
    gen_srcs = [x.rstrip(".proto") + ".grpc.pb.cc" for x in srcs]
    gen_hdrs = [x.rstrip(".proto") + ".grpc.pb.h" for x in srcs]
    proto_src_deps = []
    for x in deps:
        if x.endswith("_proto"):
            proto_src_deps.append(_normalize_proto(x))
    src_paths = ["$(location {})".format(x) for x in srcs]

    if generate_mocks:
        gen_mocks = [x.rstrip(".proto") + "_mock.grpc.pb.h" for x in srcs]
    else:
        gen_mocks = []

    native.genrule(
        name = name + "_gen",
        srcs = srcs,
        outs = gen_srcs + gen_hdrs + gen_mocks,
        tools = proto_src_deps + [
            "@protobuf_protoc//:protoc_bin",
            "@tensorflow_includes//:protos",
            "@com_github_grpc_grpc//src/compiler:grpc_cpp_plugin",
        ],
        cmd = """
        OUTDIR=$$(echo $(RULEDIR) | sed -e 's#courier.*##')
        $(location @protobuf_protoc//:protoc_bin) \
          --plugin=protoc-gen-grpc=$(location @com_github_grpc_grpc//src/compiler:grpc_cpp_plugin) \
          --proto_path=external/tensorflow_includes/tensorflow_includes/ \
          --proto_path=. \
          --grpc_out={} {}""".format(
            "generate_mock_code=true:$$OUTDIR" if generate_mocks else "$$OUTDIR",
            " ".join(src_paths),
        ),
    )

    native.cc_library(
        name = name,
        srcs = gen_srcs,
        hdrs = gen_hdrs + gen_mocks,
        deps = depset(deps + ["@com_github_grpc_grpc//:grpc++_codegen_proto"]),
        **kwargs
    )

def lp_cc_test(name, srcs, deps = [], **kwargs):
    """launchpad-specific version of cc_test.

    Args:
      name: Target name.
      srcs: Target sources.
      deps: Target deps.
      **kwargs: Additional args to cc_test.
    """
    new_deps = [
        "@com_github_grpc_grpc//:grpc++_test",
        "@com_google_googletest//:gtest",
        "@tensorflow_includes//:includes",
        "@tensorflow_solib//:framework_lib",
        "@com_google_googletest//:gtest_main",
    ]
    size = kwargs.pop("size", "small")
    native.cc_test(
        name = name,
        size = size,
        copts = lp_copts(),
        srcs = srcs,
        deps = depset(deps + new_deps),
        **kwargs
    )

def lp_gen_op_wrapper_py(name, out, kernel_lib, linkopts = [], **kwargs):
    """Generates the py_library `name` with a data dep on the ops in kernel_lib.

    The resulting py_library creates file `$out`, and has a dependency on a
    symbolic library called lib{$name}_gen_op.so, which contains the kernels
    and ops and can be loaded via `tf.load_op_library`.

    Args:
      name: The name of the py_library.
      out: The name of the python file.  Use "gen_{name}_ops.py".
      kernel_lib: A cc_kernel_library target to generate for.
      **kwargs: Any args to the `cc_binary` and `py_library` internal rules.
    """
    if not out.endswith(".py"):
        fail("Argument out must end with '.py', but saw: {}".format(out))

    module_name = "lib{}_gen_op".format(name)
    version_script_file = "%s-version-script.lds" % module_name
    native.genrule(
        name = module_name + "_version_script",
        outs = [version_script_file],
        cmd = "echo '{global:\n *tensorflow*;\n *deepmind*;\n local: *;};' >$@",
        output_licenses = ["unencumbered"],
        visibility = ["//visibility:private"],
    )
    native.cc_binary(
        name = "{}.so".format(module_name),
        deps = [kernel_lib] + lp_tf_deps() + [version_script_file],
        copts = lp_copts() + [
            "-fno-strict-aliasing",  # allow a wider range of code [aliasing] to compile.
            "-fvisibility=hidden",  # avoid symbol clashes between DSOs.
        ],
        linkshared = 1,
        linkopts = linkopts + _rpath_linkopts(module_name) + [
            "-Wl,--version-script",
            "$(location %s)" % version_script_file,
        ],
        **kwargs
    )
    native.genrule(
        name = "{}_genrule".format(out),
        outs = [out],
        cmd = """
        echo 'import tensorflow as tf
_lp_gen_op = tf.load_op_library(
    tf.compat.v1.resource_loader.get_path_to_datafile(
       "lib{}_gen_op.so"))
_locals = locals()
for k in dir(_lp_gen_op):
  _locals[k] = getattr(_lp_gen_op, k)
del _locals' > $@""".format(name),
    )
    native.py_library(
        name = name,
        srcs = [out],
        data = [":lib{}_gen_op.so".format(name)],
        **kwargs
    )

def lp_pytype_library(**kwargs):
    if "strict_deps" in kwargs:
        kwargs.pop("strict_deps")
    native.py_library(**kwargs)

lp_pytype_strict_library = native.py_library

def _make_search_paths(prefix, levels_to_root):
    return ",".join(
        [
            "-rpath,%s/%s" % (prefix, "/".join([".."] * search_level))
            for search_level in range(levels_to_root + 1)
        ],
    )

def _rpath_linkopts(name):
    # Search parent directories up to the TensorFlow root directory for shared
    # object dependencies, even if this op shared object is deeply nested
    # (e.g. tensorflow/contrib/package:python/ops/_op_lib.so). tensorflow/ is then
    # the root and tensorflow/libtensorflow_framework.so should exist when
    # deployed. Other shared object dependencies (e.g. shared between contrib/
    # ops) are picked up as long as they are in either the same or a parent
    # directory in the tensorflow/ tree.
    levels_to_root = native.package_name().count("/") + name.count("/")
    return ["-Wl,%s" % (_make_search_paths("$$ORIGIN", levels_to_root),)]

def _py_extension_bundle_impl(ctx):
    # Create CcInfo from the dependencies.
    cc_infos = []
    for dep in ctx.attr.deps:
        if CcInfo in dep:
            cc_infos.append(dep[CcInfo])
        elif PyCcLinkParamsProvider in dep:
            cc_infos.append(dep[PyCcLinkParamsProvider].cc_info)

    # 'direct_cc_infos' will export header files of the direct dependencies.
    cc_info = cc_common.merge_cc_infos(direct_cc_infos = cc_infos)

    # Create PyInfo from the dependencies.
    py_infos = [dep[PyInfo] for dep in ctx.attr.deps if PyInfo in dep]
    py_info = PyInfo(
        transitive_sources = depset(transitive = [info.transitive_sources for info in py_infos]),
        uses_shared_libraries = any([info.uses_shared_libraries for info in py_infos]),
        imports = depset(transitive = [info.imports for info in py_infos]),
        has_py2_only_sources = any([info.has_py2_only_sources for info in py_infos]),
        has_py3_only_sources = any([info.has_py3_only_sources for info in py_infos]),
    )

    default_info = ctx.runfiles()
    for dep in ctx.attr.deps:
        default_info = DefaultInfo(runfiles = default_info.merge(dep[DefaultInfo].default_runfiles))

    return [default_info, cc_info, py_info]

_py_extension_bundle = rule(
    implementation = _py_extension_bundle_impl,
    provides = [CcInfo, PyInfo],
    attrs = {
        "deps": attr.label_list(providers = [[CcInfo], [PyInfo]]),
    },
)

def lp_py_extension_bundle(name, deps = [], **kwargs):
    """A cc_library and py_library in a single target.

    It is only meant to be used by another `py_library` or `py_extension`,
    *not* a `cc_library`.

    This rule is basically to work around the problem
    where `py_extension` doesn't allow `py_library` in its deps. This occurs
    when a `py_extension` target wants to depend on a `pybind_extension`
    target (whose public target is a `py_library`).

    All dependencies will be added to the final result.

    Args:
      name: the name of the target
      deps: Other Python or CC dependencies.
      **kwargs: common attributes for the target.
    """

    # Wrap the rule in a macro so that, should macro-level features be
    # needed in the future, they can be easily used without worrying
    # about obscure, Hyrum's law dependencies on the symbol being a rule.
    _py_extension_bundle(name = name, deps = deps, **kwargs)

def lp_pybind_extension(
        name,
        srcs,
        hdrs = [],
        features = [],
        srcs_version = "PY3",
        data = [],
        copts = [],
        linkopts = [],
        deps = [],
        defines = [],
        visibility = None,
        testonly = None,
        licenses = None,
        compatible_with = None,
        restricted_to = None,
        deprecation = None):
    """Builds a generic Python extension module.

    The module can be loaded in python by performing "import ${name}.".

    Args:
      name: Name.
      srcs: cc files.
      hdrs: h files.
      features: see bazel docs.
      srcs_version: srcs_version for py_library.
      data: data deps.
      copts: compilation opts.
      linkopts: linking opts.
      deps: cc_library deps.
      defines: cc_library defines.
      visibility: visibility.
      testonly: whether the rule is testonly.
      licenses: see bazel docs.
      compatible_with: see bazel docs.
      restricted_to: see bazel docs.
      deprecation:  see bazel docs.
    """
    py_file = "%s.py" % name
    so_file = "%s.so" % name
    pyd_file = "%s.pyd" % name
    symbol = "init%s" % name
    symbol2 = "init_%s" % name
    symbol3 = "PyInit_%s" % name
    exported_symbols_file = "%s-exported-symbols.lds" % name
    version_script_file = "%s-version-script.lds" % name
    native.genrule(
        name = name + "_exported_symbols",
        outs = [exported_symbols_file],
        cmd = "echo '_%s\n_%s\n_%s' >$@" % (symbol, symbol2, symbol3),
        output_licenses = ["unencumbered"],
        visibility = ["//visibility:private"],
        testonly = testonly,
    )
    native.genrule(
        name = name + "_version_script",
        outs = [version_script_file],
        cmd = "echo '{global:\n %s;\n %s;\n %s;\n local: *;};' >$@" % (symbol, symbol2, symbol3),
        output_licenses = ["unencumbered"],
        visibility = ["//visibility:private"],
        testonly = testonly,
    )
    native.cc_binary(
        name = so_file,
        srcs = srcs + hdrs,
        data = data,
        copts = copts + [
            "-fno-strict-aliasing",  # allow a wider range of code [aliasing] to compile.
            "-fexceptions",  # pybind relies on exceptions, required to compile.
            "-fvisibility=hidden",  # avoid pybind symbol clashes between DSOs.
        ],
        linkopts = linkopts + _rpath_linkopts(name) + [
            "-Wl,--version-script",
            "$(location %s)" % version_script_file,
        ],
        deps = depset(deps + [
            exported_symbols_file,
            version_script_file,
        ]),
        defines = defines,
        features = features + ["-use_header_modules"],
        linkshared = 1,
        testonly = testonly,
        licenses = licenses,
        visibility = visibility,
        deprecation = deprecation,
        restricted_to = restricted_to,
        compatible_with = compatible_with,
    )
    native.genrule(
        name = name + "_pyd_copy",
        srcs = [so_file],
        outs = [pyd_file],
        cmd = "cp $< $@",
        output_to_bindir = True,
        visibility = visibility,
        deprecation = deprecation,
        restricted_to = restricted_to,
        compatible_with = compatible_with,
        testonly = testonly,
    )
    native.genrule(
        name = name + "_py_file",
        outs = [py_file],
        cmd = (
            "echo 'import tensorflow as _tf; from .%s import *; del _tf' >$@" %
            name
        ),
        output_licenses = ["unencumbered"],
        visibility = visibility,
        testonly = testonly,
    )
    native.py_library(
        name = name,
        data = [so_file],
        srcs = [py_file],
        srcs_version = srcs_version,
        licenses = licenses,
        testonly = testonly,
        visibility = visibility,
        deprecation = deprecation,
        restricted_to = restricted_to,
        compatible_with = compatible_with,
    )

def lp_pybind_library(
        name,
        copts = [],
        features = [],
        tags = [],
        deps = [],
        py_deps = [],
        visibility = None,
        compatible_with = None,
        **kwargs):
    """A cc_library compatible with pybind11, to be reused by other pybind targets.

    The library can also have 'py_deps', useful when the C++ code imports
    Python modules.

    Args:
      name: Generated py_library/py_extension module name.
      copts: Options for native cc_library.
      features: Build features for all libraries.
      tags: Build tags for all libraries.
      py_deps: Python deps (py_libraries, py_extensions, pybind_extensions).
      deps: cc only deps (pybind11_libraries and/or cc_libraries).
      visibility: passed to inner targets.
      compatible_with: passed to inner targets.
      **kwargs: Additional arguments to pass to the cc_library.
    """
    lib_rule = name + "_pybind"
    native.cc_library(
        name = lib_rule,
        copts = copts + ["-fexceptions"],
        features = features + ["-use_header_modules"],
        visibility = visibility,
        compatible_with = compatible_with,
        deps = deps + lp_pybind_deps(),
        **kwargs
    )
    py_deps = py_deps + [lib_rule]
    lp_py_extension_bundle(
        name = name,
        tags = tags,
        features = features,
        deps = py_deps,
        visibility = visibility,
        compatible_with = compatible_with,
    )

def lp_py_standard_imports():
    return []

def lp_py_test(
        name,
        srcs = [],
        deps = [],
        paropts = [],
        python_version = "PY3",
        **kwargs):
    size = kwargs.pop("size", "small")
    native.py_test(
        name = name,
        size = size,
        srcs = srcs,
        deps = deps,
        python_version = python_version,
        **kwargs
    )
    return

def lp_pybind_deps():
    return [
        "@pybind11",
    ]

def lp_tf_ops_visibility():
    return [
        "//launchpad:__subpackages__",
    ]

def lp_tf_deps():
    return [
        "@tensorflow_includes//:includes",
        "@tensorflow_solib//:framework_lib",
    ] + lp_absl_deps()

def lp_grpc_deps():
    return ["@com_github_grpc_grpc//:grpc++"]

def lp_absl_deps():
    return [
        # We purposefully don't include absl::flat_hash_{map,set} so that users
        # are forced to use platform:hash_{map,set}, which uses a safer hasher.
        "@com_google_absl//absl/base",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/functional:bind_front",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/numeric:int128",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/random:distributions",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:span",
        "@com_google_absl//absl/flags:flag",
    ]

lp_script = native.py_binary

lp_library = native.py_library

lp_test = lp_py_test

lp_py_grpc_library = py_grpc_library
