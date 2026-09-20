# Versioning policy

MegaCache follows Semantic Versioning 2.0.0 for the server, wire extensions,
and all supported SDKs. Version 0.8.0 keeps the SDKs in lockstep with the
server.

Before 1.0, a minor release may make incompatible API changes only when the
release notes and migration guidance call them out. Patch releases preserve
documented APIs and protocol behavior. After 1.0, incompatible changes require
a major release.

The supported RESP2 subset is additive within a major version. Existing
commands, response shapes, error categories, and defaults are not removed or
reinterpreted in minor or patch releases. New optional fields may be added to
JSON responses; clients must ignore unknown fields.

SDK public methods, typed error categories, package coordinates, and minimum
runtime versions are compatibility commitments. Framework adapters are
examples unless explicitly marked stable. A release is supported only when
its conformance suite passes on a listed toolchain in CI.
