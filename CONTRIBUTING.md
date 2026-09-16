# Contributing

Thank you for improving MegaCache.

1. Create a focused branch and keep changes scoped to one problem.
2. Run `make test`.
3. Update API or operations documentation when behavior changes.
4. Open a pull request explaining the motivation, behavior, and compatibility
   impact.

Code must support Python 3.9+, avoid unnecessary runtime dependencies, validate
untrusted input, and preserve explicit error behavior. Add tests for fixes and
new functionality. RESP changes must remain binary-safe, include wire-level
tests, and update `docs/resp.md`.

Use GitHub's private vulnerability reporting instead of a public issue for
security problems.
