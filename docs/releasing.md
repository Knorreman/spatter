# Release readiness (no publication performed)

Current workspace versions: `spatter-core` and `spatter` 0.1.0. Registry
availability is not implied by those version numbers or by a GitHub merge.

## Validation performed on this branch

- README library example built and ran in an external path-dependent Cargo project.
- Local and three-rank wordcount quickstarts returned `keys=4 sum=6`.
- Workspace doctests passed, including the public library example.
- `RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-deps --locked` passed.
- `cargo package -p spatter-core --locked --allow-dirty` packaged and verified
  successfully; archive file lists for both crates were inspected.
- `cargo package -p spatter --locked --allow-dirty` stopped at dependency
  resolution: no matching `spatter-core` package in the crates.io index.
  Full registry-based spatter package verification remains blocked.

`--allow-dirty` was used only to check the uncommitted documentation changes.
Release packaging should use a clean reviewed commit. Deployment commands are
documented but were not rerun against Kubernetes or Podman for this docs branch.

Before publishing:

1. Review CI for stable/MSRV, docs, cluster smoke, forced spill and kind jobs.
2. Confirm ownership/availability of both crates.io names and review the
   Apache-2.0 distribution/license material.
3. Run package validation from the reviewed release commit:

   ```sh
   cargo package -p spatter-core --locked
   cargo package -p spatter --locked
   ```

   Packaging spatter resolves its versioned dependency on spatter-core against
   the registry. The core crate must be published/indexed first for the normal
   package verification path. A local path build alone does not verify this.
4. Review archive file lists (`cargo package -p CRATE --list`) and extracted
   manifests: repository, README, version, license, dependencies and MSRV.
5. After explicit release approval, publish core first, wait for registry
   availability, then verify/package and publish spatter. No publish command
   should be run as part of ordinary documentation development.
6. Test an external project against the registry versions, then tag the
   reviewed commit and publish release notes documenting the experimental
   cluster/failure/memory limits.

For pre-release development, the README's path dependency works in an external
project. Public API doc examples are exercised by `cargo test --doc`.

Outstanding validation: a crates.io external-project test after publication,
Spark baseline, genuine multi-host benchmark, and broad distributed
chained-shuffle correctness. These are not covered by the current localhost
wordcount benchmark or the single-pod Podman demonstration.
