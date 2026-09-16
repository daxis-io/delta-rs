# Axon native stack

`axon/native-stack` is the maintained integration branch in this Daxis fork. V2 topic PRs target it; the branch starts from the reviewed native/browser-capable source rather than the fork's older default branch.

`.cargo/config.toml` pins the Daxis Arrow/Parquet, object-store and DataFusion graph to immutable tested revisions. Cargo discovers this configuration in the repository and its nested workspaces. Consumers must apply the same fork overrides in their own Cargo root. Committed lockfiles keep `--locked` builds reproducible; do not substitute crates.io builds for fork-only APIs or features.

Use Rust/Cargo 1.97.0 for the exercised qualification commands, with two Cargo jobs, incremental disabled, and debug information disabled. The Kernel workspace also retains its declared MSRV checks. Run `cargo metadata --locked` in the repository root and every nested workspace before compiling.

Kernel CI covers formatting, feature-on/off compilation and clippy, owning tests, docs, examples and its DataFusion executor. delta-rs CI must include `axon/native-stack` in its branch filters so the V2 PR receives build/test checks rather than metadata-only checks.

V2 Kernel and delta-rs changes are separate topic PRs. The delta-rs V2 topic must pin a Kernel revision containing the companion snapshot guard and additive protocol accessor. Update both the revision and lockfile explicitly, then rerun CI.

Retained native checkpoint evidence proves only exercised correctness paths. Shipping integration, browser, performance, release and deployment require their own evidence.
