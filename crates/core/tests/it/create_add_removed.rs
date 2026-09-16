use std::fs;
use std::path::Path;
use std::process::Command;

fn write_file(path: &Path, contents: &str) {
    fs::write(path, contents).unwrap();
}

fn toml_basic_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

#[test]
fn create_add_is_not_available_to_downstream_crates() {
    let temp_dir = tempfile::tempdir().unwrap();
    let crate_dir = temp_dir.path();
    let src_dir = crate_dir.join("src");

    fs::create_dir(&src_dir).unwrap();

    write_file(
        &crate_dir.join("Cargo.toml"),
        &format!(
            r#"[package]
name = "create-add-public-api-check"
version = "0.1.0"
edition = "2021"

[dependencies]
deltalake-core = {{ path = "{}" }}
"#,
            toml_basic_string(env!("CARGO_MANIFEST_DIR"))
        ),
    );
    write_file(
        &src_dir.join("main.rs"),
        r#"use deltalake_core::writer::create_add;

fn main() {}
"#,
    );

    let workspace_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    fs::copy(
        workspace_dir.join("Cargo.lock"),
        crate_dir.join("Cargo.lock"),
    )
    .unwrap();
    // Resolve the temporary package with the workspace's pins and Cargo configuration.
    let cargo = || {
        let mut command = Command::new("cargo");
        command
            .current_dir(workspace_dir)
            .env("CARGO_TARGET_DIR", crate_dir.join("target"))
            .env("CARGO_BUILD_JOBS", "2");
        command
    };
    let resolution = cargo()
        .args([
            "metadata",
            "--offline",
            "--format-version",
            "1",
            "--manifest-path",
        ])
        .arg(crate_dir.join("Cargo.toml"))
        .output()
        .unwrap();
    assert!(
        resolution.status.success(),
        "downstream dependency resolution failed:\n{}",
        String::from_utf8_lossy(&resolution.stderr),
    );
    let output = cargo()
        .args(["check", "--locked", "--offline", "--manifest-path"])
        .arg(crate_dir.join("Cargo.toml"))
        .output()
        .unwrap();

    assert!(
        !output.status.success(),
        "expected downstream import of writer::create_add to fail, but it succeeded\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("no `create_add` in `writer`")
            || stderr.contains("no create_add in writer")
            || stderr.contains("unresolved import `deltalake_core::writer::create_add`"),
        "expected missing public export error, got stderr:\n{}",
        stderr,
    );
}
