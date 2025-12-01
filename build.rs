use std::env;
use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn output_or(cmd: &mut Command, s: &str) -> String {
    match cmd.output() {
        Ok(ref out) if out.status.success() => String::from_utf8_lossy(&out.stdout).into_owned(),
        _ => s.to_string(),
    }
}

fn main() {
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").unwrap());
    let mut git_rev_file = File::create(out_dir.join("git_rev.txt")).unwrap();
    let mut build_time_file = File::create(out_dir.join("build_time.txt")).unwrap();
    let mut fusemt_version_file = File::create(out_dir.join("fusemt_ver.txt")).unwrap();
    let mut fuser_version_file = File::create(out_dir.join("fuser_ver.txt")).unwrap();

    let git_rev = output_or(
        Command::new("git").arg("rev-parse").arg("HEAD"),
        "[no git rev]");

    let git_branch = output_or(
        Command::new("git").arg("name-rev").arg("HEAD"),
        "[unknown git branch]");

    write!(git_rev_file, "{} {}", git_rev.trim(),
            git_branch.trim()).unwrap();

    write!(build_time_file, "{}", SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs())
        .unwrap();

    let cargo = fs::read_to_string("Cargo.lock")
        .expect("failed to read Cargo.lock")
        .parse::<toml::Table>()
        .expect("failed to parse Cargo.lock as toml");

    for package in cargo.get("package")
        .expect("Cargo.lock: missing [[package]]")
        .as_array()
        .expect("Cargo.lock: [[package]] should be an array")
    {
        let package = package.as_table().expect("Cargo.lock: [[package]] element should be a table");
        let name = package.get("name").and_then(toml::Value::as_str);
        let ver = || package.get("version")
            .expect("Cargo.lock: missing package.version")
            .as_str()
            .expect("Cargo.lock: package.version should be string");
        if name == Some("fuse_mt") {
            write!(fusemt_version_file, "\"{}\"", ver()).unwrap();
        }
        if name == Some("fuser") {
            write!(fuser_version_file, "\"{}\"", ver()).unwrap();
        }
    }
}
