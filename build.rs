extern crate time;

use std::env;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::process::Command;

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

    let git_rev = output_or(
        Command::new("git").arg("rev-parse").arg("HEAD"),
        "[no git rev]");

    let git_branch = output_or(
        Command::new("git").arg("name-rev").arg("HEAD"),
        "[unknown git branch]");

    write!(git_rev_file, "{} {}", git_rev.trim(),
            git_branch.trim()).unwrap();

    write!(build_time_file, "{}", time::now().to_utc().to_timespec().sec).unwrap();
}
