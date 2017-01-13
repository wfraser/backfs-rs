use std::env;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::process::Command;

fn output_or(cmd: &mut Command, s: &str) -> String {
    match cmd.output() {
        Ok(ref out) if out.status.success() => String::from_utf8_lossy(&out.stdout).into_owned(),
        _ => s.to_string(),
    }
}

fn main() {
    let out_dir = env::var("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("git_rev.txt");
    let mut f = File::create(&dest_path).unwrap();

    let git_rev = output_or(
        Command::new("git").arg("rev-parse").arg("HEAD"),
        "[no git rev]");

    let git_branch = output_or(
        Command::new("git").arg("name-rev").arg("HEAD"),
        "[unknown git branch]");

    write!(f, "{} {}", git_rev.trim(),
            git_branch.trim()).unwrap();
}
