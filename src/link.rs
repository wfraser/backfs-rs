// Symbolic Link Utilities
//
// Copyright (c) 2016 by William R. Fraser

use std::path::{Component, Path, PathBuf};
use std::fs;
use std::io;
use std::os::unix;
use libc;

#[test]
fn test_relpaths() {
    assert_eq!(&make_path_relative_to("one/two/three", "one/foo/bar"), Path::new("../foo/bar"));
    assert_eq!(&make_path_relative_to("not/related", "at/all"), Path::new("../at/all"));
    assert_eq!(&make_path_relative_to("this", "other"), Path::new("other"));
    assert_eq!(&make_path_relative_to("foo/bar", "foo/hello/world"), Path::new("hello/world"));
    assert_eq!(&make_path_relative_to("one", "two/three"), Path::new("two/three"));
    assert_eq!(&make_path_relative_to("one/two/three", "one/two/other"), Path::new("other"));
    assert_eq!(&make_path_relative_to("one/two/three/four", "one/other"), Path::new("../../other"));
}

fn make_path_relative_to<T: AsRef<Path> + ?Sized,
                         U: AsRef<Path> + ?Sized>(
                             reference: &T,
                             path: &U
                         ) -> PathBuf {
    let p: &Path = path.as_ref();
    let mut path_adjusted = PathBuf::new();
    let mut reference_truncated: &Path = reference.as_ref();
    let mut first = true;
    loop {
        match p.strip_prefix(reference_truncated) {
            Ok(stripped) => {
                // We found the common ancestor.
                path_adjusted.push(stripped);
                break;
            },
            Err(_) => {
                // No match yet; try to back up another level.
                match reference_truncated.parent() {
                    Some(ref t) => {
                        reference_truncated = t;
                        if !first { // Just changing the filename doesn't require '..'
                            path_adjusted.push("..");
                        }
                        first = false;
                    },
                    None => {
                        // We backed up all the way.
                        path_adjusted.push(path);
                        break;
                    }
                }
            }
        }
    }
    path_adjusted
}

pub fn makelink<T: AsRef<Path> + ?Sized,
                U: AsRef<Path> + ?Sized,
                V: AsRef<Path> + ?Sized>(
                    path: &T,
                    link: &U,
                    target: Option<&V>
                ) -> io::Result<()> {
    let link_path: PathBuf = path.as_ref().to_path_buf().join(link);
    if let Err(e) = fs::remove_file(&link_path) {
        if e.raw_os_error() != Some(libc::ENOENT) {
            return Err(e);
        }
    }
    if let Some(target_path) = target {
        // target is relative to the base dir. Need to fix it up to be relative to link_path.
        let target_adjusted = make_path_relative_to(&link_path, target_path);
        unix::fs::symlink(&target_adjusted, &link_path)
    } else {
        Ok(())
    }
}

#[test]
fn test_resolve_path() {
    assert_eq!(resolve_path(PathBuf::from("one/two/three"), &PathBuf::from("../../four/five")), PathBuf::from("four/five"));
    assert_eq!(resolve_path(PathBuf::from("one"), &PathBuf::from("two/three")), PathBuf::from("two/three"));
}

fn resolve_path(mut reference: PathBuf, path: &PathBuf) -> PathBuf {
    reference.pop(); // remove the file name
    for c in path.components() {
        if c == Component::ParentDir {
            reference.pop();
        } else {
            reference.push(c.as_os_str())
        }
    }
    reference
}

pub fn getlink<T: AsRef<Path> + ?Sized,
               U: AsRef<Path> + ?Sized>(
                   path: &T,
                   link: &U
               ) -> io::Result<Option<PathBuf>> {
    let link_path: PathBuf = path.as_ref().to_path_buf().join(link);
    match fs::read_link(&link_path) {
        Ok(ref path) => {
            let x = resolve_path(link_path, path);
            Ok(Some(x))
        },
        Err(e) => {
            if e.raw_os_error() == Some(libc::ENOENT) {
                Ok(None)
            } else {
                Err(e)
            }
        }
    }
}
