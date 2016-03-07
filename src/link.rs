// Symbolic Link Utilities
//
// Copyright (c) 2016 by William R. Fraser

use std::path::{Path, PathBuf};
use std::fmt::Debug;
use std::fs;
use std::io;
use std::os::unix;
use libc;

fn make_path_relative_to<T: AsRef<Path> + ?Sized + Debug,
                         U: AsRef<Path> + ?Sized + Debug>(
                             reference: &T,
                             path: &U
                         ) -> PathBuf {
    let p: &Path = path.as_ref();
    let mut path_adjusted = PathBuf::new();
    let mut reference_truncated: &Path = reference.as_ref();
    loop {
        match p.strip_prefix(reference_truncated) {
            Ok(stripped) => {
                // We found the common ancestor.
                for _ in stripped.components() {
                    path_adjusted.push("..");
                }
                path_adjusted.push(stripped);
                break;
            },
            Err(_) => {
                // No match yet; try to back up another level.
                match reference_truncated.parent() {
                    Some(ref t) => {
                        reference_truncated = t;
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

pub fn makelink<T: AsRef<Path> + ?Sized + Debug,
                U: AsRef<Path> + ?Sized + Debug,
                V: AsRef<Path> + ?Sized + Debug>(
                    path: &T,
                    link: &U,
                    target: Option<&V>
                ) -> io::Result<()> {
    let link_path: PathBuf = path.as_ref().to_path_buf().join(link);
    match target {
        Some(target_path) => {
            // target is relative to the base dir. Need to fix it up to be relative to link_path.
            let target_adjusted = make_path_relative_to(&link_path, target_path);
            unix::fs::symlink(&target_adjusted, &link_path)
        },
        None => {
            match fs::remove_file(link_path) {
                Ok(()) => Ok(()),
                Err(e) => {
                    if e.raw_os_error() == Some(libc::ENOENT) {
                        Ok(())
                    } else {
                        Err(e)
                    }
                }
            }
        }
    }
}
