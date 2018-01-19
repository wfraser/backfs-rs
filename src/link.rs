// Symbolic Link Utilities
//
// Copyright 2016-2018 by William R. Fraser

use std::path::{Component, Path, PathBuf};
use std::fs;
use std::io;
use std::os::unix;
use libc;

#[test]
fn test_relpaths() {
    macro_rules! test {
        ($a:expr, $b:expr => $c:expr) => (assert_eq!(&make_path_relative_to($a, $b), Path::new($c)));
    }

    test!("one/two/three", "one/foo/bar" => "../foo/bar");
    test!("not/related", "at/all" => "../at/all");
    test!("this", "other" => "other");
    test!("foo/bar", "foo/hello/world" => "hello/world");
    test!("one", "two/three" => "two/three");
    test!("one/two/three", "one/two/other" => "other");
    test!("one/two/three/four", "one/other" => "../../other");
}

#[test]
fn test_absolutepaths() {
    macro_rules! test {
        ($a:expr, $b:expr => $c:expr) => (assert_eq!(&make_path_relative_to($a, $b), Path::new($c));)
    }

    test!("one/two", "/absolute/path" => "/absolute/path");
    test!("/absolute/one", "/absolute/two/three" => "two/three");
}

#[test]
#[should_panic]
fn test_invalid_relative_to_absolute() {
    make_path_relative_to("/absolute/path", "relative/path");
}

/// Takes two relative paths, which are assumed to both be relative to some unspecified common base
/// path, and returns the second one, altered so as to be relative to the first.
fn make_path_relative_to<T: AsRef<Path> + ?Sized,
                         U: AsRef<Path> + ?Sized>(
                             reference: &T,
                             path: &U
                         ) -> PathBuf {
    let r: &Path = reference.as_ref();
    let p: &Path = path.as_ref();

    if r.is_absolute() && !p.is_absolute() {
        // There's nothing sensible we can do here, because we have no idea what `path` is
        // originally relative to.
        panic!("invalid arguments to link::make_path_relative_to");
    }

    if p.is_absolute() && !r.is_absolute() {
        return p.to_path_buf();
    }

    let mut path_adjusted = PathBuf::new();
    let mut reference_truncated: &Path = r;
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
                    Some(t) => {
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
    macro_rules! test {
        ($a:expr, $b:expr => $c:expr) => (assert_eq!(resolve_path(PathBuf::from($a),
                                                                  &PathBuf::from($b)),
                                                     PathBuf::from($c)));
    }
    test!("one/two/three", "../../four/five" => "four/five");
    test!("one", "two/three" => "two/three");

    // The second argument is supposed to be relative to the first, but we can resolve to something
    // sensible anyway.
    test!("one/two", "/absolute/path" => "/absolute/path");

    // The first argument is supposed to be a relative path, but we can resolve to something
    // sensible anyway.
    test!("/absolute/path", "one/two" => "/absolute/one/two");

    // Other cases with bad inputs:
    test!("/one/absolute", "/two/absolute" => "/two/absolute");
    test!("/one/absolute", "/one/more/absolute" => "/one/more/absolute");
}

/// Given a reference path relative to some unspecified base path, and a path assumed to be
/// relative to the reference, returns the second, altered so as to be relative to the base path.
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
