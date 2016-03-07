// FSLL :: Filesystem Linked List
//
// Copyright (c) 2016 by William R. Fraser
//

use std::ffi::{OsStr, OsString};
use std::os::unix::fs::symlink;
use std::path::Path;

pub struct FSLL {
    base_dir: OsString,
    head_link: OsString,
    tail_link: OsString,
}

impl FSLL {
    pub fn new<T: AsRef<OsStr> + ?Sized>(
        base_dir: &T,
        head_link: &T,
        tail_link: &T
    ) -> FSLL {
        FSLL {
            base_dir: OsString::from(base_dir),
            head_link: OsString::from(head_link),
            tail_link: OsString::from(tail_link),
        }
    }

    pub fn to_head<T: AsRef<Path> + ?Sized>(&self, path: &T) {
        let _p: &Path = path.as_ref();
        // TODO
    }
}
