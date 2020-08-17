// OsStr and OsString extra functions
//
// Copyright 2016-2020 by William R. Fraser
//

use std::ffi::{OsStr, OsString};
use std::os::unix::ffi::OsStrExt;

pub struct Split<'a> {
    string: &'a [u8],
    sep: u8,
    position: usize,
}

impl<'a> Iterator for Split<'a> {
    type Item = &'a OsStr;
    fn next(&mut self) -> Option<Self::Item> {
        if self.position == self.string.len() {
            return None;
        }

        let old_position = self.position;

        for i in old_position .. self.string.len() {
            if self.string[i] == self.sep {
                self.position = i + 1;
                return Some(OsStr::from_bytes(&self.string[old_position .. i]));
            }
        }

        self.position = self.string.len();
        Some(OsStr::from_bytes(&self.string[old_position ..]))
    }
}

pub struct SplitN<'a> {
    split: Split<'a>,
    count: usize,
    max: usize,
}

impl<'a> Iterator for SplitN<'a> {
    type Item = &'a OsStr;
    fn next(&mut self) -> Option<Self::Item> {
        if self.count == self.max || self.split.position == self.split.string.len() {
            None
        } else if self.count == self.max - 1 {
            self.count += 1;
            Some(OsStr::from_bytes(&self.split.string[ self.split.position .. ]))
        } else {
            match self.split.next() {
                Some(s) => {
                    self.count += 1;
                    Some(s)
                },
                None => None
            }
        }
    }
}

pub trait AsBytes {
    fn as_bytes_ext(&self) -> &[u8];
}

impl<'a> AsBytes for &'a str {
    fn as_bytes_ext(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl AsBytes for OsStr {
    fn as_bytes_ext(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl AsBytes for &OsString {
    fn as_bytes_ext(&self) -> &[u8] {
        self.as_bytes()
    }
}

pub trait OsStrExtras {
    fn is_empty(&self) -> bool;
    fn starts_with(&self, s: impl AsBytes) -> bool;
    fn split(&self, pat: u8) -> Split<'_>;
    fn splitn(&self, count: usize, pat: u8) -> SplitN<'_>;
}

impl OsStrExtras for OsStr {
    fn is_empty(&self) -> bool {
        self == OsStr::new("")
    }

    fn starts_with(&self, s: impl AsBytes) -> bool {
        self.as_bytes().starts_with(s.as_bytes_ext())
    }

    fn split(&self, pat: u8) -> Split<'_> {
        Split {
            string: self.as_bytes(),
            sep: pat,
            position: 0,
        }
    }

    fn splitn(&self, count: usize, pat: u8) -> SplitN<'_> {
        SplitN {
            split: Split {
                string: self.as_bytes(),
                sep: pat,
                position: 0,
            },
            count: 0,
            max: count,
        }
    }
}
