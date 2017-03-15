// FSLL :: Filesystem Linked List
//
// Copyright (c) 2016 by William R. Fraser
//

use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fmt::Debug;
use std::io;
use std::path::{Path, PathBuf};

use log;

use link;

macro_rules! log2 {
    ($lvl:expr, $($arg:tt)+) => (
        log!(target: "FSLL", $lvl, $($arg)+));
}

macro_rules! error {
    ($($arg:tt)+) => (log2!(log::LogLevel::Error, $($arg)+));
}

macro_rules! warn {
    ($($arg:tt)+) => (log2!(log::LogLevel::Warn, $($arg)+));
}

macro_rules! info {
    ($($arg:tt)+) => (log2!(log::LogLevel::Info, $($arg)+));
}

macro_rules! debug {
    ($($arg:tt)+) => (log2!(log::LogLevel::Debug, $($arg)+));
}

macro_rules! error_ret {
    ($($arg:tt)+) => ({
        let msg = fmt::format(format_args!($($arg)+));
        error!("{}", msg);
        return Err(io::Error::new(io::ErrorKind::Other, msg));
    });
}

pub struct FSLL {
    base_dir: OsString,
    head_link: OsString,
    tail_link: OsString,
}

pub trait PathLinkedList {
    fn is_empty(&self) -> bool;
    fn get_tail(&self) -> Option<PathBuf>;
    fn to_head<T: AsRef<Path> + ?Sized + Debug>(&self, path: &T) -> io::Result<()>;
    fn insert_as_head<T: AsRef<Path> + ?Sized + Debug>(&self, path: &T) -> io::Result<()>;
    fn insert_as_tail<T: AsRef<Path> + ?Sized + Debug>(&self, path: &T) -> io::Result<()>;
    fn disconnect<T: AsRef<Path> + ?Sized + Debug>(&self, path: &T) -> io::Result<()>;
}

impl FSLL {
    pub fn new<T: AsRef<OsStr> + ?Sized,
               U: AsRef<OsStr> + ?Sized,
               V: AsRef<OsStr> + ?Sized>(
        base_dir: &T,
        head_link: &U,
        tail_link: &V
    ) -> FSLL {
        FSLL {
            base_dir: OsString::from(base_dir),
            head_link: OsString::from(head_link),
            tail_link: OsString::from(tail_link),
        }
    }

    fn getlink<T: AsRef<Path> + ?Sized + Debug,
               U: AsRef<Path> + ?Sized + Debug>(
                   &self,
                   path: &T,
                   link: &U
               ) -> io::Result<Option<PathBuf>> {
        match link::getlink(path, link) {
            Ok(None) => Ok(None),
            Ok(Some(result)) => {
                // TODO: try to fix up absolute paths
                Ok(Some(result))
            },
            Err(e) => {
                error!("reading link {:?}/{:?}: {}", path, link, e);
                Err(e)
            }
        }
    }

    fn makelink<T: AsRef<Path> + ?Sized + Debug,
                U: AsRef<Path> + ?Sized + Debug,
                V: AsRef<Path> + ?Sized + Debug>(
                    &self,
                    path: &T,
                    link: &U,
                    target: Option<&V>
                ) -> io::Result<()> {
        debug!("makelink: {:?}: {:?} -> {:?}", path, link, target);
        match link::makelink(path, link, target) {
            Ok(()) => Ok(()),
            Err(e) => {
                if target.is_none() {
                    error!("error removing link {:?}/{:?}: {}", path, link, e);
                } else {
                    error!("error creating link {:?}/{:?}: {}", path, link, e);
                }
                Err(e)
            }
        }
    }

    fn get_head_tail(&self, method_name: &str) -> io::Result<(PathBuf, PathBuf)> {
        let head = match self.getlink(&self.base_dir, &self.head_link).unwrap() {
            Some(path) => path,
            None => { error_ret!("{}: head {:?} is unset", method_name, self.head_link); }
        };
        let tail = match self.getlink(&self.base_dir, &self.tail_link).unwrap() {
            Some(path) => path,
            None => { error_ret!("{}: tail {:?} is unset", method_name, self.tail_link); }
        };

        Ok((head, tail))
    }
}

impl PathLinkedList for FSLL {
    fn is_empty(&self) -> bool {
        self.getlink(&self.base_dir, &self.head_link).unwrap().is_none()
            && self.getlink(&self.base_dir, &self.tail_link).unwrap().is_none()
    }

    fn get_tail(&self) -> Option<PathBuf> {
        self.getlink(&self.base_dir, &self.tail_link).unwrap()
    }

    fn to_head<T: AsRef<Path> + ?Sized + Debug>(&self, path: &T) -> io::Result<()> {
        debug!("to_head: {:?}", path);
        let p: &Path = path.as_ref();

        // There must not be the situation where the list is empty (no head or tail yet set)
        // because this function is only for promoting an existing element to the head.
        // Use insert_as_head for the other case.
        let (head, tail) = try!(self.get_head_tail("to_head"));

        debug!("head {:?}", head);
        debug!("tail {:?}", tail);

        let next = try!(self.getlink(path, Path::new("next")));
        let prev = try!(self.getlink(path, Path::new("prev")));

        if prev.is_none() != (head == p) {
            if prev.is_some() {
                error_ret!("head entry has a prev: {:?}", path);
            } else {
                error_ret!("entry has no prev but is not head: {:?}", path);
            }
        }

        if next.is_none() != (tail == p) {
            if next.is_some() {
                error_ret!("tail entry has a next: {:?}", path);
            } else {
                error_ret!("entry has no next but is not tail: {:?}", path);
            }
        }

        if next.is_some() && (next.as_ref().unwrap() == p) {
            error_ret!("entry points to itself as next: {:?}", path);
        }
        if prev.is_some() && (prev.as_ref().unwrap() == p) {
            error_ret!("entry points to itself as prev: {:?}", path);
        }

        match prev.as_ref() {
            Some(p) => {
                try!(self.makelink(p, Path::new("next"), next.as_ref()));
            },
            None => {
                // already head; we're done!
                return Ok(());
            }
        }

        match next.as_ref() {
            Some(p) => {
                try!(self.makelink(p, Path::new("prev"), prev.as_ref()));
            },
            None => {
                try!(self.makelink(&self.base_dir, &self.tail_link, prev.as_ref()));
            }
        }

        // assuming head != None
        try!(self.makelink(&head, Path::new("prev"), Some(path)));
        try!(self.makelink(path, Path::new("next"), Some(&head)));
        try!(self.makelink(path, Path::new("prev"), None::<&Path>));
        try!(self.makelink(&self.base_dir, &self.head_link, Some(path)));

        Ok(())
    }

    fn insert_as_head<T: AsRef<Path> + ?Sized + Debug>(&self, path: &T) -> io::Result<()> {
        debug!("insert_as_head: {:?}", path);
        let maybe_head = try!(self.getlink(&self.base_dir, &self.head_link));
        let maybe_tail = try!(self.getlink(&self.base_dir, &self.tail_link));

        if maybe_head.is_some() && maybe_tail.is_some() {
            let head = maybe_head.as_ref().unwrap();
            try!(self.makelink(path, Path::new("next"), Some(head)));
            try!(self.makelink(head, Path::new("prev"), Some(path)));
            try!(self.makelink(&self.base_dir, &self.head_link, Some(path)));
        } else if maybe_head.is_none() && maybe_tail.is_none() {
            debug!("inserting {:?} as head and tail", path);
            try!(self.makelink(&self.base_dir, &self.head_link, Some(path)));
            try!(self.makelink(&self.base_dir, &self.tail_link, Some(path)));
            try!(self.makelink(path, Path::new("next"), None::<&Path>));
            try!(self.makelink(path, Path::new("prev"), None::<&Path>));
        } else if maybe_head.is_some() {
            error_ret!("list has a head {:?} but no tail!", maybe_head.unwrap());
        } else {
            error_ret!("list has a tail {:?} but no head!", maybe_tail.unwrap());
        }

        Ok(())
    }

    fn insert_as_tail<T: AsRef<Path> + ?Sized + Debug>(&self, path: &T) -> io::Result<()> {
        let maybe_head = try!(self.getlink(&self.base_dir, &self.head_link));
        let maybe_tail = try!(self.getlink(&self.base_dir, &self.tail_link));

        if maybe_head.is_some() && maybe_tail.is_some() {
            let tail = maybe_tail.as_ref().unwrap();
            try!(self.makelink(path, Path::new("prev"), Some(tail)));
            try!(self.makelink(tail, Path::new("next"), Some(path)));
            try!(self.makelink(&self.base_dir, &self.tail_link, Some(path)));
        } else if maybe_head.is_none() && maybe_tail.is_none() {
            try!(self.makelink(&self.base_dir, &self.head_link, Some(path)));
            try!(self.makelink(&self.base_dir, &self.tail_link, Some(path)));
            try!(self.makelink(path, Path::new("next"), None::<&Path>));
            try!(self.makelink(path, Path::new("prev"), None::<&Path>));
        } else if maybe_head.is_some() {
            error_ret!("list has a head {:?} but no tail!", maybe_head.unwrap());
        } else {
            error_ret!("list has a tail {:?} but no head!", maybe_tail.unwrap());
        }

        Ok(())
    }

    fn disconnect<T: AsRef<Path> + ?Sized + Debug>(&self, path: &T) -> io::Result<()> {
        let p: &Path = path.as_ref();

        let (head, tail) = try!(self.get_head_tail("disconnect"));
        let next = try!(self.getlink(path, Path::new("next")));
        let prev = try!(self.getlink(path, Path::new("prev")));

        if head == p {
            if next.is_none() {
                if tail == p {
                    try!(self.makelink(&self.base_dir, &self.tail_link, None::<&Path>));
                } else {
                    error_ret!("entry has no next but is not tail: {:?}", path);
                }
            } else {
                try!(self.makelink(&self.base_dir, &self.head_link, next.as_ref()));
                try!(self.makelink(next.as_ref().unwrap(), Path::new("prev"), None::<&Path>));
            }
        } else if prev.is_none() {
            error_ret!("entry has no prev but is not head: {:?}", path);
        }

        if tail == p {
            if prev.is_none() {
                if head == p {
                    try!(self.makelink(&self.base_dir, &self.head_link, None::<&Path>));
                } else {
                    error_ret!("entry has no prev but is not head: {:?}", path);
                }
            } else {
                try!(self.makelink(&self.base_dir, &self.tail_link, prev.as_ref()));
                try!(self.makelink(prev.as_ref().unwrap(), Path::new("next"), None::<&Path>));
            }
        } else if next.is_none() {
            error_ret!("entry has no next but is not tail: {:?}", path);
        }

        if next.is_some() && prev.is_some() {
            try!(self.makelink(next.as_ref().unwrap(), Path::new("prev"), prev.as_ref()));
            try!(self.makelink(prev.as_ref().unwrap(), Path::new("next"), next.as_ref()));
        }

        try!(self.makelink(path, Path::new("next"), None::<&Path>));
        try!(self.makelink(path, Path::new("prev"), None::<&Path>));

        Ok(())
    }
}
