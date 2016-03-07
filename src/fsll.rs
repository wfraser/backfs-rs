// FSLL :: Filesystem Linked List
//
// Copyright (c) 2016 by William R. Fraser
//

use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fmt::Debug;
use std::fs;
use std::io;
use std::os::unix;
use std::path::{Path, PathBuf};

use libc::*;

macro_rules! log {
    ($s:expr, $fmt:expr) => ($s.log(format_args!($fmt)));
    ($s:expr, $fmt:expr, $($arg:tt)*) => ($s.log(format_args!($fmt, $($arg)*)));
}

macro_rules! error {
    ($s:expr, $fmt:expr) => {
        {
            let msg = fmt::format(format_args!($fmt));
            $s.log(format_args!("error: {}", msg));
            return Err(io::Error::new(io::ErrorKind::Other, msg));
        }
    };
    ($s:expr, $fmt:expr, $($arg:tt)*) => {
        {
            let msg = fmt::format(format_args!($fmt, $($arg)*));
            $s.log(format_args!("{}", msg));
            return Err(io::Error::new(io::ErrorKind::Other, msg));
        }
    };
}

pub struct FSLL {
    base_dir: OsString,
    head_link: OsString,
    tail_link: OsString,
    pub debug: bool,
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
            debug: false,
        }
    }

    fn log(&self, args: fmt::Arguments) {
        if self.debug {
            println!("FSLL: {}", fmt::format(args));
        }
    }

    fn getlink<T: AsRef<Path> + ?Sized,
               U: AsRef<Path> + ?Sized>(
                   &self,
                   path: &T,
                   link: &U
               ) -> Option<PathBuf> {
        let mut link_path: PathBuf = path.as_ref().to_path_buf();
        link_path.push(link);

        match fs::read_link(&link_path) {
            Ok(path) => {
                link_path.pop();
                link_path.push(path);
                Some(link_path)
            },
            Err(e) => {
                if e.raw_os_error() != Some(ENOENT) {
                    log!(self, "warning: error reading link {:?}: {}", link_path, e);
                }
                None
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
        let mut link_path: PathBuf = path.as_ref().to_path_buf();
        link_path.push(link);

        match target {
            Some(target_path) => {
                unix::fs::symlink(target_path, link_path)
            },
            None => {
                let result = fs::remove_file(link_path);
                if let Err(ref e) = result {
                    if e.raw_os_error() != Some(ENOENT) {
                        error!(self, "error removing link {:?}: {}", link, e);
                    }
                }

                result
            }
        }
    }

    fn get_head_tail(&self, method_name: &str) -> io::Result<(PathBuf, PathBuf)> {
        let head = match self.getlink(&self.base_dir, &self.head_link) {
            Some(path) => path,
            None => { error!(self, "{}: head {:?} is unset", method_name, self.head_link); }
        };
        let tail = match self.getlink(&self.base_dir, &self.tail_link) {
            Some(path) => path,
            None => { error!(self, "{}: tail {:?} is unset", method_name, self.tail_link); }
        };

        Ok((head, tail))
    }

    pub fn to_head<T: AsRef<Path> + ?Sized + Debug>(&self, path: &T) -> io::Result<()> {
        let p: &Path = path.as_ref();

        // There must not be the situation where the list is empty (no head or tail yet set)
        // because this function is only for promoting an existing element to the head.
        // Use insert_as_head for the other case.
        let (head, tail) = try!(self.get_head_tail("to_head"));

        let next = self.getlink(path, Path::new("next"));
        let prev = self.getlink(path, Path::new("prev"));

        if prev.is_none() == (head == p) {
            if prev.is_some() {
                error!(self, "head entry has a prev: {:?}", path);
            } else {
                error!(self, "entry has no prev but is not head: {:?}", path);
            }
        }

        if next.is_none() == (tail == p) {
            if next.is_some() {
                error!(self, "tail entry has a next: {:?}", path);
            } else {
                error!(self, "entry has no next but is not tail: {:?}", path);
            }
        }

        if next.is_some() && (next.as_ref().unwrap() == p) {
            error!(self, "entry points to itself as next: {:?}", path);
        }
        if prev.is_some() && (prev.as_ref().unwrap() == p) {
            error!(self, "entry points to itself as prev: {:?}", path);
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

    pub fn insert_as_head<T: AsRef<Path> + ?Sized + Debug>(&self, path: &T) -> io::Result<()> {
        let maybe_head = self.getlink(&self.base_dir, &self.head_link);
        let maybe_tail = self.getlink(&self.base_dir, &self.tail_link);

        if maybe_head.is_some() && maybe_tail.is_some() {
            let head = maybe_head.as_ref().unwrap();
            try!(self.makelink(path, Path::new("next"), Some(head)));
            try!(self.makelink(head, Path::new("prev"), Some(path)));
            try!(self.makelink(&self.base_dir, &self.head_link, Some(path)));
        } else if maybe_head.is_none() && maybe_tail.is_none() {
            try!(self.makelink(&self.base_dir, &self.head_link, Some(path)));
            try!(self.makelink(&self.base_dir, &self.tail_link, Some(path)));
            try!(self.makelink(path, Path::new("next"), None::<&Path>));
            try!(self.makelink(path, Path::new("prev"), None::<&Path>));
        } else {
            if maybe_head.is_some() {
                error!(self, "list has a head {:?} but no tail!", maybe_head.unwrap());
            } else {
                error!(self, "list has a tail {:?} but no head!", maybe_tail.unwrap());
            }
        }

        Ok(())
    }

    pub fn insert_as_tail<T: AsRef<Path> + ?Sized + Debug>(&self, path: &T) -> io::Result<()> {
        let maybe_head = self.getlink(&self.base_dir, &self.head_link);
        let maybe_tail = self.getlink(&self.base_dir, &self.tail_link);

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
        } else {
            if maybe_head.is_some() {
                error!(self, "list has a head {:?} but no tail!", maybe_head.unwrap());
            } else {
                error!(self, "list has a tail {:?} but no head!", maybe_tail.unwrap());
            }
        }

        Ok(())
    }

    pub fn disconnect<T: AsRef<Path> + ?Sized + Debug>(&self, path: &T) -> io::Result<()> {
        let p: &Path = path.as_ref();

        let (head, tail) = try!(self.get_head_tail("disconnect"));
        let next = self.getlink(path, Path::new("next"));
        let prev = self.getlink(path, Path::new("prev"));

        if head == p {
            if next.is_none() {
                if tail == p {
                    try!(self.makelink(&self.base_dir, &self.tail_link, None::<&Path>));
                } else {
                    error!(self, "entry has no next but is not tail: {:?}", path);
                }
            } else {
                try!(self.makelink(&self.base_dir, &self.head_link, next.as_ref()));
                try!(self.makelink(next.as_ref().unwrap(), Path::new("prev"), None::<&Path>));
            }
        }

        if tail == p {
            if prev.is_none() {
                if head == p {
                    try!(self.makelink(&self.base_dir, &self.head_link, None::<&Path>));
                } else {
                    error!(self, "entry has no prev but is not head: {:?}", path);
                }
            } else {
                try!(self.makelink(&self.base_dir, &self.tail_link, prev.as_ref()));
                try!(self.makelink(prev.as_ref().unwrap(), Path::new("next"), None::<&Path>));
            }
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
