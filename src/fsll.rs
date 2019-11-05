// FSLL :: Filesystem Linked List
//
// Copyright 2016-2018 by William R. Fraser
//

use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fmt::Debug;
use std::io;
use std::path::{Path, PathBuf};

use link;

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
    pub fn new<P1, P2, P3>(base_dir: &P1, head_link: &P2, tail_link: &P3) -> Self
        where P1: AsRef<OsStr> + ?Sized,
              P2: AsRef<OsStr> + ?Sized,
              P3: AsRef<OsStr> + ?Sized,
    {
        FSLL {
            base_dir: OsString::from(base_dir),
            head_link: OsString::from(head_link),
            tail_link: OsString::from(tail_link),
        }
    }

    fn getlink<P1, P2>(&self, path: &P1, link: &P2) -> io::Result<Option<PathBuf>>
        where P1: AsRef<Path> + ?Sized + Debug,
              P2: AsRef<Path> + ?Sized + Debug,
    {
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

    fn makelink<P1, P2, P3>(&self, path: &P1, link: &P2, target: Option<&P3>) -> io::Result<()>
        where P1: AsRef<Path> + ?Sized + Debug,
              P2: AsRef<Path> + ?Sized + Debug,
              P3: AsRef<Path> + ?Sized + Debug,
    {
        debug!("makelink: {:?}: {:?} -> {:?}", path, link, target);
        link::makelink(path, link, target)
            .map_err(|e| {
                if target.is_none() {
                    error!("error removing link {:?}/{:?}: {}", path, link, e);
                } else {
                    error!("error creating link {:?}/{:?}: {}", path, link, e);
                }
                e
            })
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
        let (head, tail) = self.get_head_tail("to_head")?;

        debug!("head {:?}", head);
        debug!("tail {:?}", tail);

        let next = self.getlink(path, Path::new("next"))?;
        let prev = self.getlink(path, Path::new("prev"))?;

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

        if let Some(ref p) = prev {
            self.makelink(p, Path::new("next"), next.as_ref())?;
        } else {
            // already head; we're done!
            return Ok(());
        }

        if let Some(ref p) = next {
            self.makelink(p, Path::new("prev"), prev.as_ref())?;
        } else {
            self.makelink(&self.base_dir, &self.tail_link, prev.as_ref())?;
        }

        // assuming head != None
        self.makelink(&head, Path::new("prev"), Some(path))?;
        self.makelink(path, Path::new("next"), Some(&head))?;
        self.makelink(path, Path::new("prev"), None::<&Path>)?;
        self.makelink(&self.base_dir, &self.head_link, Some(path))?;

        Ok(())
    }

    fn insert_as_head<T: AsRef<Path> + ?Sized + Debug>(&self, path: &T) -> io::Result<()> {
        debug!("insert_as_head: {:?}", path);
        let maybe_head = self.getlink(&self.base_dir, &self.head_link)?;
        let maybe_tail = self.getlink(&self.base_dir, &self.tail_link)?;

        match (maybe_head, maybe_tail) {
            (Some(ref head), Some(ref _tail)) => {
                self.makelink(path, Path::new("next"), Some(head))?;
                self.makelink(head, Path::new("prev"), Some(path))?;
                self.makelink(&self.base_dir, &self.head_link, Some(path))?;
            }
            (None, None) => {
                debug!("inserting {:?} as head and tail", path);
                self.makelink(&self.base_dir, &self.head_link, Some(path))?;
                self.makelink(&self.base_dir, &self.tail_link, Some(path))?;
                self.makelink(path, Path::new("next"), None::<&Path>)?;
                self.makelink(path, Path::new("prev"), None::<&Path>)?;
            }
            (Some(head), None) => {
                error_ret!("list has a head {:?} but no tail!", head);
            }
            (None, Some(tail)) => {
                error_ret!("list has a tail {:?} but no head!", tail);
            }
        }

        Ok(())
    }

    fn insert_as_tail<T: AsRef<Path> + ?Sized + Debug>(&self, path: &T) -> io::Result<()> {
        let maybe_head = self.getlink(&self.base_dir, &self.head_link)?;
        let maybe_tail = self.getlink(&self.base_dir, &self.tail_link)?;

        match (maybe_head, maybe_tail) {
            (Some(ref _head), Some(ref tail)) => {
                self.makelink(path, Path::new("prev"), Some(tail))?;
                self.makelink(tail, Path::new("next"), Some(path))?;
                self.makelink(&self.base_dir, &self.tail_link, Some(path))?;
            }
            (None, None) => {
                self.makelink(&self.base_dir, &self.head_link, Some(path))?;
                self.makelink(&self.base_dir, &self.tail_link, Some(path))?;
                self.makelink(path, Path::new("next"), None::<&Path>)?;
                self.makelink(path, Path::new("prev"), None::<&Path>)?;
            }
            (Some(head), None) => {
                error_ret!("list has a head {:?} but no tail!", head);
            }
            (None, Some(tail)) => {
                error_ret!("list has a tail {:?} but no head!", tail);
            }
        }

        Ok(())
    }

    fn disconnect<T: AsRef<Path> + ?Sized + Debug>(&self, path: &T) -> io::Result<()> {
        let p: &Path = path.as_ref();

        let (head, tail) = self.get_head_tail("disconnect")?;
        let next = self.getlink(path, Path::new("next"))?;
        let prev = self.getlink(path, Path::new("prev"))?;

        if head == p {
            if let Some(ref next) = next {
                self.makelink(&self.base_dir, &self.head_link, Some(next))?;
                self.makelink(next, Path::new("prev"), None::<&Path>)?;
            } else if tail == p {
                self.makelink(&self.base_dir, &self.tail_link, None::<&Path>)?;
            } else {
                error_ret!("entry has no next but is not tail: {:?}", path);
            }
        } else if prev.is_none() {
            error_ret!("entry has no prev but is not head: {:?}", path);
        }

        if tail == p {
            if let Some(ref prev) = prev {
                self.makelink(&self.base_dir, &self.tail_link, Some(prev))?;
                self.makelink(prev, Path::new("next"), None::<&Path>)?;
            } else if head == p {
                self.makelink(&self.base_dir, &self.head_link, None::<&Path>)?;
            } else {
                error_ret!("entry has no prev but is not head: {:?}", path);
            }
        } else if next.is_none() {
            error_ret!("entry has no next but is not tail: {:?}", path);
        }

        if let (Some(ref next), Some(ref prev)) = (next, prev) {
            self.makelink(next, Path::new("prev"), Some(prev))?;
            self.makelink(prev, Path::new("next"), Some(next))?;
        }

        self.makelink(path, Path::new("next"), None::<&Path>)?;
        self.makelink(path, Path::new("prev"), None::<&Path>)?;

        Ok(())
    }
}
