// BackFS Filesystem Cache Block -> Bucket Map
//
// Copyright 2016-2018 by William R. Fraser
//

use std::ffi::{OsStr, OsString};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use link;
use utils;

use libc;
use walkdir::WalkDir;

macro_rules! trylog {
    ($e:expr, $fmt:expr) => {
        match $e {
            Ok(x) => x,
            Err(e) => {
                error!(concat!($fmt, ": {}\n"), e);
                return Err(e);
            }
        }
    };
    ($e:expr, $fmt:expr, $($arg:tt)*) => {
        match $e {
            Ok(x) => x,
            Err(e) => {
                error!(concat!($fmt, ": {}\n"), $($arg)*, e);
                return Err(e);
            },
        }
    }
}

#[must_use]
#[derive(PartialEq)]
pub enum CacheBlockMapFileResult {
    Current,
    Stale,
    NotPresent,
}

pub trait CacheBlockMap {
    fn check_file_mtime(&self, path: &OsStr, mtime: i64) -> io::Result<CacheBlockMapFileResult>;
    fn set_file_mtime(&mut self, path: &OsStr, mtime: i64) -> io::Result<()>;
    fn get_block(&self, path: &OsStr, block: u64) -> io::Result<Option<OsString>>;
    fn put_block(&mut self, path: &OsStr, block: u64, bucket_path: &OsStr) -> io::Result<()>;
    fn get_block_path(&self, path: &OsStr, block: u64) -> OsString;
    fn invalidate_path<F>(&mut self, path: &OsStr, delete_handler: F) -> io::Result<()>
        where F: FnMut(&OsStr) -> io::Result<()>;
    fn unmap_block(&mut self, block_path: &OsStr) -> io::Result<()>;
    fn is_block_mapped(&self, block_path: &OsStr) -> io::Result<bool>;
    fn for_each_block_under_path<F>(&self, path: &OsStr, handler: F) -> io::Result<()>
        where F: FnMut(&OsStr) -> io::Result<()>;
}

pub struct FSCacheBlockMap {
    map_dir: PathBuf,
}

impl FSCacheBlockMap {
    pub fn new(map_dir: OsString) -> FSCacheBlockMap {
        FSCacheBlockMap {
            map_dir: PathBuf::from(map_dir),
        }
    }

    fn map_path(&self, path: &OsStr) -> PathBuf {
        let path: &Path = Path::new(path);
        let relative_path: &Path = if path.is_absolute() {
            path.strip_prefix("/").unwrap()
        } else {
            path
        };

        self.map_dir.join(relative_path)
    }

    fn prune_empty_directories(&self, mut start: PathBuf) -> io::Result<()> {
        loop {
            if let Err(e) = fs::remove_dir(&start) {
                if e.raw_os_error() == Some(libc::ENOTEMPTY) {
                    break;
                } else {
                    error!("error pruning map directory {:?}: {}", start, e);
                    return Err(e);
                }
            }
            debug!("pruned empty map directory {:?}", start);
            start.pop();
            if start == self.map_dir {
                break;
            }
        }
        Ok(())
    }

    fn has_any_blocks(path: &Path) -> io::Result<bool> {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let name = entry.file_name();
            if &name == "." || &name == ".." {
                continue;
            }
            if &name == "mtime" && entry.file_type()?.is_file() {
                continue;
            }
            return Ok(true);
        }
        Ok(false)
    }
}

impl CacheBlockMap for FSCacheBlockMap {
    fn check_file_mtime(&self, path: &OsStr, mtime: i64) -> io::Result<CacheBlockMapFileResult> {
        let mtime_file = self.map_path(path).join("mtime");
        match utils::read_number_file(&mtime_file, None::<i64>) {
            Ok(Some(n)) => {
                if n == mtime {
                    Ok(CacheBlockMapFileResult::Current)
                } else {
                    Ok(CacheBlockMapFileResult::Stale)
                }
            },
            Ok(None) => Ok(CacheBlockMapFileResult::NotPresent),
            Err(e) => {
                error!("problem with mtime file {:?}: {}", &mtime_file, e);
                Err(e)
            }
        }
    }

    fn set_file_mtime(&mut self, path: &OsStr, mtime: i64) -> io::Result<()> {
        let file_map_dir = self.map_path(path);
        trylog!(fs::create_dir_all(&file_map_dir),
                "set_file_mtime: error creating {:?}", file_map_dir);

        let mtime_file = file_map_dir.join("mtime");
        trylog!(utils::write_number_file(&mtime_file, &mtime),
                "failed to write mtime file {:?}", mtime_file);

        Ok(())
    }

    fn get_block(&self, path: &OsStr, block: u64) -> io::Result<Option<OsString>> {
        let file_map_dir = self.map_path(path);
        match link::getlink(&file_map_dir, &format!("{}", block)) {
            Ok(Some(pathbuf)) => Ok(Some(pathbuf.into_os_string())),
            Ok(None) => Ok(None),
            Err(e) => Err(e)
        }
    }

    fn put_block(&mut self, path: &OsStr, block: u64, bucket_path: &OsStr) -> io::Result<()> {
        debug!("mapping {:?}/{} to {:?}", path, block, bucket_path);
        let file_block = self.map_path(path).join(format!("{}", block));
        trylog!(link::makelink("", &file_block, Some(bucket_path)),
                "error making map link from {:?} to {:?}", &file_block, bucket_path);

        // this makes assumptions on the bucket store implementation
        debug_assert_eq!(link::getlink(bucket_path, "parent").unwrap(), Some(file_block));

        Ok(())
    }

    fn get_block_path(&self, path: &OsStr, block: u64) -> OsString {
        self.map_path(path).join(format!("{}", block)).into_os_string()
    }

    fn invalidate_path<F>(&mut self, path: &OsStr, f: F) -> io::Result<()>
            where F: FnMut(&OsStr) -> io::Result<()> {
        self.for_each_block_under_path(path, f)?;

        let mut map_path = self.map_path(path);
        trylog!(fs::remove_dir_all(&map_path),
                "Error removing map path {:?}", map_path);

        map_path.pop();
        self.prune_empty_directories(map_path)?;
        Ok(())
    }

    fn unmap_block(&mut self, map_block_path: &OsStr) -> io::Result<()> {
        debug!("unmapping {:?}", &map_block_path);

        trylog!(fs::remove_file(&map_block_path),
                "unable to remove map block link {:?}", map_block_path);

        let mut parent = PathBuf::from(map_block_path);
        parent.pop();

        let has_any_blocks = Self::has_any_blocks(&parent)
            .unwrap_or_else(|e| {
                error!("error checking {:?} for any blocks: {}", parent, e);
                false
            });
        if !has_any_blocks {
            let mtime = parent.join("mtime");
            if let Err(e) = fs::remove_file(&mtime) {
                if e.raw_os_error() != Some(libc::ENOENT) {
                    warn!("error removing mtime file {:?}: {}", mtime, e);
                }
            }
        }

        self.prune_empty_directories(parent)?;
        Ok(())
    }

    fn is_block_mapped(&self, block_path: &OsStr) -> io::Result<bool> {
        let bucket_path = trylog!(link::getlink("", block_path),
                                  "is_block_mapped: error reading link {:?}", block_path);
        Ok(bucket_path.is_some())
    }

    fn for_each_block_under_path<F>(&self, path: &OsStr, mut f: F) -> io::Result<()>
            where F: FnMut(&OsStr) -> io::Result<()> {
        let map_path: PathBuf = self.map_path(path);
        for entry_result in WalkDir::new(&map_path) {
            match entry_result {
                Ok(entry) => {
                    let entry_path = entry.path();
                    if entry.file_type().is_symlink() {
                        let bucket_path = match link::getlink("", entry_path) {
                            Ok(Some(path)) => path,
                            Err(e) => {
                                error!("for_each_block_under_path: error reading link {:?}: {}",
                                     entry.path(), e);
                                continue;
                            },
                            Ok(None) => unreachable!()
                        };

                        trylog!(f(bucket_path.as_os_str()),
                                "for_each_block_under_path: callback returned error");
                    }
                },
                Err(e) => {
                    let is_start = e.path() == Some(&map_path);
                    let ioerr = io::Error::from(e);
                    if is_start && ioerr.raw_os_error() == Some(libc::ENOENT) {
                        // If the map directory doesn't exist, there's nothing to do.
                        return Ok(())
                    } else {
                        error!("for_each_block_under_path: error reading directory entry from {:?}: {}",
                               map_path, ioerr);
                        return Err(ioerr)
                    }
                }
            }
        }
        Ok(())
    }
}
