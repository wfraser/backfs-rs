// BackFS Filesystem Cache Block -> Bucket Map
//
// Copyright (c) 2016 by William R. Fraser
//

use std::boxed::Box;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use link;
use utils;

pub enum CacheBlockMapFileEntryResult {
    Entry(Box<CacheBlockMapFileEntry>),
    StaleDataPresent,
}

pub trait CacheBlockMapFileEntry {
    fn get(&self, block: u64) -> io::Result<Option<OsString>>;
    fn put(&self, block: u64, bucket_path: &OsStr) -> io::Result<()>;
}

pub trait CacheBlockMap {
    fn get_file_entry(&mut self, path: &OsStr, mtime: i64)
        -> io::Result<CacheBlockMapFileEntryResult>;
    fn invalidate<F>(&mut self, path: &OsStr, f: F) -> io::Result<()>
        where F: FnMut(&OsStr) -> io::Result<()>;
    fn delete(&mut self, bucket_path: &OsStr) -> io::Result<()>;
}

pub struct FSCacheBlockMap {
    map_dir: OsString,
}

pub struct FSCacheBlockMapFileEntry {
    file_map_dir: OsString,
}

impl FSCacheBlockMap {
    pub fn new(map_dir: OsString) -> FSCacheBlockMap {
        FSCacheBlockMap {
            map_dir: map_dir,
        }
    }

    fn map_path(&self, path: &OsStr) -> PathBuf {
        let mut map_path = PathBuf::from(&self.map_dir);

        let path: &Path = Path::new(path);
        let relative_path: &Path = if path.is_absolute() {
            path.strip_prefix("/").unwrap()
        } else {
            path
        };

        map_path.push(relative_path);
        map_path
    }

    fn enumerate_blocks_of_path<F>(&self, map_path: &Path, f: F) -> io::Result<()>
            where F: FnMut(&OsStr) -> io::Result<()> {
        unimplemented!();
    }

}

impl CacheBlockMap for FSCacheBlockMap {
    fn get_file_entry(&mut self, path: &OsStr, mtime: i64)
            -> io::Result<CacheBlockMapFileEntryResult> {
        let map_path = self.map_path(path);
        debug!("get_file_entry: {:?}", map_path);

        if let Err(e) = fs::create_dir_all(&map_path) {
            error!("get_file_entry: error creating {:?}: {}", map_path, e);
            return Err(e);
        }

        let mtime_path = map_path.join("mtime");
        match utils::read_number_file(&mtime_path, None::<i64>) {
            Ok(Some(n)) => {
                if n != mtime {
                    info!("cached data is stale: {:?}", path);
                    return Ok(CacheBlockMapFileEntryResult::StaleDataPresent);
                }
            },
            Ok(None) => {
                try!(utils::write_number_file(&mtime_path, mtime));
            },
            Err(e) => {
                error!("problem with mtime file {:?}: {}", &mtime_path, e);
                return Err(e);
            }
        }

        Ok(CacheBlockMapFileEntryResult::Entry(Box::new(FSCacheBlockMapFileEntry {
            file_map_dir: map_path.into_os_string()
        })))
    }

    fn invalidate<F>(&mut self, path: &OsStr, f: F) -> io::Result<()>
            where F: FnMut(&OsStr) -> io::Result<()> {
        let map_path: PathBuf = self.map_path(path);
        try!(self.enumerate_blocks_of_path(&map_path, f));
        if let Err(e) = fs::remove_dir_all(&map_path) {
            error!("Error removing map path {:?}: {}", map_path, e);
            return Err(e);
        }
        Ok(())
    }

    fn delete(&mut self, bucket_path: &OsStr) -> io::Result<()> {
        // TODO: read the 'parent' link inside the now-freed bucket
        // remove the parent link, and remove the map directory it points to.
        unimplemented!();
    }
}

impl CacheBlockMapFileEntry for FSCacheBlockMapFileEntry {
    fn get(&self, block: u64) -> io::Result<Option<OsString>> {
        match link::getlink(&self.file_map_dir, &format!("{}", block)) {
            Ok(Some(pathbuf)) => Ok(Some(pathbuf.into_os_string())),
            Ok(None) => Ok(None),
            Err(e) => Err(e)
        }
    }
    fn put(&self, block: u64, bucket_path: &OsStr) -> io::Result<()> {
        unimplemented!();
    }
}
