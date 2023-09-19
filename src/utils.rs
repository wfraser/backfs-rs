// Miscellaneous BackFS Utility Functions
//
// Copyright 2016-2020 by William R. Fraser
//

use std::ffi::CString;
use std::fs::{self, File, OpenOptions};
use std::fmt::{Display, Debug};
use std::io::{self, Read, Write};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::str::FromStr;

pub fn read_number_file<N, P>(path: &P, default: Option<N>) -> io::Result<Option<N>>
        where N: Display + FromStr,
              <N as FromStr>::Err: Debug,
              P: AsRef<Path> + ?Sized + Debug
{
    let (mut file, new) = if default.is_none() {
        // If no default value was given, don't create a file, just open the existing one if
        // there is one, or return None.
        let file = match File::open(path) {
            Ok(file) => file,
            Err(e) => {
                if e.raw_os_error() == Some(libc::ENOENT) {
                    return Ok(None);
                } else {
                    error!("read_number_file: error creating file {:?}: {}", path, e);
                    return Err(e);
                }
            }
        };
        (file, false)
    } else {
        let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(path)
                .map_err(|e| {
                    error!("read_number_file: error creating file {:?}: {}", path, e);
                    e
                })?;

        let new = file.metadata()
                .map_err(|e| {
                    error!("read_number_file: error getting metadata for {:?}: {}", path, e);
                    e
                })?
                .len() == 0;

        (file, new)
    };

    if new {
        match default {
            Some(n) => match write!(file, "{}", n) {
                Ok(_) => Ok(Some(n)),
                Err(e) => {
                    error!("read_number_file: error writing to {:?}: {}", path, e);
                    Err(e)
                }
            },
            None => Ok(None)
        }
    } else {
        let mut data: Vec<u8> = vec![];
        if let Err(e) = file.read_to_end(&mut data) {
            error!("read_number_file: error reading from {:?}: {}", path, e);
            return Err(e);
        }

        let string = match String::from_utf8(data) {
            Ok(s) => s,
            Err(e) => {
                let msg = format!("read_number_file: error interpreting file {:?} as UTF8 string: {}", path, e);
                error!("{}", msg);
                return Err(io::Error::new(io::ErrorKind::Other, msg));
            }
        };

        let number: N = match string.trim().parse() {
            Ok(n) => n,
            Err(e) => {
                let msg = format!("read_number_file: error interpreting file {:?} as number: {:?}", path, e);
                error!("{}", msg);
                return Err(io::Error::new(io::ErrorKind::Other, msg));
            }
        };

        Ok(Some(number))
    }
}

pub fn write_number_file<N, P>(path: P, number: &N) -> io::Result<()>
    where N: Display + FromStr,
          P: AsRef<Path> + Debug,
{
    match OpenOptions::new()
                      .write(true)
                      .truncate(true)
                      .create(true)
                      .open(path.as_ref()) {
        Ok(mut file) => {
            if let Err(e) = write!(file, "{}", number) {
                error!("write_number_file: error writing to {:?}: {}", path, e);
                return Err(e);
            }
        },
        Err(e) => {
            error!("write_number_file: error opening {:?}: {}", path, e);
            return Err(e);
        }
    }
    Ok(())
}

pub fn create_dir_and_check_access<T>(path: T) -> io::Result<()>
    where T: AsRef<Path> + Debug,
{
    let path = path.as_ref();
    if let Err(e) = fs::create_dir(path) {
        // Already existing is fine.
        if e.raw_os_error() != Some(libc::EEXIST) {
            error!("create_dir_and_check_access: unable to create {:?}: {}", path, e);
            return Err(e);
        }
    }

    // Check for read, write, and execute permissions on the folder.
    // This doesn't 100% guarantee things will work, but it will catch most common problems
    // early, so it's still worth doing.
    unsafe {
        // safe because it can't have NUL bytes if we already got this far...
        let path_c = CString::from_vec_unchecked(Vec::from(path.as_os_str().as_bytes()));
        if 0 != libc::access(path_c.as_ptr(), libc::R_OK | libc::W_OK | libc::X_OK) {
            let e = io::Error::last_os_error();
            error!("no R/W/X access to {:?}: {}", path, e);
            return Err(e);
        }
    }

    Ok(())
}
