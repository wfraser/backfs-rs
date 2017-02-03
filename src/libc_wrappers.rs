// Libc Wrappers :: Safe wrappers around system calls.
//
// Copyright (c) 2016 by William R. Fraser
//

use std::ffi::{CString, OsString};
use std::io;
use std::mem;
use std::ptr;
use std::os::unix::ffi::OsStringExt;

macro_rules! into_cstring {
    ($path:expr, $syscall:expr) => {
        match CString::new($path.into_vec()) {
            Ok(s) => s,
            Err(e) => {
                error!(concat!($syscall, ": path {:?} contains interior NUL byte"),
                       OsString::from_vec(e.into_vec()));
                return Err(libc::EINVAL);
            }
        }
    }
}

mod libc {
    pub use ::libc::*;

    #[cfg(target_os = "macos")]
    #[allow(non_camel_case_types)]
    pub type stat64 = stat;

    #[cfg(target_os = "macos")]
    pub unsafe fn lstat64(path: *const c_char, stat: *mut stat64) -> c_int {
        lstat(path, stat)
    }

    #[cfg(target_os = "macos")]
    pub const XATTR_NOFOLLOW: c_int = 1;

    #[cfg(target_os = "macos")]
    pub unsafe fn llistxattr(path: *const c_char, namebuf: *mut c_char, size: size_t) -> ssize_t {
        listxattr(path, namebuf, size, XATTR_NOFOLLOW)
    }

    #[cfg(target_os = "macos")]
    pub unsafe fn lgetxattr(path: *const c_char, name: *const c_char, value: *mut c_void, size: size_t) -> ssize_t {
        getxattr(path, name, value, size, 0, XATTR_NOFOLLOW)
    }
}

pub fn opendir(path: OsString) -> Result<usize, libc::c_int> {
    let path_c = into_cstring!(path, "opendir");

    let dir: *mut libc::DIR = unsafe { libc::opendir(mem::transmute(path_c.as_ptr())) };
    if dir.is_null() {
        return Err(io::Error::last_os_error().raw_os_error().unwrap());
    }

    Ok(dir as usize)
}

pub fn readdir(fh: usize) -> Result<Option<libc::dirent>, libc::c_int> {
    let dir: *mut libc::DIR = unsafe { mem::transmute(fh) };
    let mut entry: libc::dirent = unsafe { mem::zeroed() };
    let mut result: *mut libc::dirent = ptr::null_mut();

    let error: i32 = unsafe { libc::readdir_r(dir, &mut entry, &mut result) };
    if error != 0 {
        return Err(error);
    }

    if result.is_null() {
        return Ok(None);
    }

    Ok(Some(entry))
}

pub fn closedir(fh: usize) -> Result<(), libc::c_int> {
    let dir: *mut libc::DIR = unsafe { mem::transmute(fh) };
    if -1 == unsafe { libc::closedir(dir) } {
        Err(io::Error::last_os_error().raw_os_error().unwrap())
    } else {
        Ok(())
    }
}

pub fn open(path: OsString, flags: libc::c_int) -> Result<usize, libc::c_int> {
    let path_c = into_cstring!(path, "open");

    let fd: libc::c_int = unsafe { libc::open(mem::transmute(path_c.as_ptr()), flags) };
    if fd == -1 {
        return Err(io::Error::last_os_error().raw_os_error().unwrap());
    }

    Ok(fd as usize)
}

pub fn close(fh: usize) -> Result<(), libc::c_int> {
    let fd = fh as libc::c_int;
    if -1 == unsafe { libc::close(fd) } {
        Err(io::Error::last_os_error().raw_os_error().unwrap())
    } else {
        Ok(())
    }
}

pub fn lstat(path: OsString) -> Result<libc::stat64, libc::c_int> {
    let path_c = into_cstring!(path, "lstat");

    let mut buf: libc::stat64 = unsafe { mem::zeroed() };
    if -1 == unsafe { libc::lstat64(mem::transmute(path_c.as_ptr()), &mut buf) } {
        return Err(io::Error::last_os_error().raw_os_error().unwrap());
    }

    Ok(buf)
}

pub fn llistxattr(path: OsString, buf: &mut [u8]) -> Result<usize, libc::c_int> {
    let path_c = into_cstring!(path, "llistxattr");

    let result = unsafe {
        libc::llistxattr(path_c.as_ptr(), mem::transmute(buf.as_mut_ptr()), buf.len())
    };
    match result {
        -1 => Err(io::Error::last_os_error().raw_os_error().unwrap()),
        nbytes => Ok(nbytes as usize),
    }
}

pub fn lgetxattr(path: OsString, name: OsString, buf: &mut [u8]) -> Result<usize, libc::c_int> {
    let path_c = into_cstring!(path, "lgetxattr");
    let name_c = into_cstring!(name, "lgetxattr");

    let result = unsafe {
        libc::lgetxattr(path_c.as_ptr(), name_c.as_ptr(), mem::transmute(buf.as_mut_ptr()),
                        buf.len())
    };
    match result {
        -1 => Err(io::Error::last_os_error().raw_os_error().unwrap()),
        nbytes => Ok(nbytes as usize),
    }
}
