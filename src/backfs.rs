// BackFS FUSE Filesystem implementation
//
// Copyright 2016-2021 by William R. Fraser
//

use std::cmp;
use std::ffi::{CStr, CString, OsStr, OsString};
use std::fs;
use std::fs::File;
use std::io;
use std::mem;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::MetadataExt;
use std::os::unix::io::{FromRawFd, IntoRawFd};
use std::path::{Path, PathBuf};
use std::str;
use std::time::{Duration, SystemTime};

use crate::arg_parse::BackfsSettings;
use crate::block_map::FsCacheBlockMap;
use crate::bucket_store::FsCacheBucketStore;
use crate::fscache::{FsCache, Cache};
use crate::fsll::Fsll;
use crate::libc_wrappers;
use crate::utils;

use daemonize::Daemonize;
use fuse_mt::*;

const TTL: Duration = Duration::from_secs(1);

const BACKFS_CONTROL_FILE_NAME: &str = ".backfs_control";
const BACKFS_CONTROL_FILE_PATH: &str = "/.backfs_control";

const BACKFS_VERSION_FILE_NAME: &str = ".backfs_version";
const BACKFS_VERSION_FILE_PATH: &str = "/.backfs_version";

const BACKFS_CONTROL_FILE_HELP: &str = "commands: test, noop, invalidate <path>, free_orphans\n";

fn epoch_time(secs: i64, nanos: u32) -> SystemTime {
    if secs > 0 {
        std::time::UNIX_EPOCH + Duration::new(secs as u64, nanos)
    } else {
        std::time::UNIX_EPOCH - Duration::new(secs as u64, nanos)
    }
}

pub struct BackFs {
    pub settings: BackfsSettings,
    fscache: FsCache<FsCacheBlockMap, FsCacheBlockMap,
                     FsCacheBucketStore<Fsll>, FsCacheBucketStore<Fsll>>,
    uid: u32,
}

fn is_backfs_fake_file(path: &Path) -> bool {
    path == Path::new(BACKFS_CONTROL_FILE_PATH)
        || path == Path::new(BACKFS_VERSION_FILE_PATH)
}

fn backfs_version_str() -> String {
    format!("BackFS version: {} {}\nFuseMT version: {}\n",
            super::VERSION, super::GIT_REVISION, ::fuse_mt::VERSION)
}

fn backfs_fake_file_attr(path: Option<&str>, uid: u32) -> Option<FileAttr> {
    let fake_file_attrs = FileAttr {
        size: 0,
        blocks: 0,
        atime: epoch_time(crate::BUILD_TIME, 0),
        mtime: epoch_time(crate::BUILD_TIME, 0),
        ctime: epoch_time(crate::BUILD_TIME, 0),
        crtime: epoch_time(crate::BUILD_TIME, 0),
        kind: FileType::RegularFile,
        perm: 0o000,
        nlink: 1,
        uid: 0,
        gid: 0,
        rdev: 0,
        flags: 0,
    };

    match path {
        Some(BACKFS_CONTROL_FILE_PATH) => {
            let mut attr = fake_file_attrs;
            attr.perm = 0o600; // -rw-------
            attr.uid = uid;
            attr.size = BACKFS_CONTROL_FILE_HELP.as_bytes().len() as u64;
            Some(attr)
        },
        Some(BACKFS_VERSION_FILE_PATH) => {
            let mut attr = fake_file_attrs;
            attr.perm = 0o444; // -r--r--r--
            attr.uid = uid;
            attr.size = backfs_version_str().as_bytes().len() as u64;
            Some(attr)
        },
        _ => None
    }
}

fn human_number(n: u64) -> String {
    if n >= 1024 * 1024 * 1024 {
        format!("{:.2} GiB", n as f64 / (1024. * 1024. * 1024.))
    } else if n >= 1024 * 1024 {
        format!("{:.2} MiB", n as f64 / (1024. * 1024.))
    } else if n >= 1024 {
        format!("{:.2} KiB", n as f64 / (1024.))
    } else {
        format!("{} B", n)
    }
}

fn mode_to_filetype(mode: libc::mode_t) -> Result<FileType, libc::c_int> {
    Ok(match mode & libc::S_IFMT {
        libc::S_IFDIR => FileType::Directory,
        libc::S_IFREG => FileType::RegularFile,
        libc::S_IFLNK => FileType::Symlink,
        libc::S_IFBLK => FileType::BlockDevice,
        libc::S_IFCHR => FileType::CharDevice,
        libc::S_IFIFO  => FileType::NamedPipe,
        libc::S_IFSOCK => FileType::Socket,
        unknown => {
            error!("unrecognized file type {:0x}", unknown);
            return Err(libc::EIO);
        }
    })
}

#[cfg(target_os = "macos")]
fn statfs_to_fuse(statfs: libc::statfs) -> Statfs {
    Statfs {
        blocks: statfs.f_blocks,
        bfree: statfs.f_bfree,
        bavail: statfs.f_bavail,
        files: statfs.f_files,
        ffree: statfs.f_ffree,
        bsize: statfs.f_bsize as u32,
        namelen: 255, // TODO
        frsize: 0, // TODO
    }
}

#[cfg(target_os = "linux")]
fn statfs_to_fuse(statfs: libc::statfs) -> Statfs {
    Statfs {
        blocks: statfs.f_blocks,
        bfree: statfs.f_bfree,
        bavail: statfs.f_bavail,
        files: statfs.f_files,
        ffree: statfs.f_ffree,
        bsize: statfs.f_bsize as u32,
        namelen: statfs.f_namelen as u32,
        frsize: statfs.f_frsize as u32,
    }
}

impl BackFs {
    pub fn new(settings: BackfsSettings) -> Self {
        let max_bytes = if settings.cache_size == 0 {
            None
        } else {
            Some(settings.cache_size)
        };

        let map_dir = PathBuf::from(&settings.cache).join("map").into_os_string();
        debug!("map dir: {:?}", map_dir);
        utils::create_dir_and_check_access(&map_dir).unwrap();
        let map = FsCacheBlockMap::new(map_dir);

        let buckets_dir = PathBuf::from(&settings.cache).join("buckets").into_os_string();
        debug!("buckets dir: {:?}", buckets_dir);
        utils::create_dir_and_check_access(&buckets_dir).unwrap();
        let used_list = Fsll::new(&buckets_dir, "head", "tail");
        let free_list = Fsll::new(&buckets_dir, "free_head", "free_tail");
        let store = FsCacheBucketStore::new(buckets_dir, used_list, free_list,
                                            settings.block_size, max_bytes);

        let uid = unsafe { libc::getuid() };
        debug!("uid = {}", uid);

        Self {
            fscache: FsCache::new(map, store, settings.block_size),
            settings,
            uid,
        }
    }

    fn real_path<T: AsRef<OsStr>>(&self, partial: &T) -> OsString {
        PathBuf::from(&self.settings.backing_fs)
                .join(Path::new(partial).strip_prefix("/").unwrap())
                .into_os_string()
    }


    fn stat_real<T: AsRef<OsStr> + ::std::fmt::Debug>(&self, path: &T, fh: Option<u64>) 
        -> Result<FileAttr, libc::c_int>
    {
        let real: OsString = self.real_path(path);
        debug!("stat_real: {:?} (fh={:?})", real, fh);

        let result = if let Some(fh) = fh {
            // NOTE: Currently rust-fuse doesn't ever pass us a fh because it targets too old of a
            // FUSE ABI.)
            libc_wrappers::fstat(fh as usize)
        } else {
            libc_wrappers::lstat(real)
        };

        let stat = result.inspect_err(|&errno| {
            let msg = format!("lstat: {:?}: {}", path, io::Error::from_raw_os_error(errno));
            if errno == libc::ENOENT {
                // avoid being overly noisy
                debug!("{}", msg);
            } else {
                error!("{}", msg);
            }
        })?;

        let kind = mode_to_filetype(stat.st_mode)?;

        let mut mode = stat.st_mode & 0o7777; // st_mode encodes the type AND the mode.
        if !self.settings.rw {
            mode &= !0o222; // disable the write bits if we're not in RW mode.
        }

        Ok(FileAttr {
            size: stat.st_size as u64,
            blocks: stat.st_blocks as u64,
            atime: epoch_time(stat.st_atime as i64, stat.st_atime_nsec as u32),
            mtime: epoch_time(stat.st_mtime as i64, stat.st_mtime_nsec as u32),
            ctime: epoch_time(stat.st_ctime as i64, stat.st_ctime_nsec as u32),
            crtime: std::time::UNIX_EPOCH,
            kind,
            perm: mode as u16,
            nlink: stat.st_nlink as u32,
            uid: stat.st_uid,
            gid: stat.st_gid,
            rdev: stat.st_rdev as u32,
            flags: 0,
        })
    }

    fn backfs_control_file_write(&self, data: &[u8]) -> ResultWrite {
        // remove a trailing newline if it exists
        let data_trimmed = if data.last() == Some(&0x0A) {
            &data[..data.len() - 1]
        } else {
            data
        };

        let first_space = data_trimmed.iter().position(|x| *x == 0x20)
                .unwrap_or(data_trimmed.len());
        let (command_bytes, arg_bytes) = data_trimmed.split_at(first_space);
        let command = str::from_utf8(command_bytes).unwrap_or("[invalid utf8]");

        let arg_start = if arg_bytes.is_empty() { 0 } else { 1 }; // skip over the space delimiter if there is one
        let arg = OsStr::from_bytes(&arg_bytes[arg_start..]);

        debug!("command: {:?}, arg: {:?}", command, arg);

        match command {
            "test" => {
                return Err(libc::EXDEV);
            },
            "noop" => (),
            "invalidate" => {
                let _ignore_errors = self.fscache.invalidate_path(arg);
            },
            "free_block" => {
                let path_and_block = Path::new(arg);
                let path = path_and_block.parent()
                    .ok_or_else(|| { warn!("bad path: no parent"); libc::EINVAL })?;
                let block: u64 = path_and_block.file_name()
                    .ok_or_else(|| { warn!("no filename given"); libc::EINVAL })?
                    .to_str()
                    .ok_or_else(|| { warn!("bad UTF-8"); libc::EINVAL })?
                    .parse()
                    .map_err(|e| { warn!("doesn't end in a valid number: {}", e); libc::EINVAL })?;
                match self.fscache.free_block(path.as_os_str(), block) {
                    Ok(Some(n)) => debug!("{:?}/{}: {} bytes freed", path, block, n),
                    Ok(None) => debug!("{:?}/{} file or block not found", path, block),
                    Err(e) => error!("error freeing block {} of {:?}: {}", block, path, e),
                }
            },
            "free_orphans" => {
                let _ignore_errors = self.fscache.free_orphaned_buckets();
            },
            _ => {
                return Err(libc::EBADMSG);
            }
        }

        Ok(data.len() as u32)
    }

    fn internal_init(&self) -> io::Result<()> {
        println!("BackFS: Initializing cache and scanning existing cache directory...");

        if let Err(e) = self.fscache.init() {
            println!("Error: Failed to initialize cache: {}", e);
            return Err(e);
        }

        let max_cache = if self.settings.cache_size == 0 {
            unsafe {
                let path_bytes = Vec::from(self.settings.cache.as_os_str().as_bytes());
                let path_c = CString::from_vec_unchecked(path_bytes);
                let mut statbuf: libc::statvfs = mem::zeroed();
                if -1 == libc::statvfs(path_c.into_raw(), &mut statbuf as *mut libc::statvfs) {
                    let e = io::Error::last_os_error();
                    println!("Error: failed to statvfs on the cache filesystem: {}", e);
                    return Err(e);
                } else {
                    statbuf.f_bsize as u64 * statbuf.f_blocks as u64
                }
            }
        } else {
            self.settings.cache_size
        };

        println!("BackFS: Cache: {} used out of {} ({:.2} %).",
                 human_number(self.fscache.used_size()),
                 human_number(max_cache),
                 (self.fscache.used_size() as f64 / max_cache as f64 * 100.));

        Ok(())
    }
}

impl FilesystemMT for BackFs {
    fn init(&self, _req: RequestInfo) -> ResultEmpty {
        debug!("init");

        if let Err(e) = self.internal_init() {
            println!("Error initializing BackFS: {}", e);
            panic!("{}", e);
        }

        println!("BackFS: Ready.");

        if !self.settings.foreground {
            println!("BackFS: Going to background.");
            if let Err(e) = Daemonize::new().working_directory("/").start() {
                let msg = format!("Error forking to background: {}", e);
                error!("{}", msg);
                panic!("{}", msg);
            }
        }

        Ok(())
    }

    fn getattr(&self, _req: RequestInfo, path: &Path, fh: Option<u64>) -> ResultEntry {
        debug!("getattr: {:?}", path);

        if let Some(attr) = backfs_fake_file_attr(path.to_str(), self.uid) {
            return Ok((TTL, attr));
        }

        let attr = self.stat_real(&path, fh)
            .inspect_err(|&errno| {
                let msg = format!("getattr: {:?}: {}", path, io::Error::from_raw_os_error(errno));
                if errno == libc::ENOENT {
                    debug!("{}", msg);
                } else {
                    error!("{}", msg);
                }
            })?;

        Ok((TTL, attr))
    }

    fn opendir(&self, _req: RequestInfo, path: &Path, _flags: u32) -> ResultOpen {
        debug!("opendir: {:?}", path);

        let real: OsString = self.real_path(&path);
        debug!("opendir: real = {:?}", real);

        match libc_wrappers::opendir(real) {
            Ok(fh) => Ok((fh as u64, 0)),
            Err(e) => Err(e)
        }
    }

    fn readdir(&self, _req: RequestInfo, path: &Path, fh: u64) -> ResultReaddir {
        debug!("readdir: {:?}", path);
        let mut entries: Vec<DirectoryEntry> = vec![];

        if fh == 0 {
            error!("readdir: missing fh");
            return Err(libc::EINVAL);
        }

        let is_root = path == Path::new("/");

        if is_root {
            entries.push(DirectoryEntry{
                name: OsString::from(BACKFS_CONTROL_FILE_NAME),
                kind: FileType::RegularFile
            });
            entries.push(DirectoryEntry{
                name: OsString::from(BACKFS_VERSION_FILE_NAME),
                kind: FileType::RegularFile
            });
        }

        loop {
            match libc_wrappers::readdir(fh as usize) {
                Ok(Some(entry)) => {
                    let name_c = unsafe { CStr::from_ptr(entry.d_name.as_ptr()) };
                    let name = OsStr::from_bytes(name_c.to_bytes()).to_owned();

                    let entry_path = PathBuf::from(path).join(&name);

                    let filetype = match entry.d_type {
                        libc::DT_DIR => FileType::Directory,
                        libc::DT_REG => FileType::RegularFile,
                        libc::DT_LNK => FileType::Symlink,
                        libc::DT_BLK => FileType::BlockDevice,
                        libc::DT_CHR => FileType::CharDevice,
                        libc::DT_FIFO => FileType::NamedPipe,
                        libc::DT_SOCK => FileType::Socket,
                        _ => {
                            // The directory entry has no file type info included. Do an lstat to
                            // get it. Also do this for unrecognized values before failing out,
                            // just in case lstat gives something we recognize.
                            let real_path = self.real_path(&entry_path);
                            if entry.d_type != libc::DT_UNKNOWN {
                                warn!("unrecognized dirent.d_type value {:0x} for {:?}",
                                      entry.d_type,
                                      real_path);
                            }
                            match libc_wrappers::lstat(real_path) {
                                Ok(stat64) => mode_to_filetype(stat64.st_mode)?,
                                Err(errno) => {
                                    let ioerr = io::Error::from_raw_os_error(errno);
                                    panic!("lstat failed after readdir_r gave no file type for {:?}: {}",
                                           path, ioerr);
                                }
                            }

                        }
                    };

                    debug!("readdir: adding entry {:?} of type {:?}", name, filetype);
                    entries.push(DirectoryEntry {
                        name,
                        kind: filetype,
                    });
                },
                Ok(None) => { break; },
                Err(e) => {
                    error!("readdir: {:?}: {}", path, e);
                    return Err(e);
                }
            }
        }

        Ok(entries)
    }

    fn releasedir(&self, _req: RequestInfo, path: &Path, fh: u64, _flags: u32) -> ResultEmpty {
        debug!("releasedir: {:?}", path);
        match libc_wrappers::closedir(fh as usize) {
            Ok(()) => { Ok(()) }
            Err(e) => {
                error!("closedir({:?}): {}", path, io::Error::from_raw_os_error(e));
                Err(e)
            }
        }
    }

    fn open(&self, _req: RequestInfo, path: &Path, flags: u32) -> ResultOpen {
        debug!("open: {:?} flags={:#x}", path, flags);

        if let Some(path) = path.to_str() {
            if path == BACKFS_CONTROL_FILE_PATH || path == BACKFS_VERSION_FILE_PATH {
                return Ok((0, flags));
            }
        }

        let real_path = self.real_path(&path);

        match libc_wrappers::open(real_path, flags as libc::c_int) {
            Ok(fh) => { Ok((fh as u64, flags)) },
            Err(e) => {
                error!("open({:?}): {}", path, io::Error::from_raw_os_error(e));
                Err(e)
            }
        }
    }

    fn release(&self, _req: RequestInfo, path: &Path, fh: u64, _flags: u32, _lock_owner: u64, _flush: bool) -> ResultEmpty {
        debug!("release: {:?}", path);

        if is_backfs_fake_file(path) {
            // we didn't open any real file
            return Ok(());
        }

        match libc_wrappers::close(fh as usize) {
            Ok(()) => { Ok(()) },
            Err(e) => {
                error!("close({:?}): {}", path, io::Error::from_raw_os_error(e));
                Err(e)
            }
        }
    }

    fn read(
        &self,
        _req: RequestInfo,
        path: &Path,
        fh: u64,
        offset: u64,
        size: u32,
        result: impl FnOnce(ResultSlice<'_>) -> CallbackResult,
    ) -> CallbackResult {
        debug!("read: {:?} {:#x} @ {:#x}", path, size, offset);

        let fake_data: Option<Vec<u8>> = match path.to_str() {
            Some(BACKFS_CONTROL_FILE_PATH) => Some(BACKFS_CONTROL_FILE_HELP.bytes().collect()),
            Some(BACKFS_VERSION_FILE_PATH) => Some(backfs_version_str().into_bytes()),
            _ => None
        };

        if let Some(data) = fake_data {
            return if offset as usize >= data.len() {
                // Request out of range; return empty result.
                result(Ok(&[]))
            } else {
                let offset = offset as usize;
                let size = size as usize;
                let end = cmp::min(data.len(), offset + size);
                result(Ok(&data[offset .. end]))
            }
        }

        let mut real_file = unsafe { File::from_raw_fd(fh as libc::c_int) };

        let mtime = match real_file.metadata() {
            Ok(metadata) => metadata.mtime(),
            Err(e) => {
                error!("unable to get metadata from {:?}: {}", path, e);
                return result(Err(e.raw_os_error().unwrap()));
            }
        };

        let ret = match self.fscache.fetch(path.as_os_str(), offset, size as u64, &mut real_file, mtime) {
            Ok(data) => {
                result(Ok(&data))
            },
            Err(e) => {
                result(Err(e.raw_os_error().unwrap()))
            }
        };

        // Release control of the file descriptor, so it is not closed when this function
        // returns.
        let _ = real_file.into_raw_fd();

        ret
    }

    fn write(&self, _req: RequestInfo, path: &Path, _fh: u64, offset: u64, data: Vec<u8>, _flags: u32) -> ResultWrite {
        debug!("write: {:?} {:#x}@{:#x}", path, data.len(), offset);

        match path.to_str() {
            Some(BACKFS_CONTROL_FILE_PATH) => {
                return self.backfs_control_file_write(&data);
            },
            Some(BACKFS_VERSION_FILE_PATH) => {
                return Err(libc::EACCES);
            }
            _ => ()
        }

        if !self.settings.rw {
            return Err(libc::EROFS);
        }

        // TODO
        Err(libc::ENOSYS)
    }

    fn readlink(&self, _req: RequestInfo, path: &Path) -> ResultData {
        debug!("readlink: {:?}", path);

        let real_path = self.real_path(&path);

        match fs::read_link(&real_path) {
            Ok(path) => {
                Ok(path.into_os_string().into_vec())
            },
            Err(e) => {
                error!("readlink({:?}): {}", real_path, e);
                Err(e.raw_os_error().unwrap())
            }
        }
    }

    fn statfs(&self, _req: RequestInfo, path: &Path) -> ResultStatfs {
        debug!("statfs: {:?}", path);

        let real = self.real_path(&path);
        let mut buf: libc::statfs = unsafe { ::std::mem::zeroed() };
        let result = unsafe {
            let path_c = CString::from_vec_unchecked(real.into_vec());
            libc::statfs(path_c.as_ptr(), &mut buf)
        };

        if -1 == result {
            let e = io::Error::last_os_error();
            error!("statfs({:?}): {}", path, e);
            Err(e.raw_os_error().unwrap())
        } else {
            Ok(statfs_to_fuse(buf))
        }
    }

    fn listxattr(&self, _req: RequestInfo, path: &Path, size: u32) -> ResultXattr {
        debug!("listxattr: {:?}", path);

        let extra = b"user.backfs.in_cache\0";

        let real = self.real_path(&path);
        if size == 0 {
            let mut nbytes = libc_wrappers::llistxattr(real, &mut[]).unwrap_or(0);
            nbytes += extra.len();
            Ok(Xattr::Size(nbytes as u32))
        } else {
            let mut data = Vec::<u8>::with_capacity(size as usize);
            data.extend_from_slice(extra);
            unsafe { data.set_len(size as usize) };
            let nread = libc_wrappers::llistxattr(real, &mut data.as_mut_slice()[extra.len()..])
                .unwrap_or(0);
            data.truncate(nread + extra.len());
            Ok(Xattr::Data(data))
        }
    }

    fn getxattr(&self, _req: RequestInfo, path: &Path, name: &OsStr, size: u32) -> ResultXattr {
        debug!("getxattr: {:?} {:?} {}", path, name, size);

        let extra = OsStr::new("user.backfs.in_cache");

        let real = self.real_path(&path);
        if size == 0 {
            if name == extra {
                Ok(Xattr::Size(21)) // number of digits in 2^64, plus null byte
            } else {
                let nbytes = libc_wrappers::lgetxattr(real, name.to_owned(), &mut[])?;
                Ok(Xattr::Size(nbytes as u32))
            }
        } else if name == extra {
            let nbytes = self.fscache.count_cached_bytes(path.as_os_str());
            let mut data = format!("{}", nbytes).into_bytes();
            data.truncate(size as usize);
            Ok(Xattr::Data(data))
        } else {
            let mut data = Vec::<u8>::with_capacity(size as usize);
            let nread = libc_wrappers::lgetxattr(
                real, name.to_owned(), data.spare_capacity_mut())?;
            unsafe { data.set_len(nread) };
            Ok(Xattr::Data(data))
        }
    }

    // TODO: implement the rest of the syscalls needed
}
