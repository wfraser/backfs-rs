// BackFS FUSE Filesystem implementation
//
// Copyright (c) 2016 by William R. Fraser
//

use std::cmp;
use std::ffi::{CStr, OsStr, OsString};
use std::fs;
use std::fs::File;
use std::io;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::MetadataExt;
use std::os::unix::io::{FromRawFd, IntoRawFd};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::str;

use arg_parse::BackfsSettings;
use fscache::FSCache;
use fsll::FSLL;
use inodetable::InodeTable;
use libc_wrappers;

use daemonize::Daemonize;
use fuse::*;
use libc;
use log;
use time::Timespec;

pub const BACKFS_VERSION: &'static str = "BackFS version: 0.1.0\n";

const TTL: Timespec = Timespec { sec: 1, nsec: 0 };

// inode 2:
const BACKFS_CONTROL_FILE_NAME: &'static str = ".backfs_control";
const BACKFS_CONTROL_FILE_PATH: &'static str = "/.backfs_control";

// inode 3:
const BACKFS_VERSION_FILE_NAME: &'static str = ".backfs_version";
const BACKFS_VERSION_FILE_PATH: &'static str = "/.backfs_version";

const BACKFS_CONTROL_FILE_HELP: &'static str = "commands: test, noop, invalidate <path>, free_orphans\n";

const BACKFS_FAKE_FILE_ATTRS: FileAttr = FileAttr {
    ino: 0,
    size: 0,
    blocks: 0,
    atime: Timespec { sec: 0, nsec: 0 },
    mtime: Timespec { sec: 0, nsec: 0 },
    ctime: Timespec { sec: 0, nsec: 0 },
    crtime: Timespec { sec: 0, nsec: 0 },
    kind: FileType::RegularFile,
    perm: 0o000,
    nlink: 1,
    uid: 0,
    gid: 0,
    rdev: 0,
    flags: 0,
};

pub struct BackFS {
    pub settings: BackfsSettings,
    inode_table: InodeTable,
    fscache: FSCache<FSLL>,
}

macro_rules! log2 {
    ($lvl:expr, $($arg:tt)+) => (
        log!(target: "BackFS", $lvl, $($arg)+));
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

fn backfs_fake_file_attr(path: Option<&str>) -> Option<FileAttr> {
    match path {
        Some(BACKFS_CONTROL_FILE_PATH) => {
            let mut attr = BACKFS_FAKE_FILE_ATTRS.clone();
            attr.ino = 2;
            attr.perm = 0o600; // -rw-------
            attr.size = BACKFS_CONTROL_FILE_HELP.as_bytes().len() as u64;
            Some(attr)
        },
        Some(BACKFS_VERSION_FILE_PATH) => {
            let mut attr = BACKFS_FAKE_FILE_ATTRS.clone();
            attr.ino = 3;
            attr.perm = 0o444; // -r--r--r--
            attr.size = BACKFS_VERSION.as_bytes().len() as u64;
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

impl BackFS {
    pub fn new(settings: BackfsSettings) -> BackFS {
        let buckets_dir = PathBuf::from(&settings.cache).join("buckets").into_os_string();
        BackFS {
            fscache: FSCache::new(&settings.cache, settings.block_size, settings.cache_size,
                                  FSLL::new(&buckets_dir, "head", "tail"),
                                  FSLL::new(&buckets_dir, "free_head", "free_tail"),
                                  buckets_dir),
            settings: settings,
            inode_table: InodeTable::new(),
        }
    }

    fn real_path(&self, partial: &OsString) -> OsString {
        PathBuf::from(&self.settings.backing_fs)
                .join(Path::new(partial).strip_prefix("/").unwrap())
                .into_os_string()
    }

    fn stat_real(&mut self, path: &Rc<OsString>) -> io::Result<FileAttr> {
        let real: OsString = self.real_path(&path);
        debug!("stat_real: {:?}", real);

        match libc_wrappers::lstat(real) {
            Ok(stat) => {
                let inode = self.inode_table.add_or_get(path.clone());

                let kind = match stat.st_mode & libc::S_IFMT {
                    libc::S_IFDIR => FileType::Directory,
                    libc::S_IFREG => FileType::RegularFile,
                    libc::S_IFLNK => FileType::Symlink,
                    libc::S_IFBLK => FileType::BlockDevice,
                    libc::S_IFCHR => FileType::CharDevice,
                    libc::S_IFIFO  => FileType::NamedPipe,
                    libc::S_IFSOCK => {
                        warn!("FUSE doesn't support Socket file type; translating to NamedPipe instead.");
                        FileType::NamedPipe
                    },
                    _ => { panic!("unknown file type"); }
                };

                let mut mode = stat.st_mode & 0o7777; // st_mode encodes the type AND the mode.
                if !self.settings.rw {
                    mode &= !0o222; // disable the write bits if we're not in RW mode.
                }

                Ok(FileAttr {
                    ino: inode,
                    size: stat.st_size as u64,
                    blocks: stat.st_blocks as u64,
                    atime: Timespec { sec: stat.st_atime as i64, nsec: stat.st_atime_nsec as i32 },
                    mtime: Timespec { sec: stat.st_mtime as i64, nsec: stat.st_mtime_nsec as i32 },
                    ctime: Timespec { sec: stat.st_ctime as i64, nsec: stat.st_ctime_nsec as i32 },
                    crtime: Timespec { sec: 0, nsec: 0 },
                    kind: kind,
                    perm: mode as u16,
                    nlink: stat.st_nlink as u32,
                    uid: stat.st_uid,
                    gid: stat.st_gid,
                    rdev: stat.st_rdev as u32,
                    flags: 0,
                })
            },
            Err(e) => {
                let err = io::Error::from_raw_os_error(e);
                error!("lstat({:?}): {}", path, err);
                Err(err)
            }
        }
    }

    fn backfs_control_file_write(&mut self, data: &[u8], reply: ReplyWrite) {
        // remove a trailing newline if it exists
        let data_trimmed = if data.last() == Some(&0x0A) {
            &data[..data.len() - 1]
        } else {
            data
        };

        let first_space = data_trimmed.iter().position(|x| *x == 0x20).unwrap_or(data_trimmed.len());
        let (command_bytes, arg_bytes) = data_trimmed.split_at(first_space);
        let command = str::from_utf8(command_bytes).unwrap_or("[invalid utf8]");

        let arg_start = if arg_bytes.is_empty() { 0 } else { 1 }; // skip over the space delimiter if there is one
        let arg = OsStr::from_bytes(&arg_bytes[arg_start..]);

        debug!("command: {:?}, arg: {:?}", command, arg);

        match command {
            "test" => {
                reply.error(libc::EXDEV);
                return;
            },
            "noop" => (),
            "invalidate" => {
                self.fscache.invalidate_path(arg);
            },
            "free_orphans" => {
                self.fscache.free_orphaned_buckets();
            },
            _ => {
                reply.error(libc::EBADMSG);
                return;
            }
        }

        reply.written(data.len() as u32);
    }

    fn internal_init(&mut self) -> io::Result<()> {
        println!("BackFS: Initializing cache and scanning existing cache directory...");
        self.inode_table.add(OsString::from("/"));
        self.inode_table.add(OsString::from(BACKFS_CONTROL_FILE_PATH));
        self.inode_table.add(OsString::from(BACKFS_VERSION_FILE_PATH));

        if let Err(e) = self.fscache.init() {
            println!("Error: Failed to initialize cache: {}", e);
            return Err(e);
        }

        let max_cache = if self.settings.cache_size == 0 {
            match self.fscache.max_size() {
                Ok(n) => n,
                Err(e) => {
                    println!("Error: failed to statvfs on the cache filesystem: {}", e);
                    return Err(e);
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

impl Filesystem for BackFS {
    fn init(&mut self, _req: &Request) -> Result<(), libc::c_int> {
        debug!("init");

        if let Err(e) = self.internal_init() {
            println!("Error initializing BackFS: {}", e);
            panic!(e);
        }

        println!("BackFS: Ready.");

        if !self.settings.foreground {
            println!("BackFS: Going to background.");
            if let Err(e) = Daemonize::new().working_directory("/").start() {
                let msg = format!("Error forking to background: {}", e);
                error!("{}", msg);
                panic!(msg);
            }
        }

        Ok(())
    }

    fn destroy(&mut self, _req: &Request) {
        debug!("destroy");
    }

    fn lookup(&mut self, _req: &Request, parent: u64, name: &Path, reply: ReplyEntry) {
        if let Some(parent_path) = self.inode_table.get_path(parent) {

            // Combine the parent path and the name being looked up.
            let pathrc: Rc<OsString> = {
                let mut path = (*parent_path).clone();
                if path.to_str() != Some("/") {
                    path.push(OsStr::new("/"));
                }
                path.push(name.as_os_str());
                Rc::new(path)
            };

            debug!("lookup: {:?}", pathrc);

            match backfs_fake_file_attr((*pathrc).to_str()) {
                Some(attr) => {
                    reply.entry(&TTL, &attr, 0);
                    return;
                }
                None => ()
            }

            match self.stat_real(&pathrc) {
                Ok(attr) => {
                    reply.entry(&TTL, &attr, 0);
                }
                Err(e) => {
                    let msg = format!("lookup: {:?}: {}", pathrc, e);
                    if e.raw_os_error() == Some(libc::ENOENT) {
                        debug!("{}", msg);
                    } else {
                        error!("{}", msg);
                    }
                    reply.error(e.raw_os_error().unwrap_or(libc::EIO));
                }
            }

        } else {
            error!("lookup: could not resolve parent inode {}", parent);
            reply.error(libc::ENOENT);
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        if let Some(path) = self.inode_table.get_path(ino) {
            debug!("getattr: {}: {:?}", ino, path);

            let pathrc = Rc::new(path);

            match backfs_fake_file_attr((*pathrc).to_str()) {
                Some(attr) => {
                    reply.attr(&TTL, &attr);
                    return;
                }
                None => ()
            }

            match self.stat_real(&pathrc) {
                Ok(attr) => {
                    reply.attr(&TTL, &attr);
                },
                Err(e) => {
                    error!("getattr: inode {}, path {:?}: {}", ino, pathrc, e);
                    reply.error(e.raw_os_error().unwrap_or(libc::EIO));
                }
            }
        } else {
            error!("getattr: could not resolve inode {}", ino);
            reply.error(libc::ENOENT);
        }
    }

    fn opendir(&mut self, _req: &Request, ino: u64, _flags: u32, reply: ReplyOpen) {
        if let Some(path) = self.inode_table.get_path(ino) {
            debug!("opendir: {:?}", path);

            let real: OsString = self.real_path(&path);
            debug!("opendir: real = {:?}", real);

            match libc_wrappers::opendir(real) {
                Ok(fh) => reply.opened(fh as u64, 0),
                Err(e) => reply.error(e)
            }
        } else {
            error!("opendir: could not resolve inode {}", ino);
            reply.error(libc::ENOENT);
        }
    }

    fn readdir(&mut self, _req: &Request, ino: u64, fh: u64, offset: u64, mut reply: ReplyDirectory) {
        if let Some(path) = self.inode_table.get_path(ino) {
            debug!("readdir: {:?} @ {}", path, offset);

            if fh == 0 {
                error!("readdir: missing fh");
                return;
            }

            let is_root = ino == 1;
            let mut index = 0u64;

            if offset == 0 {
                let parent_inode = if is_root {
                    ino
                } else {
                    let parent_path = Path::new(path.as_os_str()).parent().unwrap();
                    let parent: OsString = parent_path.to_path_buf().into_os_string();
                    match self.inode_table.get_inode(&parent) {
                        Some(inode) => inode,
                        None => {
                            error!("readdir: unable to get inode for parent of {:?}", path);
                            reply.error(libc::EIO);
                            return;
                        }
                    }
                };

                reply.add(ino, 0, FileType::Directory, ".");
                reply.add(parent_inode, 1, FileType::Directory, "..");
                index += 2;

                if is_root {
                    reply.add(2, 2, FileType::RegularFile, BACKFS_CONTROL_FILE_NAME);
                    reply.add(3, 3, FileType::RegularFile, BACKFS_VERSION_FILE_NAME);
                    index += 2;
                }
            }

            loop {
                match libc_wrappers::readdir(fh as usize) {
                    Ok(Some(entry)) => {
                        let name_c = unsafe { CStr::from_ptr(entry.d_name.as_ptr()) };
                        let name = OsStr::from_bytes(name_c.to_bytes());

                        let pathrc = {
                            let mut entry_path = (*path).clone();
                            if entry_path.to_str() != Some("/") {
                                entry_path.push(OsStr::new("/"));
                            }
                            entry_path.push(name);
                            Rc::new(entry_path)
                        };

                        let inode = self.inode_table.add_or_get(pathrc);
                        let filetype = match entry.d_type {
                            libc::DT_DIR => FileType::Directory,
                            libc::DT_REG => FileType::RegularFile,
                            libc::DT_LNK => FileType::Symlink,
                            libc::DT_BLK => FileType::BlockDevice,
                            libc::DT_CHR => FileType::CharDevice,
                            libc::DT_FIFO => FileType::NamedPipe,
                            libc::DT_SOCK => {
                                warn!("FUSE doesn't support Socket file type; translating to NamedPipe instead.");
                                FileType::NamedPipe
                            },
                            _ => { panic!("unknown file type"); }
                        };

                        debug!("readdir: adding entry {}: {:?} of type {:?}", inode, name, filetype);
                        let buffer_full: bool = reply.add(inode, index, filetype, name);

                        if buffer_full {
                            debug!("readdir: reply buffer is full");
                            break;
                        }

                        index += 1;
                    },
                    Ok(None) => { break; },
                    Err(e) => {
                        error!("readdir: {:?}: {}", path, e);
                        reply.error(e);
                        return;
                    }
                }
            }

            reply.ok();
        } else {
            error!("readdir: could not resolve inode {}", ino);
            reply.error(libc::ENOENT);
        }
    }

    fn releasedir(&mut self, _req: &Request, ino: u64, fh: u64, _flags: u32, reply: ReplyEmpty) {
        if let Some(path) = self.inode_table.get_path(ino) {
            debug!("releasedir: {:?}", path);
            match libc_wrappers::closedir(fh as usize) {
                Ok(()) => { reply.ok(); }
                Err(e) => {
                    error!("closedir({:?}): {}", path, io::Error::from_raw_os_error(e));
                    reply.error(e);
                }
            }
        } else {
            error!("releasedir: could not resolve inode {}", ino);
            reply.error(libc::ENOENT);
        }
    }

    fn open(&mut self, _req: &Request, ino: u64, flags: u32, reply: ReplyOpen) {
        if let Some(path) = self.inode_table.get_path(ino) {
            debug!("open: {:?} flags={:#x}", path, flags);

            if match path.to_str() {
                Some(BACKFS_CONTROL_FILE_PATH) => true,
                Some(BACKFS_VERSION_FILE_PATH) => true,
                _ => false
            } {
                reply.opened(0, flags);
                return;
            }

            let real_path = self.real_path(&path);

            match libc_wrappers::open(real_path, flags as libc::c_int) {
                Ok(fh) => { reply.opened(fh as u64, flags); },
                Err(e) => {
                    error!("open({:?}): {}", path, io::Error::from_raw_os_error(e));
                    reply.error(e);
                }
            }
        } else {
            error!("open: could not resolve inode {}", ino);
            reply.error(libc::ENOENT);
        }
    }

    fn release(&mut self, _req: &Request, ino: u64, fh: u64, _flags: u32, _lock_owner: u64, _flush: bool, reply: ReplyEmpty) {
        if let Some(path) = self.inode_table.get_path(ino) {
            debug!("release: {:?}", path);

            match libc_wrappers::close(fh as usize) {
                Ok(()) => { reply.ok(); },
                Err(e) => {
                    error!("close({:?}): {}", path, io::Error::from_raw_os_error(e));
                    reply.error(e);
                }
            }
        } else {
            error!("release: could not resolve inode {}", ino);
            reply.error(libc::ENOENT);
        }
    }

    fn read(&mut self, _req: &Request, ino: u64, fh: u64, offset: u64, size: u32, reply: ReplyData) {
        if let Some(path) = self.inode_table.get_path(ino) {
            debug!("read: {:?} {:#x} @ {:#x}", path, size, offset);

            let fake_data: Option<&[u8]> = match path.to_str() {
                Some(BACKFS_CONTROL_FILE_PATH) => Some(BACKFS_CONTROL_FILE_HELP.as_bytes()),
                Some(BACKFS_VERSION_FILE_PATH) => Some(BACKFS_VERSION.as_bytes()),
                _ => None
            };

            if let Some(data) = fake_data {
                if offset as usize >= data.len() {
                    // Request out of range; return empty result.
                    reply.data(&[0; 0]);
                } else {
                    let end = cmp::min(data.len(), (offset as usize + size as usize));
                    reply.data(&data[offset as usize .. end]);
                }
                return;
            }

            let mut real_file = unsafe { File::from_raw_fd(fh as libc::c_int) };

            let mtime = match real_file.metadata() {
                Ok(metadata) => metadata.mtime() as i64,
                Err(e) => {
                    error!("unable to get metadata from {:?}: {}", path, e);
                    reply.error(e.raw_os_error().unwrap());
                    return;
                }
            };

            match self.fscache.fetch(&path, offset, size as u64, &mut real_file, mtime) {
                Ok(data) => {
                    reply.data(&data);
                },
                Err(e) => {
                    reply.error(e.raw_os_error().unwrap());
                }
            }

            // Release control of the file descriptor, so it is not closed when this function
            // returns.
            real_file.into_raw_fd();

        } else {
            error!("read: could not resolve inode {}", ino);
            reply.error(libc::ENOENT);
        }
    }

    fn write(&mut self, _req: &Request, ino: u64, _fh: u64, offset: u64, data: &[u8], _flags: u32, reply: ReplyWrite) {
        if let Some(path) = self.inode_table.get_path(ino) {
            debug!("write: {:?} {:#x}@{:#x}", path, data.len(), offset);

            match path.to_str() {
                Some(BACKFS_CONTROL_FILE_PATH) => {
                    self.backfs_control_file_write(data, reply);
                    return;
                },
                Some(BACKFS_VERSION_FILE_PATH) => {
                    reply.error(libc::EACCES);
                    return;
                }
                _ => ()
            }

            if !self.settings.rw {
                reply.error(libc::EROFS);
                return;
            }

            // TODO
            reply.error(libc::ENOSYS);
        } else {
            error!("write: could not resolve inode {}", ino);
            reply.error(libc::ENOENT);
        }
    }

    fn readlink(&mut self, _req: &Request, ino: u64, reply: ReplyData) {
        if let Some(path) = self.inode_table.get_path(ino) {
            debug!("readlink: {:?}", path);

            let real_path = self.real_path(&path);

            match fs::read_link(&real_path) {
                Ok(path) => {
                    reply.data(path.into_os_string().into_vec().as_ref());
                },
                Err(e) => {
                    error!("readlink({:?}): {}", real_path, e);
                    reply.error(e.raw_os_error().unwrap());
                }
            }
        } else {
            error!("readlink: could not resolve inode {}", ino);
            reply.error(libc::ENOENT);
        }
    }

    // TODO: implement the rest of the syscalls needed
}
