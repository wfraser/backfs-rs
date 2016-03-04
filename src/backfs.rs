// BackFS FUSE Filesystem implementation
//
// Copyright (c) 2016 by William R. Fraser
//

use std::cmp;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs;
use std::fs::File;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::path::Path;
use std::rc::Rc;
use std::str;

use arg_parse::BackfsSettings;
use inodetable::InodeTable;
use fscache::FSCache;

use fuse::{FileType, FileAttr, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry, ReplyWrite, Request};
use libc::*;
use time::Timespec;

pub const BACKFS_VERSION: &'static str = "BackFS version: 0.1.0\n";

const TTL: Timespec = Timespec { sec: 1, nsec: 0 };

// inode 2:
const BACKFS_CONTROL_FILE_NAME: &'static str = ".backfs_control";
const BACKFS_CONTROL_FILE_PATH: &'static str = "/.backfs_control";

// inode 3:
const BACKFS_VERSION_FILE_NAME: &'static str = ".backfs_version";
const BACKFS_VERSION_FILE_PATH: &'static str = "/.backfs_version";

const BACKFS_CONTROL_FILE_HELP: &'static str = "commands: test, noop\n";

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

pub struct BackFS<'a> {
    pub settings: BackfsSettings<'a>,
    inode_table: InodeTable,
    fscache: FSCache,
}

macro_rules! log {
    ($s:expr, $fmt:expr) => ($s.log(format_args!($fmt)));
    ($s:expr, $fmt:expr, $($arg:tt)*) => ($s.log(format_args!($fmt, $($arg)*)));
}

fn fuse_file_type(ft: &fs::FileType) -> FileType {
    if ft.is_dir() {
        FileType::Directory
    } else if ft.is_file() {
        FileType::RegularFile
    } else if ft.is_symlink() {
        FileType::Symlink
    } else if ft.is_block_device() {
        FileType::BlockDevice
    } else if ft.is_char_device() {
        FileType::CharDevice
    } else if ft.is_fifo() {
        FileType::NamedPipe
    } else if ft.is_socket() {
        // ???
        FileType::NamedPipe
    } else {
        panic!("unknown file type");
    }
}

fn backfs_fake_file_attr(path: Option<&str>) -> Option<FileAttr> {
    match path {
        Some(BACKFS_CONTROL_FILE_PATH) => {
            let mut attr = BACKFS_FAKE_FILE_ATTRS.clone();
            attr.ino = 2;
            attr.perm = 0o600;
            attr.size = BACKFS_CONTROL_FILE_HELP.as_bytes().len() as u64;
            Some(attr)
        },
        Some(BACKFS_VERSION_FILE_PATH) => {
            let mut attr = BACKFS_FAKE_FILE_ATTRS.clone();
            attr.ino = 3;
            attr.perm = 0o444;
            attr.size = BACKFS_VERSION.as_bytes().len() as u64;
            Some(attr)
        },
        _ => None
    }
}

impl<'a> BackFS<'a> {
    pub fn new(settings: BackfsSettings<'a>) -> BackFS<'a> {
        let mut backfs = BackFS {
            fscache: FSCache::new(&settings.cache, settings.block_size as u64),
            settings: settings,
            inode_table: InodeTable::new(),
        };
        if backfs.settings.verbose {
            backfs.fscache.debug = true;
        }
        backfs
    }

    fn log(&self, args: fmt::Arguments) {
        if self.settings.verbose {
            println!("BackFS: {}", fmt::format(args));
        }
    }

    fn real_path(&self, partial: &OsString) -> OsString {
        let mut path = OsString::from(self.settings.backing_fs);
        path.push(partial);
        path
    }

    fn stat_real(&mut self, path: &Rc<OsString>) -> io::Result<FileAttr> {
        let real: OsString = self.real_path(&path);
        log!(self, "stat_real: {}", real.to_string_lossy());

        let metadata = try!(fs::metadata(Path::new(&real)));

        let inode = self.inode_table.add_or_get(path.clone());

        Ok(FileAttr {
            ino: inode,
            size: metadata.len(),
            blocks: metadata.blocks() as u64,
            atime: Timespec { sec: metadata.atime(), nsec: metadata.atime_nsec() as i32 },
            mtime: Timespec { sec: metadata.mtime(), nsec: metadata.mtime_nsec() as i32 },
            ctime: Timespec { sec: metadata.ctime(), nsec: metadata.ctime_nsec() as i32 },
            crtime: Timespec { sec: 0, nsec: 0 },
            kind: fuse_file_type(&metadata.file_type()),
            perm: metadata.mode() as u16,
            nlink: metadata.nlink() as u32,
            uid: metadata.uid(),
            gid: metadata.gid(),
            rdev: metadata.rdev() as u32,
            flags: 0,
        })
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

        log!(self, "command: {:?}, arg: {:?}", command, arg);

        match command {
            "test" => {
                reply.error(EXDEV);
                return;
            },
            "noop" => (),
            "invalidate" => {
                self.fscache.invalidate_path(arg);
            },
            //TODO: "free_orphans"
            _ => {
                reply.error(EBADMSG);
                return;
            }
        }

        reply.written(data.len() as u32);
    }
}

impl<'a> Filesystem for BackFS<'a> {
    fn init(&mut self, _req: &Request) -> Result<(), c_int> {
        log!(self, "init");
        self.inode_table.add(OsString::from("/"));
        self.inode_table.add(OsString::from(BACKFS_CONTROL_FILE_PATH));
        self.inode_table.add(OsString::from(BACKFS_VERSION_FILE_PATH));
        Ok(())
    }

    fn destroy(&mut self, _req: &Request) {
        log!(self, "destroy");
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

            log!(self, "lookup: {}", pathrc.to_string_lossy());

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
                    log!(self, "error: lookup: {}: {:?}", pathrc.to_string_lossy(), e);
                    reply.error(e.raw_os_error().unwrap_or(EIO));
                }
            }

        } else {
            log!(self, "error: lookup: could not resolve parent inode {}", parent);
            reply.error(ENOENT);
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        if let Some(path) = self.inode_table.get_path(ino) {
            log!(self, "getattr: {}: {}", ino, path.to_string_lossy());

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
                    log!(self, "error: getattr: inode {}, path {}: {:?}", ino, pathrc.to_string_lossy(), e);
                    reply.error(e.raw_os_error().unwrap_or(EIO));
                }
            }
        } else {
            log!(self, "error: getattr: could not resolve inode {}", ino);
            reply.error(ENOENT);
        }
    }

    fn readdir(&mut self, _req: &Request, ino: u64, _fh: u64, offset: u64, mut reply: ReplyDirectory) {
        if offset != 0 {
            reply.ok();
            return;
        }

        if let Some(path) = self.inode_table.get_path(ino) {
            log!(self, "readdir: {}", path.to_string_lossy());

            let is_root = ino == 1;

            let parent_inode = if is_root {
                ino
            } else {
                let parent_path = Path::new(path.as_os_str()).parent().unwrap();
                let parent: OsString = parent_path.to_path_buf().into_os_string();
                log!(self, "readdir: parent of {} is {}", path.to_string_lossy(), parent.to_string_lossy());
                match self.inode_table.get_inode(&parent) {
                    Some(inode) => inode,
                    None => {
                        log!(self, "error: readdir: unable to get inode for parent of {}", path.to_string_lossy());
                        reply.error(EIO);
                        return;
                    }
                }
            };

            let real = self.real_path(&path);
            log!(self, "readdir: real = {}", real.to_string_lossy());

            match fs::read_dir(real) {
                Ok(entries) => {
                    reply.add(ino, 0, FileType::Directory, ".");
                    reply.add(parent_inode, 1, FileType::Directory, "..");
                    let mut index = 2u64;

                    if is_root {
                        reply.add(2, 2, FileType::RegularFile, BACKFS_CONTROL_FILE_NAME);
                        reply.add(3, 3, FileType::RegularFile, BACKFS_VERSION_FILE_NAME);
                        index += 2;
                    }

                    for entry_result in entries {
                        match entry_result {
                            Ok(entry) => {
                                let name: OsString = entry.file_name();

                                // Combine the our path and entry.
                                let pathrc = {
                                    let mut entry_path = (*path).clone();
                                    if entry_path.to_str() != Some("/") {
                                        entry_path.push(OsStr::new("/"));
                                    }
                                    entry_path.push(name.as_os_str());
                                    Rc::new(entry_path)
                                };

                                let inode = self.inode_table.add_or_get(pathrc);
                                let filetype = fuse_file_type(&entry.file_type().unwrap());

                                log!(self, "readdir: adding entry {}: {} of type {:?}", inode, name.to_string_lossy(), filetype);
                                let buffer_full: bool = reply.add(inode, index, filetype, name);
                                index += 1;

                                if buffer_full {
                                    // resize the buffer
                                    reply = reply.sized((index * 2) as usize);
                                }
                            },
                            Err(e) => {
                                log!(self, "error: readdir: {:?}", e);
                            }
                        }
                    }
                    reply.ok();
                },
                Err(e) => {
                    log!(self, "error: readdir: {}: {:?}", path.to_string_lossy(), e);
                    reply.error(e.raw_os_error().unwrap_or(EIO));
                }
            }

        } else {
            log!(self, "error: readdir: could not resolve inode {}", ino);
            reply.error(ENOENT);
        }
    }

    fn read(&mut self, _req: &Request, ino: u64, _fh: u64, offset: u64, size: u32, reply: ReplyData) {
        if let Some(path) = self.inode_table.get_path(ino) {
            log!(self, "read: {} {}@{}", path.to_string_lossy(), size, offset);

            match path.to_str() {
                Some(BACKFS_CONTROL_FILE_PATH) => {
                    let data = BACKFS_CONTROL_FILE_HELP.as_bytes();
                    let end = cmp::min(data.len(), (size as usize - offset as usize));
                    reply.data(&data[offset as usize .. end]);
                    return;
                },
                Some(BACKFS_VERSION_FILE_PATH) => {
                    let data: &[u8] = BACKFS_VERSION.as_bytes();
                    let end = cmp::min(data.len(), (size as usize - offset as usize));
                    reply.data(&data[offset as usize .. end]);
                    return;
                },
                _ => ()
            }

            let real_path = self.real_path(&path);
            let mut real_file: File;

            match File::open(Path::new(&real_path)) {
                Ok(f) => { real_file = f; },
                Err(e) => {
                    reply.error(e.raw_os_error().unwrap());
                    return;
                }
            }

            match self.fscache.fetch(&path, offset, size as u64, &mut real_file) {
                Ok(data) => {
                    reply.data(&data);
                },
                Err(e) => {
                    reply.error(e.raw_os_error().unwrap());
                }
            }

        } else {
            log!(self, "error: read: could not resolve inode {}", ino);
            reply.error(ENOENT);
        }
    }

    fn write(&mut self, _req: &Request, ino: u64, _fh: u64, offset: u64, data: &[u8], _flags: u32, reply: ReplyWrite) {
        if let Some(path) = self.inode_table.get_path(ino) {
            log!(self, "write: {} {}@{}", path.to_string_lossy(), data.len(), offset);

            match path.to_str() {
                Some(BACKFS_CONTROL_FILE_PATH) => {
                    self.backfs_control_file_write(data, reply);
                    return;
                },
                Some(BACKFS_VERSION_FILE_PATH) => {
                    reply.error(EACCES);
                    return;
                }
                _ => ()
            }

            // TODO
            reply.error(EHWPOISON);
        } else {
            log!(self, "error: write: could not resolve inode {}", ino);
            reply.error(ENOENT);
        }
    }

    // TODO: implement the rest of the syscalls needed
}
