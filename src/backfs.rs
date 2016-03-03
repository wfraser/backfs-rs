// BackFS FUSE Filesystem implementation
//
// Copyright (c) 2016 by William R. Fraser
//

use std::ffi::{OsStr, OsString};
use std::fs;
use std::io;
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::path::Path;
use std::rc::Rc;

use arg_parse::BackfsSettings;
use inodetable::InodeTable;

use fuse::{FileType, FileAttr, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry, Request};
use libc::*;
use time::Timespec;

const TTL: Timespec = Timespec { sec: 1, nsec: 0 };

pub struct BackFS<'a> {
    pub settings: BackfsSettings<'a>,
    inode_table: InodeTable
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

impl<'a> BackFS<'a> {
    pub fn new(settings: BackfsSettings<'a>) -> BackFS<'a> {
        BackFS {
            settings: settings,
            inode_table: InodeTable::new()
        }
    }

    fn log(&self, s: String) {
        if self.settings.verbose {
            println!("BackFS: {}", s);
        }
    }

    fn real_path(&self, partial: &OsString) -> OsString {
        let mut path = OsString::from(self.settings.backing_fs);
        path.push(partial);
        path
    }

    fn stat_real(&mut self, path: &Rc<OsString>) -> io::Result<FileAttr> {
        let real: OsString = self.real_path(&path);
        self.log(format!("stat_real: {}", real.to_string_lossy()));

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
}

impl<'a> Filesystem for BackFS<'a> {    
    fn init(&mut self, _req: &Request) -> Result<(), c_int> {
        self.log(format!("init"));
        self.inode_table.add(OsString::from("/"));
        Ok(())
    }

    fn destroy(&mut self, _req: &Request) {
        self.log(format!("destroy"));
    }

    fn lookup(&mut self, _req: &Request, parent: u64, name: &Path, reply: ReplyEntry) {
        if let Some(parent_path) = self.inode_table.get_path(parent) {

            // Combine the parent path and the name being looked up.
            let pathrc = {
                let mut path = (*parent_path).clone();
                if path.to_str() != Some("/") {
                    path.push(OsStr::new("/"));
                }
                path.push(name.as_os_str());
                Rc::new(path)
            };

            self.log(format!("lookup: {}", pathrc.to_string_lossy()));

            match self.stat_real(&pathrc) {
                Ok(attr) => {
                    reply.entry(&TTL, &attr, 0);
                }
                Err(e) => {
                    self.log(format!("error: lookup: {}: {:?}", pathrc.to_string_lossy(), e));
                    reply.error(e.raw_os_error().unwrap_or(EIO));
                }
            }

        } else {
            self.log(format!("error: lookup: could not resolve parent inode {}", parent));
            reply.error(ENOENT);
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        if let Some(path) = self.inode_table.get_path(ino) {
            self.log(format!("getattr: {}: {}", ino, path.to_string_lossy()));

            let pathrc = Rc::new(path);
            match self.stat_real(&pathrc) {
                Ok(attr) => {
                    reply.attr(&TTL, &attr);
                },
                Err(e) => {
                    self.log(format!("error: getattr: inode {}, path {}: {:?}", ino, pathrc.to_string_lossy(), e));
                    reply.error(e.raw_os_error().unwrap_or(EIO));
                }
            }
        } else {
            self.log(format!("error: getattr: could not resolve inode {}", ino));
            reply.error(ENOENT);
        }
    }

    fn readdir(&mut self, _req: &Request, ino: u64, _fh: u64, offset: u64, mut reply: ReplyDirectory) {
        if offset != 0 {
            reply.ok();
            return;
        }

        if let Some(path) = self.inode_table.get_path(ino) {
            self.log(format!("readdir: {}", path.to_string_lossy()));

            let parent_inode = if path.as_os_str() == OsStr::new("/") {
                ino
            } else {
                let parent_path = Path::new(path.as_os_str()).parent().unwrap();
                let parent: OsString = parent_path.to_path_buf().into_os_string();
                self.log(format!("readdir: parent of {} is {}", path.to_string_lossy(), parent.to_string_lossy()));
                match self.inode_table.get_inode(&parent) {
                    Some(inode) => inode,
                    None => {
                        self.log(format!("error: readdir: unable to get inode for parent of {}", path.to_string_lossy()));
                        reply.error(EIO);
                        return;
                    }
                }
            };

            let real = self.real_path(&path);
            self.log(format!("readdir: real = {}", real.to_string_lossy()));

            match fs::read_dir(real) {
                Ok(entries) => {
                    reply.add(ino, 0, FileType::Directory, ".");
                    reply.add(parent_inode, 1, FileType::Directory, "..");
                    let mut index = 2u64;
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

                                self.log(format!("readdir: adding entry {}: {} of type {:?}", inode, name.to_string_lossy(), filetype));
                                let buffer_full: bool = reply.add(inode, index, filetype, name);
                                index += 1;

                                if buffer_full {
                                    // resize the buffer
                                    reply = reply.sized((index * 2) as usize);
                                }
                            },
                            Err(e) => {
                                self.log(format!("error: readdir: {:?}", e));
                            }
                        }
                    }
                    reply.ok();
                },
                Err(e) => {
                    self.log(format!("error: readdir: {}: {:?}", path.to_string_lossy(), e));
                    reply.error(e.raw_os_error().unwrap_or(EIO));
                }
            }

        } else {
            self.log(format!("error: readdir: could not resolve inode {}", ino));
            reply.error(ENOENT);
        }
    }

    fn read(&mut self, _req: &Request, ino: u64, _fh: u64, offset: u64, size: u32, reply: ReplyData) {
        if let Some(path) = self.inode_table.get_path(ino) {
            self.log(format!("read: {} {}@{}", path.to_string_lossy(), size, offset));

            // TODO
            reply.error(EHWPOISON); // "Memory page has hardware error" lol

        } else {
            self.log(format!("error: read: could not resolve inode {}", ino));
            reply.error(ENOENT);
        }
    }
    
    // TODO: implement the rest of the syscalls needed
}
