// BackFS FUSE Filesystem implementation
//
// Copyright (c) 2016 by William R. Fraser
//

use std::ffi::{OsStr, OsString};
use std::path::Path;

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
}

impl<'a> Filesystem for BackFS<'a> {    
    fn init(&mut self, _req: &Request) -> Result<(), c_int> {
        self.log(format!("init"));
        self.inode_table.add(OsString::from("/"));
        self.inode_table.add(OsString::from("/foo"));
        Ok(())
    }

    fn lookup(&mut self, _req: &Request, parent: u64, name: &Path, reply: ReplyEntry) {
        if let Some(parent_path) = self.inode_table.get_path(parent) {

            // Combine the parent path and the name being looked up.
            let mut path = (*parent_path).clone();
            if path.to_str() != Some("/") {
                path.push(OsStr::new("/"));
            }
            path.push(name.as_os_str());

            self.log(format!("lookup: {}", path.to_string_lossy()));

            if let Some(inode) = self.inode_table.get_inode(&path) {

                // TODO

                match path.to_str() {
                    Some("/foo") => {
                        let bogus_time = Timespec { sec: 915177600, nsec: 0 }; // 1999-01-01 0:00
                        let attr = FileAttr {
                            ino: inode,
                            size: 0,
                            blocks: 0,
                            atime: bogus_time,
                            mtime: bogus_time,
                            ctime: bogus_time,
                            crtime: bogus_time,
                            kind: FileType::RegularFile,
                            perm: 0o000,
                            nlink: 1,
                            uid: 0,
                            gid: 0,
                            rdev: 0,
                            flags: 0,
                        };
                        reply.entry(&TTL, &attr, 0);
                    },
                    _ => {
                        self.log(format!("error: lookup: unexpected path: {}", path.to_string_lossy()));
                        reply.error(EIO);
                    }
                }
            } else {
                self.log(format!("error: lookup: could not find inode for path {}", path.to_string_lossy()));
                reply.error(ENOENT);
            }
        } else {
            self.log(format!("error: lookup: could not resolve parent inode {}", parent));
            reply.error(ENOENT);
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        if let Some(path) = self.inode_table.get_path(ino) {
            self.log(format!("getattr: {}: {}", ino, path.to_string_lossy()));

            // TODO

            match path.to_str() {
                Some("/") => {
                    let bogus_time = Timespec { sec: 915177600, nsec: 0 };
                    let attr = FileAttr {
                        ino: 1,
                        size: 0,
                        blocks: 0,
                        atime: bogus_time,
                        mtime: bogus_time,
                        ctime: bogus_time,
                        crtime: bogus_time,
                        kind: FileType::Directory,
                        perm: 0o555,
                        nlink: 2,
                        uid: 0,
                        gid: 0,
                        rdev: 0,
                        flags: 0,
                    };

                    reply.attr(&TTL, &attr);
                },
                _ => {
                    self.log(format!("error: getattr: unexpected inode {}, path {}", ino, path.to_string_lossy()));
                    reply.error(EIO);
                }
            }
        } else {
            self.log(format!("error: getattr: could not resolve inode {}", ino));
            reply.error(ENOENT);
        }
    }

    fn readdir(&mut self, _req: &Request, ino: u64, _fh: u64, offset: u64, mut reply: ReplyDirectory) {
        if let Some(path) = self.inode_table.get_path(ino) {
            self.log(format!("readdir: {}", path.to_string_lossy()));

            // TODO

            match path.to_str() {
                Some("/") => {
                    if offset == 0 {
                        reply.add(ino, 0, FileType::Directory, ".");
                        reply.add(ino, 1, FileType::Directory, "..");
                        reply.add(2,   2, FileType::RegularFile, "foo");
                    }
                    reply.ok();
                },
                _ => {
                    self.log(format!("error: readdir: unexpected inode {}, path {}", ino, path.to_string_lossy()));
                    reply.error(EIO);
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
            
            match path.to_str() {
                Some("/foo") => {
                    reply.error(EHWPOISON); // "Memory page has hardware error" lol
                },
                _ => {
                    self.log(format!("error: read: unexpected inode {}, path {}", ino, path.to_string_lossy()));
                    reply.error(EIO);
                }
            }
        } else {
            self.log(format!("error: read: could not resolve inode {}", ino));
            reply.error(ENOENT);
        }
    }
    
    // TODO: implement the rest of the syscalls needed
}
