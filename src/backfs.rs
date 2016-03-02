// BackFS FUSE Filesystem implementation
//
// Copyright (c) 2016 by William R. Fraser
//

use arg_parse::BackfsSettings;
use inodetable::InodeTable;

use fuse::{Filesystem, Request};
use libc::*;

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
}

impl<'a> Filesystem for BackFS<'a> {    
    fn init(&mut self, _req: &Request) -> Result<(), c_int> {
        println!("init!");
        Ok(())
    }
    
    // TODO: implement the rest of the syscalls needed
}