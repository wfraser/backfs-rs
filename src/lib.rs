// BackFS Library Crate Module Definitions and External Dependency List
//
// Copyright (c) 2016 by William R. Fraser
//

pub mod arg_parse;
pub mod backfs;
pub mod bucket_store;
pub mod block_map;
pub mod fscache;
pub mod fsll;
mod libc_wrappers;
mod link;
mod utils;
mod osstrextras;

extern crate daemonize;
extern crate fuse_mt;
extern crate libc;
extern crate time;
extern crate walkdir;

#[macro_use]
extern crate log;

// This env variable is set by Cargo
pub const VERSION: &'static str = env!("CARGO_PKG_VERSION");

// This file is produced by build.rs
pub const GIT_REVISION: &'static str = include_str!(concat!(env!("OUT_DIR"), "/git_rev.txt"));

pub use backfs::BackFS;
