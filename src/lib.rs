// BackFS Library Crate Module Definitions and External Dependency List
//
// Copyright (c) 2016 by William R. Fraser
//

extern crate daemonize;
extern crate fuse_mt;
extern crate libc;
#[macro_use] extern crate log;
extern crate time;
extern crate walkdir;

pub mod arg_parse;
pub mod backfs;
pub mod bucket_store;
pub mod block_map;
pub mod fscache;
pub mod fsll;
pub mod osstrextras; // useful for test code
mod libc_wrappers;
mod link;
mod utils;

// This env variable is set by Cargo
pub const VERSION: &'static str = env!("CARGO_PKG_VERSION");

// These files are produced by build.rs
pub const GIT_REVISION: &'static str = include_str!(concat!(env!("OUT_DIR"), "/git_rev.txt"));
pub const BUILD_TIME: i64 = include!(concat!(env!("OUT_DIR"), "/build_time.txt"));

pub use backfs::BackFS;
