// BackFS Library Crate Module Definitions and External Dependency List
//
// Copyright 2016-2020 by William R. Fraser
//

#![deny(rust_2018_idioms)]

#[macro_use] extern crate log;

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
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

// These files are produced by build.rs
pub const GIT_REVISION: &str = include_str!(concat!(env!("OUT_DIR"), "/git_rev.txt"));

#[allow(clippy::unreadable_literal)]
pub const BUILD_TIME: i64 = include!(concat!(env!("OUT_DIR"), "/build_time.txt"));

pub use crate::backfs::BackFS;
