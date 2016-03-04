// BackFS command-line argument parsing
//
// Copyright (c) 2016 by William R. Fraser
//
// This is hand-rolled parsing code because none of the available command-line parsing crates I
// found could cope with the odd 'mount'-style arguments this uses (i.e. -o foo, -o bar).
// 

pub const USAGE: &'static str = "
BackFS.

Usage:
    backfs [-o <option,[option]>] <backing> <mount point>
    
General Options:
    -h --help         Show this help.
    -V --version      Show the program version.

BackFS Options:
    -o cache            Cache location (REQUIRED)
    -o backing_fs       Backing filesystem location (REQUIRED here or 
                            as the first non-option argument)
    -o cache_size       Maximum size for the cache (default is for the cache to
                            grow to fill the device it is on)
    -o rw               Be a read-write cache (default is read-only)
    -o block_size       Cache block size. Defaults to 128K
    -v --verbose        Enable all debugging messages
       -o verbose
    -f --foreground     Enable foreground operation.
       -o foreground
    -d --debug          Enable debugging mode. Same as specifying -v -f.
       -o debug
";

#[derive(Debug)]
pub struct BackfsSettings<'a> {
    pub mount_point: &'a str,
    pub fuse_options: Vec<&'a str>,
    pub help: bool,
    pub version: bool,
    pub cache: &'a str,
    pub backing_fs: &'a str,
    pub cache_size: u64,
    pub rw: bool,
    pub block_size: u32,
    pub foreground: bool,
    pub verbose: bool,
}

impl<'a> BackfsSettings<'a> {
    pub fn parse(args: &'a Vec<String>) -> BackfsSettings<'a> {
        
        // These are the default settings:
        let mut settings = BackfsSettings {
            mount_point: "",
            fuse_options: vec![],
            help: false,
            version: false,
            cache: "",
            backing_fs: "",
            cache_size: 0,
            rw: false,
            block_size: 0x20_000,   // 131072 = 128 KiB
            foreground: false,
            verbose: false
        };
        
        let mut options: Vec<&'a str> = vec![];
        let mut values: Vec<&'a str> = vec![];
        let mut is_opt = false;
        let mut parsing_options = true;
        let mut first = true;
        for arg in args {
            // skip the program name
            if first {
                first = false;
                continue;
            }
            
            if parsing_options {
                if arg == "-o" {
                    is_opt = true;
                    continue;
                }
                
                if is_opt {
                    for option in arg.split(',') {
                        options.push(option);
                    }
                    is_opt = false;
                } else {
                    if arg == "--" {
                        parsing_options = false;
                    } else if arg == "-h" || arg == "--help" {
                        options.push("help");
                    } else if arg == "-V" || arg == "--version" {
                        options.push("version");
                    } else if arg == "-v" || arg == "--verbose" {
                        options.push("verbose");
                    } else if arg == "-f" || arg == "--foreground" {
                        options.push("foreground");
                    } else if arg == "-d" || arg == "--debug" {
                        options.push("foreground");
                        options.push("verbose");
                    } else if arg.starts_with("-") {
                        println!("unrecognized option \"{}\"", arg);
                        options.push("help");
                        break;
                    } else {
                        parsing_options = false;
                        values.push(arg);
                    }
                }
            } else {
                values.push(arg);
            }
        }
        
        // now interpret the options and values
        for opt in options {
            let parts = opt.splitn(2, '=').collect::<Vec<&str>>();
            if parts.len() == 2 {
                match parts[0] {
                    "cache" => settings.cache = parts[1],
                    "backing_fs" => settings.backing_fs = parts[1],
                    "cache_size" => match parts[1].parse::<u64>() {
                        Ok(n) => { settings.cache_size = n; },
                        Err(e) => { println!("invalid cache size: {}", e); }  
                    },
                    "block_size" => match parts[1].parse::<u32>() {
                        Ok(n) => { settings.block_size = n; },
                        Err(e) => { println!("invalid block size: {}", e); }  
                    },
                    _ => settings.fuse_options.push(parts[1])
                }
            } else {
                match opt {
                    "help" => settings.help = true,
                    "version" => settings.version = true,
                    "rw" => settings.rw = true,
                    "verbose" => settings.verbose = true,
                    "foreground" => settings.foreground = true,
                    _ => settings.fuse_options.push(&opt)
                }
            }
        }
        
        match values.len() {
            1 => {
                if settings.backing_fs.is_empty() {
                    println!("required backing filesystem argument not specified!");
                    settings.help = true;
                } else {
                    settings.mount_point = values[0]
                }
            },
            2 => {
                settings.backing_fs = values[0];
                settings.mount_point = values[1];
            },
            _ => {
                if !settings.help && !settings.version {
                    println!("invalid number of non-option arguments given.");
                    settings.help = true;
                }
            }
        }
        
        settings
    }
}
