// BackFS Log Output
//
// Copyright (c) 2016 by William R. Fraser
//

use std::boxed::Box;
use log;

struct Log {
    global_filter: log::LogLevelFilter,
    target_filter: Vec<(String, log::LogLevelFilter)>,
}

pub fn init(global_filter: log::LogLevelFilter,
            target_filter: Vec<(String, log::LogLevelFilter)>)
    -> Result<(), log::SetLoggerError> {
    log::set_logger(|max_log_level| {
        max_log_level.set(global_filter);
        Box::new(Log {
            global_filter: global_filter,
            target_filter: target_filter,
        })
    })
}

impl log::Log for Log {
    fn enabled(&self, metadata: &log::LogMetadata) -> bool {
        //self.global_filter >= metadata.level()
        if self.global_filter < metadata.level() {
            return false;
        }

        for &(ref target, ref filter) in &self.target_filter {
            if metadata.target() == target {
                if *filter < metadata.level() {
                    return false;
                }
            }
        }

        true
    }

    fn log(&self, record: &log::LogRecord) {
        if self.enabled(record.metadata()) {
            // TODO: write to syslog when we are in the background.
            println!("{}: {}: {}", record.target(), record.level(), record.args());
        }
    }
}
