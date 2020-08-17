// BackFS Log Output
//
// Copyright 2016-2020 by William R. Fraser
//

use std::boxed::Box;
use syslog::{Facility, Logger, Severity};

struct Log {
    global_filter: log::LevelFilter,
    target_filter: Vec<(String, log::LevelFilter)>,
    syslog: Option<Box<Logger>>,
}

pub fn init(global_filter: log::LevelFilter,
            target_filter: Vec<(String, log::LevelFilter)>,
            use_syslog: bool)
    -> Result<(), log::SetLoggerError>
{
    log::set_max_level(global_filter);

    let syslog = if use_syslog {
        match syslog::unix(Facility::LOG_USER) {
            Ok(writer) => Some(writer),
            Err(e) => {
                println!("Error opening connection to syslog: {}", e);
                println!("Logging disabled!");
                None
            }
        }
    } else {
        None
    };

    log::set_boxed_logger(Box::new(Log {
        global_filter,
        target_filter,
        syslog,
    }))
}

fn loglevel_to_syslog_severity(level: log::Level) -> Severity {
    #[allow(clippy::match_same_arms)]
    match level {
        log::Level::Error => Severity::LOG_ERR,
        log::Level::Warn  => Severity::LOG_WARNING,
        log::Level::Info  => Severity::LOG_INFO,
        log::Level::Debug => Severity::LOG_DEBUG,
        log::Level::Trace => Severity::LOG_DEBUG,
    }
}

impl log::Log for Log {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        if self.global_filter < metadata.level() {
            return false;
        }

        for &(ref target, ref filter) in &self.target_filter {
            if metadata.target() == target && *filter < metadata.level() {
                return false;
            }
        }

        true
    }

    fn log(&self, record: &log::Record<'_>) {
        if self.enabled(record.metadata()) {
            let msg = format!("backfs: {}: {}: {}", record.target(), record.level(), record.args());
            if let Some(ref syslog) = self.syslog {
                let severity = loglevel_to_syslog_severity(record.level());
                let _errors_ignored = syslog.send(severity, &msg);
            } else {
                println!("{}", msg);
            }
        }
    }

    fn flush(&self) {}
}
