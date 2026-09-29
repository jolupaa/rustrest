//! Internal diagnostics. With the `tracing` feature events go to `tracing`
//! (target `rustrest`); without it, errors are written to stderr and
//! per-connection debug noise is dropped so clients cannot flood the logs.

macro_rules! log_error {
    ($($arg:tt)*) => {{
        #[cfg(feature = "tracing")]
        ::tracing::error!(target: "rustrest", $($arg)*);
        #[cfg(not(feature = "tracing"))]
        eprintln!($($arg)*);
    }};
}

macro_rules! log_debug {
    ($($arg:tt)*) => {{
        #[cfg(feature = "tracing")]
        ::tracing::debug!(target: "rustrest", $($arg)*);
        #[cfg(not(feature = "tracing"))]
        {
            let _ = format_args!($($arg)*);
        }
    }};
}

pub(crate) use {log_debug, log_error};

/// Connection-level failures that are a normal part of serving clients
/// (disconnects mid-message, resets, timeouts) rather than server faults.
pub(crate) fn is_routine_connection_error(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut current = Some(error);
    while let Some(error) = current {
        if let Some(error) = error.downcast_ref::<hyper::Error>() {
            if error.is_incomplete_message()
                || error.is_timeout()
                || error.is_canceled()
                || error.is_closed()
            {
                return true;
            }
        }
        if let Some(error) = error.downcast_ref::<std::io::Error>() {
            if matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::UnexpectedEof
                    | std::io::ErrorKind::TimedOut
            ) {
                return true;
            }
        }
        current = error.source();
    }
    false
}
