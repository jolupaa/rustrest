use std::error::Error;
use std::fmt::{Display, Formatter};
use std::pin::Pin;

use futures_util::{Stream, StreamExt, future};
use http_body_util::BodyExt;
use hyper::body::{Body as _, Bytes, Incoming, SizeHint};

use super::{BoxError, HttpError};

pub(crate) const DEFAULT_BODY_LIMIT: usize = 64 * 1024;

/// A one-shot stream of request body chunks.
pub type BodyStream = Pin<Box<dyn Stream<Item = Result<Bytes, BoxError>> + Send>>;

/// The request body handed to middleware and route handlers.
///
/// Incoming bodies are consumed once. A body that has been collected
/// successfully is cached, so later buffered reads return the same bytes.
pub struct RequestBody {
    state: BodyState,
    default_limit: usize,
}

enum BodyState {
    Incoming(Incoming),
    Buffered(Bytes),
    Stream(BodyStream),
    Taken,
}

#[derive(Debug)]
pub(crate) struct BodyLimitExceeded {
    limit: usize,
}

impl BodyLimitExceeded {
    pub(crate) fn limit(&self) -> usize {
        self.limit
    }
}

impl Display for BodyLimitExceeded {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "el cuerpo de la solicitud supero su limite configurado de {} bytes",
            self.limit
        )
    }
}

impl Error for BodyLimitExceeded {}

impl RequestBody {
    pub(crate) fn incoming(body: Incoming, default_limit: usize) -> Self {
        Self {
            state: BodyState::Incoming(body),
            default_limit,
        }
    }

    /// Creates a repeatably collectable in-memory body.
    pub fn buffered(body: impl Into<Bytes>, default_limit: usize) -> Self {
        Self {
            state: BodyState::Buffered(body.into()),
            default_limit,
        }
    }

    /// Creates a request body from an arbitrary fallible byte stream.
    pub fn from_stream<S, E>(stream: S, default_limit: usize) -> Self
    where
        S: Stream<Item = Result<Bytes, E>> + Send + 'static,
        E: Error + Send + Sync + 'static,
    {
        let stream = stream.map(|chunk| chunk.map_err(|error| Box::new(error) as BoxError));
        Self {
            state: BodyState::Stream(Box::pin(stream)),
            default_limit,
        }
    }

    /// Returns the transport's best known body size.
    pub fn size_hint(&self) -> SizeHint {
        match &self.state {
            BodyState::Incoming(body) => body.size_hint(),
            BodyState::Buffered(bytes) => SizeHint::with_exact(bytes.len() as u64),
            BodyState::Stream(_) | BodyState::Taken => SizeHint::default(),
        }
    }

    pub(crate) fn buffered_len(&self) -> Option<usize> {
        match &self.state {
            BodyState::Buffered(bytes) => Some(bytes.len()),
            BodyState::Incoming(_) | BodyState::Stream(_) | BodyState::Taken => None,
        }
    }

    pub(crate) fn set_default_limit(&mut self, limit: usize) {
        self.default_limit = limit;
    }

    /// Collects the body while enforcing the smaller of `limit` and the hard
    /// application/route limit configured on this body.
    ///
    /// Callers may choose a stricter local limit, but cannot use this method
    /// to bypass the server's configured maximum.
    pub async fn collect(&mut self, limit: usize) -> Result<Bytes, HttpError> {
        let limit = limit.min(self.default_limit);
        if let BodyState::Buffered(bytes) = &self.state {
            if bytes.len() > limit {
                return Err(HttpError::payload_too_large_limit(limit));
            }
            return Ok(bytes.clone());
        }

        let mut stream = self.take_raw_stream()?;
        let mut output = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(HttpError::body_read)?;
            let new_len = output
                .len()
                .checked_add(chunk.len())
                .filter(|length| *length <= limit)
                .ok_or_else(|| HttpError::payload_too_large_limit(limit))?;
            output.reserve(new_len - output.len());
            output.extend_from_slice(&chunk);
        }

        let bytes = Bytes::from(output);
        self.state = BodyState::Buffered(bytes.clone());
        Ok(bytes)
    }

    /// Collects the body using the limit configured when it was constructed.
    pub async fn collect_default(&mut self) -> Result<Bytes, HttpError> {
        self.collect(self.default_limit).await
    }

    /// Takes the one-shot body stream, enforcing its configured limit across
    /// chunks. Exceeding the limit yields an error which
    /// [`HttpError::body_read`] maps to `payload_too_large`, then terminates
    /// the stream.
    pub fn take_stream(&mut self) -> Result<BodyStream, HttpError> {
        let limit = self.default_limit;
        let stream = self.take_raw_stream()?;
        Ok(limit_stream(stream, limit))
    }

    fn take_raw_stream(&mut self) -> Result<BodyStream, HttpError> {
        let state = std::mem::replace(&mut self.state, BodyState::Taken);
        match state {
            BodyState::Incoming(body) => {
                Ok(Box::pin(body.into_data_stream().map(|chunk| {
                    chunk.map_err(|error| Box::new(error) as BoxError)
                })))
            }
            BodyState::Buffered(bytes) => {
                Ok(Box::pin(futures_util::stream::once(
                    async move { Ok(bytes) },
                )))
            }
            BodyState::Stream(stream) => Ok(stream),
            BodyState::Taken => Err(HttpError::body_already_consumed()),
        }
    }
}

fn limit_stream(stream: BodyStream, limit: usize) -> BodyStream {
    Box::pin(
        stream.scan((0_usize, false), move |(seen, finished), chunk| {
            let result = if *finished {
                None
            } else {
                match chunk {
                    Ok(chunk) => match seen
                        .checked_add(chunk.len())
                        .filter(|length| *length <= limit)
                    {
                        Some(length) => {
                            *seen = length;
                            Some(Ok(chunk))
                        }
                        None => {
                            *finished = true;
                            Some(Err(Box::new(BodyLimitExceeded { limit }) as BoxError))
                        }
                    },
                    Err(error) => Some(Err(error)),
                }
            };
            future::ready(result)
        }),
    )
}
