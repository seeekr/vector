use crate::{
    buffers::Acker,
    event::Event,
    sinks::util::{
        encoding::{self, BasicEncoding},
        SinkExt,
    },
    topology::config::DataType,
};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use futures::{future, try_ready, Async, AsyncSink, Future, Poll, Sink, StartSend};
use tokio::codec::{BytesCodec, FramedWrite};
use tokio::fs::file::{CreateFuture, File};

use tracing::field;

#[derive(Deserialize, Serialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct FileSinkConfig {
    pub path: PathBuf,
    pub encoding: Option<BasicEncoding>,
}

impl FileSinkConfig {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            encoding: None,
        }
    }
}

#[typetag::serde(name = "file")]
impl crate::topology::config::SinkConfig for FileSinkConfig {
    fn build(&self, acker: Acker) -> Result<(super::RouterSink, super::Healthcheck), String> {
        let encoding = self.encoding.clone();

        let sink = FileSink::new(self.path.clone())
            .stream_ack(acker)
            .with(move |event| encoding::log_event_as_bytes_with_nl(event, &encoding));

        Ok((Box::new(sink), Box::new(future::ok(()))))
    }

    fn input_type(&self) -> DataType {
        DataType::Log
    }
}

pub struct FileSink {
    pub path: PathBuf,
    state: FileSinkState,
}

enum FileSinkState {
    Disconnected,
    CreatingFile(CreateFuture<PathBuf>),
    FileProvided(FramedWrite<File, BytesCodec>),
}

impl FileSinkState {
    fn init(path: PathBuf) -> Self {
        debug!(message = "creating file", path = ?path.clone());
        FileSinkState::CreatingFile(File::create(path))
    }
}

pub type EmbeddedFileSink = Box<Sink<SinkItem = Event, SinkError = ()>>;

impl FileSink {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path: path.clone(),
            state: FileSinkState::init(path),
        }
    }

    pub fn new_with_encoding(path: PathBuf, encoding: Option<BasicEncoding>) -> EmbeddedFileSink {
        let sink = FileSink::new(path)
            .with(move |event| encoding::log_event_as_bytes_with_nl(event, &encoding));

        Box::new(sink)
    }

    pub fn poll_file(&mut self) -> Poll<&mut FramedWrite<File, BytesCodec>, ()> {
        loop {
            match self.state {
                FileSinkState::Disconnected => return Err(()),
                FileSinkState::CreatingFile(ref mut create_future) => match create_future.poll() {
                    Ok(Async::Ready(file)) => {
                        debug!(message = "created", file = ?file);
                        self.state =
                            FileSinkState::FileProvided(FramedWrite::new(file, BytesCodec::new()));
                    }
                    Ok(Async::NotReady) => return Ok(Async::NotReady),
                    Err(err) => {
                        error!("Error creating file {:?}: {}", self.path, err);
                        self.state = FileSinkState::Disconnected;

                        return Err(());
                    }
                },
                FileSinkState::FileProvided(ref mut sink) => return Ok(Async::Ready(sink)),
            }
        }
    }
}

impl Sink for FileSink {
    type SinkItem = Bytes;
    type SinkError = ();

    fn start_send(&mut self, line: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        match self.poll_file() {
            Ok(Async::Ready(file)) => {
                debug!(
                    message = "sending event",
                    bytes = &field::display(line.len())
                );
                match file.start_send(line) {
                    Err(err) => {
                        debug!(
                            message = "disconnected",
                            path = ?self.path
                        );
                        error!("Error while creating {:?}: {}", self.path, err);
                        self.state = FileSinkState::Disconnected;
                        Ok(AsyncSink::Ready)
                    }
                    Ok(ok) => Ok(ok),
                }
            }
            Ok(Async::NotReady) => Ok(AsyncSink::NotReady(line)),
            Err(_) => unreachable!(),
        }
    }

    fn poll_complete(&mut self) -> Result<Async<()>, Self::SinkError> {
        if let FileSinkState::Disconnected = self.state {
            return Err(());
        }

        let file = try_ready!(self.poll_file());

        match file.poll_complete() {
            Err(err) => {
                debug!(
                    message = "disconnected",
                    path = ?self.path
                );
                error!("Error while creating {:?}: {}", self.path, err);
                self.state = FileSinkState::Disconnected;
                Ok(Async::Ready(()))
            }
            Ok(ok) => Ok(ok),
        }
    }
}
