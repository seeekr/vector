use crate::{
    buffers::Acker,
    sinks::util::{
        encoding::{self, BasicEncoding},
        SinkExt,
    },
    topology::config::DataType,
};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

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

        let healthcheck = file_healthcheck(&self.path);

        let path = self
            .path
            .canonicalize()
            .map_err(|err| format!("Cannot canonicalize path {:?}: {}", self.path, err));

        let sink = FileSink::new(path?)
            .stream_ack(acker)
            .with(move |event| encoding::log_event_as_bytes_with_nl(event, &encoding));

        Ok((Box::new(sink), healthcheck))
    }

    fn input_type(&self) -> DataType {
        DataType::Log
    }
}

pub fn file_healthcheck(path: &Path) -> super::Healthcheck {
    Box::new(if path.exists() {
        future::ok(())
    } else {
        future::err("File doesn't exist.".to_string())
    })
}

pub struct FileSink {
    path: PathBuf,
    state: FileSinkState,
}

enum FileSinkState {
    Disconnected,
    CreatingFile(CreateFuture<PathBuf>),
    FileProvided(FramedWrite<File, BytesCodec>),
}

impl FileSink {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path: path,
            state: FileSinkState::Disconnected,
        }
    }

    pub fn poll_file(&mut self) -> Poll<&mut FramedWrite<File, BytesCodec>, ()> {
        loop {
            match self.state {
                FileSinkState::Disconnected => {
                    debug!(message = "creating file", path = ?self.path);
                    self.state = FileSinkState::CreatingFile(resolve_and_create(self.path.clone()));
                }
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

                        //todo: when dynamic paths will be introduced, it possibly will make sense to perform back-off
                        return Err(());
                    }
                },
                FileSinkState::FileProvided(ref mut sink) => return Ok(Async::Ready(sink)),
            }
        }
    }
}

fn resolve_and_create(path: PathBuf) -> CreateFuture<PathBuf> {
    //todo: interpolation
    File::create(path)
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
        // We want to create the file only after receiving first event
        if let FileSinkState::Disconnected = self.state {
            return Ok(Async::Ready(()));
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
