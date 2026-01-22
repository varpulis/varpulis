//! Stream abstraction for the runtime

use crate::event::Event;
use std::collections::VecDeque;
use tokio::sync::mpsc;

/// A stream of events
pub struct Stream {
    pub name: String,
    receiver: mpsc::Receiver<Event>,
    buffer: VecDeque<Event>,
}

impl Stream {
    pub fn new(name: impl Into<String>, receiver: mpsc::Receiver<Event>) -> Self {
        Self {
            name: name.into(),
            receiver,
            buffer: VecDeque::new(),
        }
    }

    pub async fn next(&mut self) -> Option<Event> {
        if let Some(event) = self.buffer.pop_front() {
            return Some(event);
        }
        self.receiver.recv().await
    }

    pub fn push_back(&mut self, event: Event) {
        self.buffer.push_back(event);
    }
}

/// Stream sender for producing events
pub struct StreamSender {
    pub name: String,
    sender: mpsc::Sender<Event>,
}

impl StreamSender {
    pub fn new(name: impl Into<String>, sender: mpsc::Sender<Event>) -> Self {
        Self {
            name: name.into(),
            sender,
        }
    }

    pub async fn send(&self, event: Event) -> Result<(), mpsc::error::SendError<Event>> {
        self.sender.send(event).await
    }
}

/// Create a stream channel pair
pub fn channel(name: impl Into<String>, buffer: usize) -> (StreamSender, Stream) {
    let name = name.into();
    let (tx, rx) = mpsc::channel(buffer);
    (
        StreamSender::new(name.clone(), tx),
        Stream::new(name, rx),
    )
}
