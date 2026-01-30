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
    (StreamSender::new(name.clone(), tx), Stream::new(name, rx))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_stream_channel() {
        let (sender, stream) = channel("test_stream", 10);

        assert_eq!(sender.name, "test_stream");
        assert_eq!(stream.name, "test_stream");
    }

    #[tokio::test]
    async fn test_stream_send_receive() {
        let (sender, mut stream) = channel("test", 10);

        let event = Event::new("TestEvent").with_field("id", 1i64);
        sender.send(event).await.unwrap();

        let received = stream.next().await.unwrap();
        assert_eq!(received.event_type, "TestEvent");
        assert_eq!(received.get_int("id"), Some(1));
    }

    #[tokio::test]
    async fn test_stream_push_back() {
        let (_sender, mut stream) = channel("test", 10);

        // Push events into buffer
        stream.push_back(Event::new("First"));
        stream.push_back(Event::new("Second"));

        // Should receive from buffer first
        let first = stream.next().await.unwrap();
        assert_eq!(first.event_type, "First");

        let second = stream.next().await.unwrap();
        assert_eq!(second.event_type, "Second");
    }

    #[tokio::test]
    async fn test_stream_buffer_then_channel() {
        let (sender, mut stream) = channel("test", 10);

        // Push to buffer
        stream.push_back(Event::new("Buffered"));

        // Send via channel
        sender.send(Event::new("FromChannel")).await.unwrap();

        // Buffer first
        let first = stream.next().await.unwrap();
        assert_eq!(first.event_type, "Buffered");

        // Then channel
        let second = stream.next().await.unwrap();
        assert_eq!(second.event_type, "FromChannel");
    }

    #[tokio::test]
    async fn test_stream_multiple_events() {
        let (sender, mut stream) = channel("test", 100);

        for i in 0..10 {
            sender
                .send(Event::new("Event").with_field("id", i as i64))
                .await
                .unwrap();
        }

        for i in 0..10 {
            let event = stream.next().await.unwrap();
            assert_eq!(event.get_int("id"), Some(i));
        }
    }

    #[tokio::test]
    async fn test_stream_closed() {
        let (sender, mut stream) = channel("test", 10);

        sender.send(Event::new("Last")).await.unwrap();
        drop(sender); // Close the channel

        let event = stream.next().await.unwrap();
        assert_eq!(event.event_type, "Last");

        // Next call should return None (channel closed)
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_stream_sender_name() {
        let (sender, _stream) = channel("sender_test", 10);
        assert_eq!(sender.name, "sender_test");
    }

    #[tokio::test]
    async fn test_stream_buffer_order() {
        let (_sender, mut stream) = channel("test", 10);

        // Push events in specific order
        stream.push_back(Event::new("A"));
        stream.push_back(Event::new("B"));
        stream.push_back(Event::new("C"));

        // Should receive in FIFO order
        assert_eq!(stream.next().await.unwrap().event_type, "A");
        assert_eq!(stream.next().await.unwrap().event_type, "B");
        assert_eq!(stream.next().await.unwrap().event_type, "C");
    }

    #[tokio::test]
    async fn test_stream_sender_closed_error() {
        let (sender, stream) = channel("test", 10);
        drop(stream); // Close receiver

        let event = Event::new("Test");
        let result = sender.send(event).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_stream_large_buffer() {
        let (sender, mut stream) = channel("test", 1000);

        // Send many events
        for i in 0..100 {
            sender
                .send(Event::new("Event").with_field("seq", i as i64))
                .await
                .unwrap();
        }

        // Receive all events
        for i in 0..100 {
            let event = stream.next().await.unwrap();
            assert_eq!(event.get_int("seq"), Some(i));
        }
    }

    #[tokio::test]
    async fn test_stream_interleaved_buffer_and_channel() {
        let (sender, mut stream) = channel("test", 10);

        // Push one to buffer
        stream.push_back(Event::new("Buf1"));

        // Send one via channel
        sender.send(Event::new("Chan1")).await.unwrap();

        // Push another to buffer
        stream.push_back(Event::new("Buf2"));

        // Send another via channel
        sender.send(Event::new("Chan2")).await.unwrap();

        // Buffer events first (in order pushed)
        assert_eq!(stream.next().await.unwrap().event_type, "Buf1");
        assert_eq!(stream.next().await.unwrap().event_type, "Buf2");

        // Then channel events (in order sent)
        assert_eq!(stream.next().await.unwrap().event_type, "Chan1");
        assert_eq!(stream.next().await.unwrap().event_type, "Chan2");
    }

    #[test]
    fn test_stream_sender_new_directly() {
        let (tx, _rx) = mpsc::channel(10);
        let sender = StreamSender::new("direct", tx);
        assert_eq!(sender.name, "direct");
    }

    #[test]
    fn test_stream_new_directly() {
        let (_tx, rx) = mpsc::channel(10);
        let stream = Stream::new("direct_stream", rx);
        assert_eq!(stream.name, "direct_stream");
    }
}
