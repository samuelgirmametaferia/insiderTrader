//! Bounded, typed event transport with explicit backpressure semantics.

#![forbid(unsafe_code)]

use std::collections::VecDeque;
use std::sync::{Condvar, Mutex};

/// Result of a non-blocking send.
#[derive(Debug, Eq, PartialEq)]
pub enum SendError<T> {
    /// The queue is at capacity; the value was not accepted.
    Full(T),
    /// All receivers have been closed; the value was not accepted.
    Closed(T),
}

/// Result of receiving from a bus.
#[derive(Debug, Eq, PartialEq)]
pub enum RecvError {
    /// No value is currently available.
    Empty,
    /// The bus has been closed and drained.
    Closed,
}

struct State<T> {
    queue: VecDeque<T>,
    closed: bool,
    receivers: usize,
}

/// A bounded FIFO bus. Sending never allocates beyond the configured capacity.
pub struct BoundedBus<T> {
    capacity: usize,
    state: Mutex<State<T>>,
    changed: Condvar,
}

impl<T> BoundedBus<T> {
    /// Creates a bus with a fixed, non-zero capacity.
    ///
    /// # Panics
    /// This function does not panic; zero capacity is represented by `None`.
    #[must_use]
    pub fn new(capacity: usize) -> Option<Self> {
        (capacity > 0).then(|| Self {
            capacity,
            state: Mutex::new(State {
                queue: VecDeque::with_capacity(capacity),
                closed: false,
                receivers: 1,
            }),
            changed: Condvar::new(),
        })
    }

    /// Returns the configured maximum number of queued values.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    /// Attempts to enqueue without waiting for capacity.
    ///
    /// # Errors
    /// Returns [`SendError::Full`] when capacity is exhausted or
    /// [`SendError::Closed`] after shutdown.
    pub fn try_send(&self, value: T) -> Result<(), SendError<T>> {
        let Ok(mut state) = self.state.lock() else {
            return Err(SendError::Closed(value));
        };
        if state.closed || state.receivers == 0 {
            return Err(SendError::Closed(value));
        }
        if state.queue.len() == self.capacity {
            return Err(SendError::Full(value));
        }
        state.queue.push_back(value);
        self.changed.notify_one();
        Ok(())
    }

    /// Waits for capacity, or returns closed if the bus is closed.
    ///
    /// # Errors
    /// Returns [`SendError::Closed`] when shutdown occurs or the state lock is
    /// poisoned.
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        let Ok(mut state) = self.state.lock() else {
            return Err(SendError::Closed(value));
        };
        while state.queue.len() == self.capacity && !state.closed && state.receivers > 0 {
            match self.changed.wait(state) {
                Ok(next) => state = next,
                Err(_) => return Err(SendError::Closed(value)),
            }
        }
        if state.closed || state.receivers == 0 {
            return Err(SendError::Closed(value));
        }
        state.queue.push_back(value);
        self.changed.notify_one();
        Ok(())
    }

    /// Removes one value without waiting.
    ///
    /// # Errors
    /// Returns [`RecvError::Empty`] when no value is ready, or
    /// [`RecvError::Closed`] after shutdown and drain.
    pub fn try_recv(&self) -> Result<T, RecvError> {
        let Ok(mut state) = self.state.lock() else {
            return Err(RecvError::Closed);
        };
        let value = state.queue.pop_front();
        if value.is_some() {
            self.changed.notify_one();
            return value.ok_or(RecvError::Empty);
        }
        if state.closed {
            Err(RecvError::Closed)
        } else {
            Err(RecvError::Empty)
        }
    }

    /// Waits until a value arrives or the bus closes.
    ///
    /// # Errors
    /// Returns [`RecvError::Closed`] after shutdown and drain, or if the state
    /// lock becomes poisoned.
    pub fn recv(&self) -> Result<T, RecvError> {
        let Ok(mut state) = self.state.lock() else {
            return Err(RecvError::Closed);
        };
        loop {
            if let Some(value) = state.queue.pop_front() {
                self.changed.notify_one();
                return Ok(value);
            }
            if state.closed {
                return Err(RecvError::Closed);
            }
            match self.changed.wait(state) {
                Ok(next) => state = next,
                Err(_) => return Err(RecvError::Closed),
            }
        }
    }

    /// Closes the bus after which no new values are accepted.
    pub fn close(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.closed = true;
            self.changed.notify_all();
        }
    }

    /// Returns the number of queued values.
    #[must_use]
    pub fn len(&self) -> usize {
        self.state.lock().map_or(0, |state| state.queue.len())
    }

    /// Returns whether no value is currently queued.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<T> Drop for BoundedBus<T> {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;

    use super::{BoundedBus, RecvError, SendError};

    #[test]
    fn zero_capacity_is_rejected_and_full_is_explicit() {
        assert!(BoundedBus::<u8>::new(0).is_none());
        let Some(bus) = BoundedBus::new(1) else {
            return;
        };
        assert_eq!(bus.try_send(1), Ok(()));
        assert_eq!(bus.try_send(2), Err(SendError::Full(2)));
        assert_eq!(bus.try_recv(), Ok(1));
    }

    #[test]
    fn fifo_and_close_semantics_are_stable() {
        let Some(bus) = BoundedBus::new(2) else {
            return;
        };
        assert_eq!(bus.try_send("a"), Ok(()));
        assert_eq!(bus.try_send("b"), Ok(()));
        bus.close();
        assert_eq!(bus.recv(), Ok("a"));
        assert_eq!(bus.recv(), Ok("b"));
        assert_eq!(bus.recv(), Err(RecvError::Closed));
        assert_eq!(bus.try_send("c"), Err(SendError::Closed("c")));
    }

    #[test]
    fn blocked_sender_is_released_when_receiver_makes_space() {
        let Some(bus) = BoundedBus::new(1) else {
            return;
        };
        let bus = Arc::new(bus);
        assert_eq!(bus.try_send(1), Ok(()));
        let producer_bus = Arc::clone(&bus);
        let producer = thread::spawn(move || producer_bus.send(2));
        assert_eq!(bus.recv(), Ok(1));
        assert_eq!(producer.join().ok(), Some(Ok(())));
        assert_eq!(bus.recv(), Ok(2));
    }
}
