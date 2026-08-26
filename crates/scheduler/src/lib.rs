//! Deterministic bounded deadline scheduler for metric and strategy work.

#![forbid(unsafe_code)]

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Mutex;

use insider_common_types::MonoTime;

/// Scheduler priority; higher priority runs first at equal readiness.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Priority {
    /// Background batch work.
    Batch,
    /// Normal decision work.
    Standard,
    /// Deadline-sensitive work.
    Fast,
    /// Highest-priority hot-path work.
    Ultra,
}

/// Work submitted to the scheduler.
#[derive(Clone, Debug)]
pub struct Work<T> {
    /// Caller-defined task identity.
    pub task_id: u64,
    /// Priority class.
    pub priority: Priority,
    /// Absolute monotonic deadline.
    pub deadline: MonoTime,
    /// Opaque task payload.
    pub payload: T,
}

impl<T> PartialEq for Work<T> {
    fn eq(&self, other: &Self) -> bool {
        self.task_id == other.task_id
            && self.priority == other.priority
            && self.deadline == other.deadline
    }
}

impl<T> Eq for Work<T> {}

impl<T> Ord for Work<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.priority
            .cmp(&other.priority)
            .then_with(|| other.deadline.cmp(&self.deadline))
            .then_with(|| other.task_id.cmp(&self.task_id))
    }
}

impl<T> PartialOrd for Work<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A popped task and whether its deadline has passed.
#[derive(Debug, Eq, PartialEq)]
pub struct Ready<T> {
    /// The scheduled work.
    pub work: Work<T>,
    /// True when `now` was after the work deadline.
    pub late: bool,
}

/// Admission failures.
#[derive(Debug, Eq, PartialEq)]
pub enum SubmitError<T> {
    /// Queue is full; the task was not admitted.
    Full(Work<T>),
    /// Scheduler lock is unavailable.
    Unavailable(Work<T>),
}

/// Bounded priority/deadline queue.
pub struct Scheduler<T> {
    capacity: usize,
    next_sequence: Mutex<u64>,
    queue: Mutex<BinaryHeap<Work<T>>>,
}

/// Bounded scheduler state suitable for health and saturation telemetry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SchedulerStats {
    /// Number of queued tasks.
    pub queued: usize,
    /// Maximum number of queued tasks.
    pub capacity: usize,
}

impl<T> Scheduler<T> {
    /// Creates a scheduler with non-zero bounded capacity.
    #[must_use]
    pub fn new(capacity: usize) -> Option<Self> {
        (capacity > 0).then(|| Self {
            capacity,
            next_sequence: Mutex::new(0),
            queue: Mutex::new(BinaryHeap::new()),
        })
    }

    /// Admits work, rejecting instead of allocating beyond capacity.
    ///
    /// # Errors
    /// Returns [`SubmitError::Full`] when capacity is exhausted or
    /// [`SubmitError::Unavailable`] when scheduler state cannot be locked.
    pub fn submit(&self, work: Work<T>) -> Result<u64, SubmitError<T>> {
        let Ok(mut queue) = self.queue.lock() else {
            return Err(SubmitError::Unavailable(work));
        };
        if queue.len() >= self.capacity {
            return Err(SubmitError::Full(work));
        }
        let Ok(mut sequence) = self.next_sequence.lock() else {
            return Err(SubmitError::Unavailable(work));
        };
        let id = *sequence;
        *sequence = sequence.saturating_add(1);
        queue.push(work);
        Ok(id)
    }

    /// Removes the highest-priority task, classifying deadline lateness.
    pub fn pop_ready(&self, now: MonoTime) -> Option<Ready<T>> {
        let Ok(mut queue) = self.queue.lock() else {
            return None;
        };
        queue.pop().map(|work| Ready {
            late: now > work.deadline,
            work,
        })
    }

    /// Number of queued tasks.
    #[must_use]
    pub fn len(&self) -> usize {
        self.queue.lock().map_or(0, |queue| queue.len())
    }

    /// Whether no tasks are queued.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Removes all queued work for a caller-provided task identity.
    ///
    /// Cancellation is bounded by the current queue capacity and never mutates
    /// unrelated tasks. It is safe to call repeatedly; subsequent calls return
    /// `false` once the identity has been removed.
    pub fn cancel(&self, task_id: u64) -> bool {
        let Ok(mut queue) = self.queue.lock() else {
            return false;
        };
        let mut retained = BinaryHeap::with_capacity(queue.len());
        let mut removed = false;
        while let Some(work) = queue.pop() {
            if work.task_id == task_id {
                removed = true;
            } else {
                retained.push(work);
            }
        }
        *queue = retained;
        removed
    }

    /// Returns queue occupancy and its hard admission bound.
    #[must_use]
    pub fn stats(&self) -> SchedulerStats {
        SchedulerStats {
            queued: self.len(),
            capacity: self.capacity,
        }
    }
}

#[cfg(test)]
mod tests {
    use insider_common_types::MonoTime;

    use super::{Priority, Scheduler, SubmitError, Work};

    fn work(priority: Priority, deadline: u64, task_id: u64) -> Work<u8> {
        Work {
            task_id,
            priority,
            deadline: MonoTime::from_nanos(deadline),
            payload: 1,
        }
    }

    #[test]
    fn priority_precedes_deadline_and_lateness_is_explicit() {
        let Some(scheduler) = Scheduler::new(2) else {
            return;
        };
        assert_eq!(scheduler.submit(work(Priority::Standard, 1, 10)), Ok(0));
        assert_eq!(scheduler.submit(work(Priority::Ultra, 100, 20)), Ok(1));
        let first = scheduler.pop_ready(MonoTime::from_nanos(50));
        assert_eq!(
            first.as_ref().map(|ready| ready.work.priority),
            Some(Priority::Ultra)
        );
        assert_eq!(first.as_ref().map(|ready| ready.late), Some(false));
        let second = scheduler.pop_ready(MonoTime::from_nanos(50));
        assert_eq!(second.as_ref().map(|ready| ready.late), Some(true));
    }

    #[test]
    fn capacity_is_enforced_without_dropping_existing_work() {
        let Some(scheduler) = Scheduler::new(1) else {
            return;
        };
        assert_eq!(scheduler.submit(work(Priority::Batch, 10, 1)), Ok(0));
        let result = scheduler.submit(work(Priority::Fast, 1, 2));
        assert!(matches!(result, Err(SubmitError::Full(_))));
        assert_eq!(scheduler.len(), 1);
    }

    #[test]
    fn cancellation_preserves_task_identity_and_unrelated_work() {
        let Some(scheduler) = Scheduler::new(3) else {
            return;
        };
        assert_eq!(scheduler.submit(work(Priority::Standard, 10, 42)), Ok(0));
        assert_eq!(scheduler.submit(work(Priority::Fast, 20, 7)), Ok(1));
        assert!(scheduler.cancel(42));
        assert!(!scheduler.cancel(42));
        assert_eq!(scheduler.stats().queued, 1);
        assert_eq!(
            scheduler
                .pop_ready(MonoTime::from_nanos(0))
                .map(|ready| ready.work.task_id),
            Some(7)
        );
    }
}
