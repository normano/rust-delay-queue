use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration};
use std::cmp::Ordering;

use priority_queue::PriorityQueue;

use std::hash::Hash;
use crate::Delay;
use chrono::Utc;

/// A concurrent unbounded blocking queue where each item can only be removed when its delay
/// expires.
///
/// The queue supports multiple producers and multiple consumers.
///
/// Items of the queue must implement the `Delayed` trait. In most situations you can just use
/// the helper struct `Delay` to wrap the values to be used by the queue.
///
/// If you implement the `Delayed` trait for your types, keep in mind that the `DelayQueue` assumes
/// that the `Instant` until which each item is delayed does not change while that item is
/// in the queue.
///
/// # Examples
///
/// Basic usage:
///
/// ```no_run
/// use delay_queue::{Delay, DelayQueue};
/// use std::time::{Duration, Instant};
/// use chrono::Utc;
///
/// let mut queue = DelayQueue::new();
/// queue.push(Delay::for_duration("2nd", Duration::from_secs(5)));
/// queue.push(Delay::until_instant("1st", Utc::now()));
///
/// println!("First pop: {}", queue.pop().value);
/// println!("Second pop: {}", queue.pop().value);
/// assert!(queue.is_empty());
/// ```
#[derive(Debug)]
pub struct DelayQueue<T: Hash + Eq> {
    /// Points to the data that is shared between instances of the same queue (created by
    /// cloning a queue). Usually the different instances of a queue will live in different
    /// threads.
    shared_data: Arc<DelayQueueSharedData<T>>,
}

/// The underlying data of a queue.
///
/// When a `DelayQueue` is cloned, it's clone will point to the same `DelayQueueSharedData`.
/// This is done so a queue be used by different threads.
#[derive(Debug)]
struct DelayQueueSharedData<T: Hash + Eq> {
    /// Mutex protected `BinaryHeap` that holds the items of the queue in the order that they
    /// should be popped.
    queue: Mutex<PriorityQueue<T, i64>>,

    /// Condition variable that signals when there is a new item at the head of the queue.
    condvar_new_head: Condvar,
}

impl<T: Hash + Eq> DelayQueue<T> {
    /// Creates an empty `DelayQueue<T>`.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// use delay_queue::{Delay, DelayQueue};
    ///
    /// let mut queue : DelayQueue<T>  = DelayQueue::new();
    /// ```
    pub fn new() -> DelayQueue<T> {
        DelayQueue {
            shared_data: Arc::new(DelayQueueSharedData {
                queue: Mutex::new(PriorityQueue::new()),
                condvar_new_head: Condvar::new(),
            }),
        }
    }

    /// Creates an empty `DelayQueue<T>` with a specific capacity.
    /// This preallocates enough memory for `capacity` elements,
    /// so that the `DelayQueue` does not have to be reallocated
    /// until it contains at least that many values.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// use delay_queue::{Delay, DelayQueue};
    ///
    /// let mut queue : DelayQueue<T>  = DelayQueue::with_capacity(10);
    /// ```
    pub fn with_capacity(capacity: usize) -> DelayQueue<T> {
        DelayQueue {
            shared_data: Arc::new(DelayQueueSharedData {
                queue: Mutex::new(PriorityQueue::with_capacity(capacity)),
                condvar_new_head: Condvar::new(),
            }),
        }
    }

    /// Pushes an item onto the queue.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// use delay_queue::{Delay, DelayQueue};
    /// use std::time::Duration;
    ///
    /// let mut queue = DelayQueue::new();
    /// queue.push(Delay::for_duration("2nd", Duration::from_secs(5)));
    /// ```
    pub fn push(&mut self, item: Delay<T>) {
        let mut queue = self.shared_data.queue.lock().unwrap();

        {
            // If the new item goes to the head of the queue then notify consumers
            let cur_head = queue.peek();
            if (cur_head == None)
                || (&item.until.timestamp_millis() < cur_head.unwrap().1)
            {
                self.shared_data.condvar_new_head.notify_one();
            }
        }

        queue.push(item.value, -item.until.timestamp_millis());
    }

    /// Pops the next item from the queue, blocking if necessary until an item is available and its
    /// delay has expired.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```no_run
    /// use delay_queue::{Delay, DelayQueue};
    /// use std::time::{Duration, Instant};
    /// use chrono::Utc;
    ///
    /// let mut queue = DelayQueue::new();
    ///
    /// queue.push(Delay::until_instant("1st", Utc::now()));
    ///
    /// // The pop will not block, since the delay has expired.
    /// println!("First pop: {}", queue.pop().value);
    ///
    /// queue.push(Delay::for_duration("2nd", Duration::from_secs(5)));
    ///
    /// // The pop will block for approximately 5 seconds before returning the item.
    /// println!("Second pop: {}", queue.pop().value);
    /// ```
    pub fn pop(&mut self) -> Option<T> {

        let mut queue = self.shared_data.queue.lock().unwrap();

        loop {
            match queue.peek() {
                Some((elem, until)) => {
                    if *until == 0 {
                        queue.pop(); // Skip removed item
                        continue;
                    }

                    let now = Utc::now().timestamp_millis();

                    if (-*until) > now {
                        return None;
                    }

                    return Some(queue.pop().unwrap().0)
                }
                // Signal that there is no element with a duration of zero
                None => return None,
            };
        }
    }

    /// Sets item to be removed item eventually
    /// should_keep_fn should return true on any items it seeks to keep
    pub fn remove<F>(&mut self, should_keep_fn: F)
    where F: Fn(&T, &i64) -> bool
    {
        let mut queue = self.shared_data.queue.lock().unwrap();

        let items = queue.iter_mut();

        for (item, mut priority) in items {
            if !should_keep_fn(&item, priority) {
                *priority = 0;
            }
        }
    }

    /// Pops the next item from the queue, blocking if necessary until an item is available and its
    /// delay has expired or until the given timeout expires.
    ///
    /// Returns `None` if the given timeout expires and no item became available to be popped.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```no_run
    /// use delay_queue::{Delay, DelayQueue};
    /// use std::time::Duration;
    ///
    /// let mut queue = DelayQueue::new();
    ///
    /// queue.push(Delay::for_duration("1st", Duration::from_secs(5)));
    ///
    /// // The pop will block for approximately 2 seconds before returning None.
    /// println!("First pop: {:?}",
    ///          queue.try_pop_for(Duration::from_secs(2))); // Prints "None"
    ///
    /// // The pop will block for approximately 3 seconds before returning the item.
    /// println!("Second pop: {}",
    ///          queue.try_pop_for(Duration::from_secs(5)).unwrap().value); // Prints "1st"
    /// ```
    pub fn try_pop_for(&mut self, timeout: Duration) -> Option<T> {
        //TODO the downcast may be a terrible thing
        self.try_pop_until(Utc::now().timestamp_millis() + timeout.as_millis() as i64)
    }

    /// Pops the next item from the queue, blocking if necessary until an item is available and its
    /// delay has expired or until the given `Instant` is reached.
    ///
    /// Returns `None` if the given `Instant` is reached and no item became available to be popped.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```no_run
    /// use delay_queue::{Delay, DelayQueue};
    /// use std::time::{Duration, Instant};
    /// use chrono::Utc;
    ///
    /// let mut queue = DelayQueue::new();
    ///
    /// queue.push(Delay::for_duration("1st", Duration::from_secs(5)));
    ///
    /// // The pop will block for approximately 2 seconds before returning None.
    /// println!("First pop: {:?}",
    ///          queue.try_pop_until(Utc::now().timestamp_millis() + Duration::from_secs(2).as_millis() as i64)); // Prints "None"
    ///
    /// // The pop will block for approximately 5 seconds before returning the item.
    /// println!("Second pop: {}",
    ///          queue.try_pop_until(Utc::now().timestamp_millis() + Duration::from_secs(5).as_millis() as i64)
    ///               .unwrap().value); // Prints "1st"
    /// ```
    pub fn try_pop_until(&mut self, try_until: i64) -> Option<T> {
        let mut queue = self.shared_data.queue.lock().unwrap();

        // Loop until an element can be popped or the timeout expires, waiting if necessary
        loop {
            let now = Utc::now().timestamp_millis();

            let next_elem_duration = match queue.peek() {
                // If there is an element and its delay is expired, break out of the loop to pop it
                Some(elem) if (-*elem.1) <= now => break,

                // Calculate the Duration until the element expires
                Some(elem) => (-*elem.1) - now,

                // Signal that there is no element with a duration of zero
                None => 0,
            };

            if now >= try_until {
                return None;
            }

            let time_left = try_until - now;

            let wait_duration = if next_elem_duration > 0 {
                // We'll wait until the time to pop the next element is reached
                // or our timeout expires, whichever comes first
                next_elem_duration.min(time_left)
            } else {
                // There is no element in the queue, we'll wait for one until our timeout expires
                time_left
            };

            // Wait until there is a new head of the queue,
            // the time to pop the current head expires,
            // or the timeout expires
            queue = self.shared_data
                .condvar_new_head
                .wait_timeout(queue, Duration::from_millis(wait_duration as u64))
                .unwrap()
                .0
        }

        Some(self.force_pop(queue))
    }

    /// Checks if the queue is empty.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// use delay_queue::{Delay, DelayQueue};
    /// use std::time::Instant;
    /// use chrono::Utc;
    ///
    /// let mut queue = DelayQueue::new();
    /// queue.push(Delay::until_instant("val", Utc::now()));
    ///
    /// assert!(!queue.is_empty());
    ///
    /// println!("First pop: {}", queue.pop().value);
    ///
    /// assert!(queue.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        let queue = self.shared_data.queue.lock().unwrap();
        queue.is_empty()
    }

    /// Pops an element from the queue, notifying `condvar_new_head` if there are elements still
    /// left in the queue.
    ///
    /// # Panics
    ///
    /// Panics if `queue` is empty.
    fn force_pop(&self, mut queue: MutexGuard<PriorityQueue<T, i64>>) -> T {
        if queue.len() > 1 {
            self.shared_data.condvar_new_head.notify_one();
        }

        queue.pop().unwrap().0
    }
}

impl<T: Hash + Eq> Default for DelayQueue<T> {
    /// Creates an empty `DelayQueue<T>`.
    fn default() -> DelayQueue<T> {
        DelayQueue::new()
    }
}

impl<T: Hash + Eq> Clone for DelayQueue<T> {
    /// Returns a new `DelayQueue` that points to the same underlying data.
    ///
    /// This method can be used to share a queue between different threads.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```no_run
    /// use delay_queue::{Delay, DelayQueue};
    /// use std::time::Duration;
    /// use std::thread;
    ///
    /// let mut queue = DelayQueue::new();
    ///
    /// queue.push(Delay::for_duration("1st", Duration::from_secs(1)));
    ///
    /// let mut cloned_queue = queue.clone();
    ///
    /// let handle = thread::spawn(move || {
    ///     println!("First pop: {}", cloned_queue.pop().value);
    ///     println!("Second pop: {}", cloned_queue.pop().value);
    /// });
    ///
    /// queue.push(Delay::for_duration("2nd", Duration::from_secs(2)));
    ///
    /// handle.join().unwrap();
    /// ```
    fn clone(&self) -> DelayQueue<T> {
        DelayQueue {
            shared_data: self.shared_data.clone(),
        }
    }
}
