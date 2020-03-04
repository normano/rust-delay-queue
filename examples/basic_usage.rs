extern crate delay_queue;

use std::time::{Duration, Instant};
use std::thread;
use delay_queue::{Delay, DelayQueue};
use chrono::Utc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

fn main() {
    let queue: DelayQueue<&str> = DelayQueue::new();
    let is_stopped = Arc::new(AtomicBool::new(false));

    // Clone the queue and move it to the consumer thread
    let is_stopped_clone = is_stopped.clone();
    let mut consumer_queue = queue.clone();
    let consumer_handle = thread::spawn(move || {
        let is_stopped = is_stopped_clone;
        // The pop() will block until an item is available and its delay has expired
        while !is_stopped.load(Ordering::SeqCst) || !consumer_queue.is_empty() {
            let item_option = consumer_queue.pop();
            if item_option.is_some() {
                println!("Popped: {}", item_option.unwrap()); // Prints "First pop: now"
            } else {
                std::thread::sleep(Duration::from_millis(500));
            }
        }
    });

    // Clone the queue and move it to the producer thread
    let mut producer_queue = queue.clone();
    let producer_handle = thread::spawn(move || {
        // This item can only be popped after 3 seconds have passed
        producer_queue.push(Delay::for_duration("3s", Duration::from_secs(3)));

        // This item can be popped immediately
        producer_queue.push(Delay::until_instant("now", Utc::now()));

        is_stopped.clone().store(true, Ordering::SeqCst);

        // This item can only be popped after 5 seconds have passed
        producer_queue.push(Delay::for_duration("10s", Duration::from_secs(10)));
    });

    consumer_handle.join().unwrap();
    producer_handle.join().unwrap();

    assert!(queue.is_empty());
}
