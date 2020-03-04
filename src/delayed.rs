use std::time::{Duration};
use chrono::{DateTime, Utc};

/// Wraps a value that should be delayed.
///
/// Implements `Delayed` and `Eq`. Two `Delay` objects are equal iff their wrapped `value`s are
/// equal and they are delayed until the same `Instant`.
///
/// # Examples
///
/// Basic usage:
///
/// ```
/// use delay_queue::{Delay};
/// use std::time::{Duration};
/// use chrono::Utc;
///
/// let delayed_one_hour = Delay::for_duration(123, Duration::from_secs(3600));
/// let delayed_now = Delay::until_instant("abc", Utc::now());
///
/// assert!(delayed_one_hour.delayed_until() > delayed_now.delayed_until());
/// assert_eq!(delayed_one_hour.value, 123);
/// ```
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Delay<T> {
    /// The value that is delayed.
    pub value: T,

    /// The `Instant` until which `value` is delayed.
    pub until: DateTime<Utc>,
}

impl<T> Delay<T> {
    /// Creates a new `Delay<T>` holding `value` and that is delayed until the given `Instant`.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// use delay_queue::Delay;
    /// use std::time::Instant;
    /// use chrono::Utc;
    ///
    /// let delayed_now = Delay::until_instant("abc", Utc::now());
    /// ```
    pub fn until_instant(value: T, until: DateTime<Utc>) -> Delay<T> {
        Delay { value, until }
    }

    /// Creates a new `Delay<T>` holding `value` and that is delayed until the given `Duration` has
    /// elapsed.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// use delay_queue::Delay;
    /// use std::time::Duration;
    ///
    /// let delayed_one_hour = Delay::for_duration("abc", Duration::from_secs(3600));
    /// ```
    pub fn for_duration(value: T, duration: Duration) -> Delay<T> {
        let now = Utc::now();
        let dt = now + chrono::Duration::from_std(duration).unwrap();

        return Delay::until_instant(value, dt);
    }

    pub fn delayed_until(&self) -> DateTime<Utc> {
        self.until
    }
}

impl<T: Default> Default for Delay<T> {
    /// Creates an expired `Delay<T>` with a default `value`.
    fn default() -> Delay<T> {
        Delay {
            value: Default::default(),
            until: Utc::now(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};
    use super::{Delay};
    use chrono::Utc;

    #[test]
    fn compare_until() {
        let delayed_one_hour = Delay::for_duration(123, Duration::from_secs(3600));
        let delayed_now = Delay::until_instant("abc", Utc::now());

        assert!(delayed_one_hour.delayed_until() > delayed_now.delayed_until());
    }

    #[test]
    fn correct_value() {
        let delayed_one_hour = Delay::for_duration(123, Duration::from_secs(3600));
        let delayed_now = Delay::until_instant("abc", Utc::now());

        assert_eq!(delayed_one_hour.value, 123);
        assert_eq!(delayed_now.value, "abc");
    }
}
