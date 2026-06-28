//! The stream ordering invariant, shared by the recorder (enforced on write)
//! and the reader (enforced on read).
//!
//! Across a whole session: `seq` must be **strictly increasing** and
//! `timestamp` **non-decreasing**. Centralising the check here guarantees that a
//! recording the [`crate::Recorder`] accepts is one the [`crate::SessionReader`]
//! reads back without a mid-stream ordering error.

use chrono::{DateTime, Utc};

use crate::error::Error;

/// Tracks the last accepted `(seq, timestamp)` and rejects any subsequent pair
/// that is not strictly increasing in `seq` and non-decreasing in `timestamp`.
#[derive(Debug, Default)]
pub(crate) struct Monotonicity {
    last: Option<(u64, DateTime<Utc>)>,
}

impl Monotonicity {
    /// Check `(seq, ts)` against the last accepted pair. On success the state
    /// advances and `Ok(())` is returned; on violation the state is left
    /// unchanged and [`Error::Monotonicity`] is returned.
    pub(crate) fn check(&mut self, seq: u64, ts: DateTime<Utc>) -> Result<(), Error> {
        if let Some((prev_seq, prev_ts)) = self.last {
            if seq <= prev_seq || ts < prev_ts {
                return Err(Error::Monotonicity {
                    prev_seq,
                    prev_ts,
                    seq,
                    ts,
                });
            }
        }
        self.last = Some((seq, ts));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::dt;

    #[test]
    fn accepts_increasing() {
        let mut m = Monotonicity::default();
        assert!(m.check(0, dt(1)).is_ok());
        assert!(m.check(1, dt(1)).is_ok()); // equal ts is allowed
        assert!(m.check(2, dt(2)).is_ok());
    }

    #[test]
    fn rejects_seq_regression_without_advancing() {
        let mut m = Monotonicity::default();
        m.check(5, dt(1)).expect("ok");
        // Violation: seq 3 <= 5.
        assert!(matches!(
            m.check(3, dt(2)),
            Err(Error::Monotonicity {
                prev_seq: 5,
                seq: 3,
                ..
            })
        ));
        // State did not advance: a later good pair relative to 5 still works,
        // and one that would have been fine relative to 3 is still rejected.
        assert!(m.check(4, dt(2)).is_err());
        assert!(m.check(6, dt(2)).is_ok());
    }

    #[test]
    fn rejects_timestamp_regression() {
        let mut m = Monotonicity::default();
        m.check(0, dt(10)).expect("ok");
        assert!(matches!(m.check(1, dt(9)), Err(Error::Monotonicity { .. })));
    }
}
