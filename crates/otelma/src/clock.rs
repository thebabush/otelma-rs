//! Monotonic-clock helpers for the capture boundary.
//!
//! The data-source adapter is the only code that reads the wall clock, and it
//! must hand the recorder a non-decreasing timeline (see the determinism
//! contract). A real wall clock can step *backward* — an NTP correction, a VM
//! migration, a manual clock change — so the adapter clamps such a sample
//! forward to the previous instant. How far backward it stepped, though, is
//! meaningful: a few milliseconds is ordinary jitter, a few seconds is worth a
//! warning, and a large jump means the captured timeline can no longer be
//! trusted.
//!
//! [`classify_backstep`] is the shared, pure decision the adapter uses to tell
//! those cases apart. It only *classifies*; clamping, logging, and aborting are
//! the caller's policy (kept out of this venue-agnostic, dependency-light core).

use chrono::{DateTime, TimeDelta, Utc};

/// How a fresh wall-clock sample relates to the monotonic timeline built so far.
///
/// Every variant except [`Backstep::Excessive`] is safe to clamp and keep going;
/// `Excessive` means the clock moved back so far the capture's timestamps are no
/// longer trustworthy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backstep {
    /// The sample is at or after the previous instant (or it is the very first
    /// sample): the timeline advances, nothing to clamp.
    None,
    /// The clock went backward by `by`, within the warn tolerance — clamp and
    /// carry on silently (ordinary jitter).
    Tolerated {
        /// How far the clock moved backward.
        by: TimeDelta,
    },
    /// The clock went backward by `by`, past the warn tolerance but within the
    /// fatal bound — clamp, but the step is worth surfacing.
    Notable {
        /// How far the clock moved backward.
        by: TimeDelta,
    },
    /// The clock went backward by `by`, past the fatal bound — the timeline can
    /// no longer be trusted.
    Excessive {
        /// How far the clock moved backward.
        by: TimeDelta,
    },
}

/// Classify a new wall-clock sample `raw` against the previous stamped instant
/// `prev`.
///
/// `warn_after` and `fatal_after` are the backward magnitudes at which a backstep
/// becomes [`Backstep::Notable`] then [`Backstep::Excessive`]; a backward step of
/// at most `warn_after` is [`Backstep::Tolerated`], and a forward step (or the
/// first sample) is [`Backstep::None`].
///
/// Pure and clock-free — the caller supplies `raw` — so it is fully
/// deterministic and unit-testable. `warn_after` should not exceed `fatal_after`.
pub fn classify_backstep(
    prev: Option<DateTime<Utc>>,
    raw: DateTime<Utc>,
    warn_after: TimeDelta,
    fatal_after: TimeDelta,
) -> Backstep {
    debug_assert!(
        warn_after <= fatal_after,
        "warn_after must not exceed fatal_after"
    );
    let Some(prev) = prev else {
        return Backstep::None;
    };
    if raw >= prev {
        return Backstep::None;
    }
    let by = prev - raw;
    if by > fatal_after {
        Backstep::Excessive { by }
    } else if by > warn_after {
        Backstep::Notable { by }
    } else {
        Backstep::Tolerated { by }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::dt;

    fn warn() -> TimeDelta {
        TimeDelta::seconds(1)
    }
    fn fatal() -> TimeDelta {
        TimeDelta::seconds(60)
    }

    #[test]
    fn first_sample_and_forward_steps_are_none() {
        assert_eq!(
            classify_backstep(None, dt(5), warn(), fatal()),
            Backstep::None
        );
        assert_eq!(
            classify_backstep(Some(dt(5)), dt(9), warn(), fatal()),
            Backstep::None
        );
        // Equal is non-decreasing → still None (no clamp needed).
        assert_eq!(
            classify_backstep(Some(dt(5)), dt(5), warn(), fatal()),
            Backstep::None
        );
    }

    #[test]
    fn small_backstep_is_tolerated() {
        // 1s back, exactly at the warn boundary → tolerated (boundary is inclusive).
        assert_eq!(
            classify_backstep(Some(dt(100)), dt(99), warn(), fatal()),
            Backstep::Tolerated {
                by: TimeDelta::seconds(1)
            }
        );
    }

    #[test]
    fn medium_backstep_is_notable() {
        // Just past warn, within fatal.
        assert_eq!(
            classify_backstep(Some(dt(100)), dt(70), warn(), fatal()),
            Backstep::Notable {
                by: TimeDelta::seconds(30)
            }
        );
        // Exactly at the fatal boundary is still notable (boundary inclusive).
        assert_eq!(
            classify_backstep(Some(dt(100)), dt(40), warn(), fatal()),
            Backstep::Notable {
                by: TimeDelta::seconds(60)
            }
        );
    }

    #[test]
    fn large_backstep_is_excessive() {
        // Just past fatal.
        assert_eq!(
            classify_backstep(Some(dt(1_000)), dt(939), warn(), fatal()),
            Backstep::Excessive {
                by: TimeDelta::seconds(61)
            }
        );
    }
}
