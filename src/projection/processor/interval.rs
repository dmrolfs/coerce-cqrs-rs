use rand::{rngs::StdRng, Rng, SeedableRng};
use std::time::Duration;
use strum_macros::{Display, EnumString, EnumVariantNames, IntoStaticStr};

pub trait CalculateInterval {
    fn next_interval(&mut self, retry_attempt: u32) -> Duration;
}

#[derive(Debug, Copy, Clone, PartialEq, Display, EnumString, EnumVariantNames, IntoStaticStr)]
pub enum CalculateIntervalFactory {
    Regular(Duration),
    ExponentialBackoff {
        min: Duration,
        max: Duration,
        jitter: f32,
        factor: u32,
    },
}

impl CalculateIntervalFactory {
    pub fn make(&self) -> Box<dyn CalculateInterval> {
        match self {
            Self::Regular(dur) => Box::new(RegularInterval(*dur)),
            Self::ExponentialBackoff {
                min,
                max,
                jitter,
                factor,
            } => Box::new(ExponentialBackoff {
                min: *min,
                max: *max,
                jitter: *jitter,
                factor: *factor,
                rng: StdRng::from_entropy(),
            }),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct RegularInterval(Duration);

impl RegularInterval {
    pub const fn of_duration(duration: Duration) -> Self {
        Self(duration)
    }
}

impl CalculateInterval for RegularInterval {
    fn next_interval(&mut self, _retry_attempt: u32) -> Duration {
        self.0
    }
}

#[derive(Debug, PartialEq)]
pub struct ExponentialBackoff {
    min: Duration,
    max: Duration,
    jitter: f32,
    factor: u32,
    rng: StdRng,
}

impl CalculateInterval for ExponentialBackoff {
    fn next_interval(&mut self, retry_attempt: u32) -> Duration {
        if retry_attempt == 0 {
            return self.min;
        }

        let exponent = self.factor.saturating_pow(retry_attempt);
        let duration = self.min.saturating_mul(exponent);

        // Apply jitter. Uses multiples of 100 to prevent relying on floats.
        let jitter_factor = (self.jitter * 100_f32) as u32;
        let random: u32 = self.rng.gen_range(0..jitter_factor * 2);
        let mut duration = duration.saturating_mul(100);
        if random < jitter_factor {
            let jitter = duration.saturating_mul(random) / 100;
            duration = duration.saturating_sub(jitter);
        } else {
            let jitter = duration.saturating_mul(random / 2) / 100;
            duration = duration.saturating_add(jitter);
        }

        duration /= 100;
        duration = duration.min(self.max);
        duration = duration.max(self.min);
        duration
    }
}
