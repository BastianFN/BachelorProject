use std::fmt;
use std::ops::Add;
use std::ops::AddAssign;
use std::ops::Sub;
use std::ops::SubAssign;
use timeunits::TP::*;
use timeunits::TS::*;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Abomonation)]
pub struct TimeInterval {
    pub start: TS,
    pub end: TS,
}

impl TimeInterval {
    pub fn new(start: TS, end: TS) -> TimeInterval {
        TimeInterval { start, end }
    }

    pub fn get_start(self) -> Option<usize> {
        match self {
            TimeInterval { start, end: _ } => match start {
                FINITE(x) => Some(x),
                INFINITY => None,
                _ => None,
            },
        }
    }

    pub fn get_end(self) -> Option<usize> {
        match self {
            TimeInterval { start: _, end } => match end {
                FINITE(x) => Some(x),
                INFINITY => None,
                _ => None,
            },
        }
    }

    pub fn get_raw_start(self) -> usize {
        match self {
            TimeInterval { start, end: _ } => match start {
                FINITE(x) => x,
                INFINITY => 0,
                _ => 0,
            },
        }
    }

    pub fn get_raw_end(self) -> usize {
        match self {
            TimeInterval { start: _, end } => match end {
                FINITE(x) => x,
                INFINITY => 0,
                _ => 0,
            },
        }
    }

    pub fn is_infinite(self) -> bool {
        match self {
            TimeInterval { start: _, end } => match end {
                FINITE(_) => false,
                INFINITY => true,
                _ => false,
            }
        }
    }
}

impl Add<usize> for TimeInterval {
    type Output = TimeInterval;

    fn add(self, other: usize) -> TimeInterval {
        TimeInterval::new(self.start + other, self.end + other)
    }
}

impl AddAssign<usize> for TimeInterval {
    fn add_assign(&mut self, other: usize) {
        *self = self.add(other)
    }
}

impl Sub<usize> for TimeInterval {
    type Output = TimeInterval;

    fn sub(self, other: usize) -> TimeInterval {
        TimeInterval::new(self.start - other, self.end - other)
    }
}

impl SubAssign<usize> for TimeInterval {
    fn sub_assign(&mut self, other: usize) {
        *self = self.sub(other)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Abomonation)]
pub enum TP {
    CURR,
    PREV,
}

impl fmt::Display for TP {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                CURR => String::from("current"),
                PREV => String::from("previous"),
            }
        )
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Abomonation)]
pub enum TS {
    FINITE(usize),
    FLAG(usize),
    INFINITY,
}

impl TS {
    pub fn new(ts: usize) -> TS {
        FINITE(ts)
    }

    pub fn infinity() -> TS {
        INFINITY
    }

    // 1 - used as a flag in evaluation plan generation to specify in a mapping
    //     of TS -> Expr, that this expression should be used when the time
    //     point is 0
    pub fn flag(i: usize) -> TS {
        FLAG(i)
    }
}

impl fmt::Display for TS {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                FINITE(n) => n.to_string(),
                INFINITY => "INFINITY".into(),
                FLAG(n) => format!("FLAG({})", n),
            }
        )
    }
}

impl Add for TS {
    type Output = TS;

    fn add(self, other: TS) -> TS {
        match (self, other) {
            (FINITE(ts1), FINITE(ts2)) => FINITE(ts1 + ts2),
            (INFINITY, _) => INFINITY,
            (_, INFINITY) => INFINITY,
            (FLAG(n), _) | (_, FLAG(n)) => FLAG(n),
        }
    }
}

impl Add<usize> for TS {
    type Output = TS;

    fn add(self, n: usize) -> TS {
        match self {
            FINITE(ts) => FINITE(ts + n),
            INFINITY => INFINITY,
            FLAG(n) => FLAG(n),
        }
    }
}

impl AddAssign for TS {
    fn add_assign(&mut self, other: TS) {
        *self = self.add(other);
    }
}

impl AddAssign<usize> for TS {
    fn add_assign(&mut self, other: usize) {
        *self = self.add(other);
    }
}

impl Sub for TS {
    type Output = TS;

    fn sub(self, other: TS) -> TS {
        match (self, other) {
            (FINITE(0), _) => FINITE(0),
            (INFINITY, _) => INFINITY,
            (FINITE(_), INFINITY) => FINITE(0),
            (FINITE(lhs), FINITE(rhs)) => {
                let mut res = 0;
                if lhs > rhs {
                    res = lhs - rhs;
                }
                FINITE(res)
            }
            (FLAG(n), _) | (_, FLAG(n)) => FLAG(n),
        }
    }
}

impl Sub<usize> for TS {
    type Output = TS;

    fn sub(self, n: usize) -> TS {
        match self {
            FINITE(ts) => FINITE(if n >= ts { 0 } else { ts - n }),
            INFINITY => INFINITY,
            FLAG(n) => FLAG(n),
        }
    }
}

impl SubAssign for TS {
    fn sub_assign(&mut self, other: TS) {
        *self = self.sub(other);
    }
}

impl SubAssign<usize> for TS {
    fn sub_assign(&mut self, other: usize) {
        *self = self.sub(other);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_add() {
        let mut ts1 = FINITE(1);
        let mut ts2 = FINITE(2);

        let mut expected = FINITE(3);
        let mut actual = ts1 + ts2;

        assert_eq!(expected, actual);

        ts1 = INFINITY;

        expected = INFINITY;
        actual = ts1 + ts2;

        assert_eq!(expected, actual);

        ts2 = INFINITY;

        expected = INFINITY;
        actual = ts1 + ts2;

        assert_eq!(expected, actual);
    }

    #[test]
    fn timestamp_add_assign() {
        let mut ts1 = FINITE(1);
        let mut ts2 = FINITE(2);

        let mut expected = FINITE(3);
        ts1 += ts2;

        assert_eq!(expected, ts1);

        ts1 = INFINITY;

        expected = INFINITY;
        ts1 += ts2;

        assert_eq!(expected, ts1);

        ts2 = INFINITY;

        expected = INFINITY;
        ts1 += ts2;

        assert_eq!(expected, ts1);
    }

    #[test]
    fn timestamp_sub() {
        let mut ts1 = FINITE(2);
        let mut ts2 = FINITE(1);

        let mut expected = FINITE(1);
        let mut actual = ts1 - ts2;

        assert_eq!(expected, actual);

        ts1 = INFINITY;

        expected = INFINITY;
        actual = ts1 - ts2;

        assert_eq!(expected, actual);

        ts2 = INFINITY;

        expected = INFINITY;
        actual = ts1 - ts2;

        assert_eq!(expected, actual);
    }

    #[test]
    fn timestamp_sub_assign() {
        let mut ts1 = FINITE(2);
        let mut ts2 = FINITE(1);

        let mut expected = FINITE(1);
        ts1 -= ts2;

        assert_eq!(expected, ts1);

        ts1 = INFINITY;

        expected = INFINITY;
        ts1 -= ts2;

        assert_eq!(expected, ts1);

        ts2 = INFINITY;

        expected = INFINITY;
        ts1 -= ts2;

        assert_eq!(expected, ts1);
    }

    #[test]
    fn time_interval_sub() {
        let mut interval = TimeInterval::new(TS::new(1), TS::new(2));

        let mut expected = TimeInterval::new(TS::new(0), TS::new(1));
        interval -= 1;

        assert_eq!(expected, interval);

        expected = TimeInterval::new(TS::new(0), TS::new(0));
        interval -= 1;

        assert_eq!(expected, interval);

        interval = TimeInterval::new(TS::new(2), TS::infinity());

        expected = TimeInterval::new(TS::new(0), TS::infinity());
        interval -= 2;

        assert_eq!(expected, interval);
    }
}
