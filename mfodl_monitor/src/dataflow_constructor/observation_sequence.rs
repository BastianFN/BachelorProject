#![allow(dead_code)]

use std::cmp::min;
use std::collections::HashSet;
use dataflow_constructor::observation_sequence::InfinityIntervals::{InfInterval, Literal, Interval};
use dataflow_constructor::observation_sequence::InfinityIntervalsReturn::Empty;

fn compare(interval : &InfinityIntervals, time_point : usize) -> i8 {
    match interval {
        Literal(a, _) => if time_point < *a { -1 } else if time_point > *a { 1 } else { 0 },
        Interval(a, b, _, _) => if time_point < *a { -1 } else if time_point > *b { 1 } else { 0 },
        InfInterval(a, _) => if time_point < *a { -1 } else { 0 },
    }
}

fn compare_ts(interval : &InfinityIntervals, time_stamp : usize) -> i8 {
    match interval {
        Literal(_, a) => if time_stamp < *a { -1 } else if time_stamp > *a { 1 } else { 0 },
        Interval(_, _, a, b) => if time_stamp < *a { -1 } else if time_stamp > *b { 1 } else { 0 },
        InfInterval(_, a) => if time_stamp < *a { -1 } else { 0 },
    }
}

fn inside_tp(interval : &InfinityIntervals, time_point : usize) -> bool {
    match interval {
        Literal(a,_) => *a == time_point,
        Interval(a, b,_ , _) => time_point >= *a && time_point <= *b,
        InfInterval(a, _) => time_point >= *a,
    }
}

fn inside_ts(interval : &InfinityIntervals, time_stamp : usize) -> bool {
    match interval {
        Literal(_, a) => *a == time_stamp,
        Interval(_, _, a, b) => time_stamp >= *a && time_stamp <= *b,
        InfInterval(_, a) => time_stamp >= *a,
    }
}

fn search_lower_bound(tmp : &Vec<InfinityIntervals>, k: usize, lower_ts : usize) -> Option<usize> {
    let mut last_literal_set = false;
    let mut last_literal = 0;
    return if k > 0 {
        for i in k..tmp.len() {
            //println!("elem {:?}  ts {} == {}",&tmp[i], lower_ts, compare_ts(&tmp[i], lower_ts) == 0);
            let comp = compare_ts(&tmp[i], lower_ts);
            if comp == 0 || comp == -1{
                //println!("tmp[i] == {:?}", tmp[i].clone());
                match tmp[i] {
                    Literal(_, tmp_ts) => {
                        if tmp_ts >= lower_ts {
                            last_literal_set = true;
                            last_literal = i;
                            break
                        }
                    } _ => {}
                }
            } else { break }
        }
        if last_literal_set {
            Some(last_literal)
        } else {
            None
        }
    } else {
        match tmp[0] {
            Literal(_, _) => {
                if compare_ts(&tmp[0], lower_ts) == 0 {
                    Some(0)
                }else {
                    None
                }
            }
            _ => None
        }
    }
}

fn search_upper_bound(tmp : &Vec<InfinityIntervals>, k: usize, lower_ts : usize) -> Option<usize> {
    let mut last_literal_set = false;
    let mut last_literal = 0;
    for i in 0..k+1 {
        //println!("elem {:?}  ts {} == {}",&tmp[i], lower_ts, compare_ts(&tmp[i], lower_ts) == 0);
        let comp = compare_ts(&tmp[i], lower_ts);
        if comp == 0 || comp == 1 {
            //println!("tmp[i] == {:?}", tmp[i].clone());
            match tmp[i] {
                Literal(_, tmp_ts) => {
                    if tmp_ts >= lower_ts {
                        last_literal_set = true;
                        last_literal = i;
                        break
                    }
                } _ => {}
            }
            } else { break }
        }
    if last_literal_set {
        Some(last_literal)
    } else {
        None
    }
}



fn search_lowest(tmp : &Vec<InfinityIntervals>, k: usize, lower_ts : usize, upper_ts : usize, exact: bool) -> Option<usize> {
    let mut last_literal_set = false;
    let mut last_literal = 0;
    return if k > 0 {
        // k+2+1: 2 because for index k we want to search from index k+1 but the loop condition is "<", add 1 to match len (highest_index+1)
        let range = if k+3 < tmp.len() {(0..k+2).rev()} else {(0..k+1).rev()};
        for i in range {
            //println!("elem {:?}  ts {} == {}",&tmp[i],ts, compare_ts(&tmp[i], ts) == 0 || compare_ts(&tmp[i], ts) == -1);
            let comp = compare_ts(&tmp[i], lower_ts);
            if comp == 0 || comp == -1 {
                //println!("tmp[i] == {:?}", tmp[i].clone());
                match tmp[i] {
                    Literal(_, tmp_ts) => {
                        if tmp_ts >= lower_ts && tmp_ts <= upper_ts {
                            last_literal_set = true;
                            last_literal = i;
                        }
                    } _ => {}
                }
            } else { break }
        }

        //println!("{} {}", last_literal_set, last_literal);

        if last_literal_set {
            Some(last_literal)
        } else {
            //println!("None");
            None
        }
    } else {
        if exact {
            match tmp[0] {
                Literal(_, _) => {
                    if compare_ts(&tmp[0], lower_ts) == 0 { Some(0) }else { None }
                }
                // if not inf interval than interval is bounded by literal to the right; such that upper bound is the same as the literal
                Interval(_, _, _, b) => {
                    if b == lower_ts { Some(1) } else { None }
                } _ => None
            }
        } else {
            match tmp[0] {
                Literal(_, _) => {
                    if compare_ts(&tmp[0], lower_ts) == 0 { Some(0) }else { None }
                }
                _ => None
            }
        }
    }
}

fn search_highest(tmp : &Vec<InfinityIntervals>, k: usize, lower_ts : usize, upper_ts : usize) -> Option<usize> {
    let mut last_literal_set = false;
    let mut last_literal = 0;

    // if the index is the last element go left to find a literal as an upper bound
    if tmp.len() == k+1 {
        for i in (0..k+1).rev() {
            let comp = compare_ts(&tmp[i], upper_ts);
            //println!("elem {:?}  ts {} == {}",&tmp[i], upper_ts, compare_ts(&tmp[i], upper_ts) == 0);
            //let val = if zero_interval {comp == 0} else { comp == 0 || comp == 1 };
            if comp == 0 || comp == 1 {
                match tmp[i] {
                    Literal(_, return_ts) => {
                        if return_ts >= lower_ts && return_ts <= upper_ts {
                            last_literal_set = true;
                            last_literal = i;
                        }
                    } _ => {}
                }
                if last_literal_set {
                    break;
                }
            }else { break }
        }
    } else {
        // If k is not a literal then there is one literal to the right of k > 0
        let range = if k > 0 { k-1..tmp.len() } else { k..tmp.len() };
        for i in range {
            let comp = compare_ts(&tmp[i], upper_ts);
            //println!("elem {:?}  ts {} == {}",&tmp[i],upper_ts, compare_ts(&tmp[i], upper_ts) == 0 || compare_ts(&tmp[i], upper_ts) == 1);
            if comp == 0 || comp == 1 {
                match tmp[i] {
                    Literal(_, return_ts) => {
                        if return_ts >= lower_ts && return_ts <= upper_ts {
                            last_literal_set = true;
                            last_literal = i;
                        }
                    } _ => {}
                }
            } else { break }
        }
    }

    if last_literal_set {
        Some(last_literal)
    } else {
        None
    }
}

fn binary_search(tmp : &Vec<InfinityIntervals>, k: usize, tp : bool) -> Option<usize> {
    if tmp.is_empty() {
        return None
    }

    let compare = if tp {compare} else { compare_ts};

    let mut start = 0i64;
    let mut end = (tmp.len() - 1) as i64;
    // Traverse the search space
    while start <= end {
        let mid = (start + end) / 2;
        // If k is found
        if mid < 0 {
            return None;
        }

        let comp = compare(&tmp[mid as usize], k);
        if comp == 0 {
            return Some(mid as usize);
        } else if comp == 1 {
            start = mid + 1;
        } else {
            end = mid - 1;
        }
    }
    return None;
}

fn find_index(tmp : &Vec<InfinityIntervals>, k: usize, tp: bool) -> Option<usize> {
    if tmp.is_empty() {
        return None
    }
    let compare = if tp {compare} else { compare_ts};

    let mut start = 0i64;
    let mut end = (tmp.len() - 1) as i64;
    // Traverse the search space
    while start <= end {
        let mid = (start + end) / 2;
        // If k is found
        if mid < 0 {
            return None;
        }

        let comp = compare(&tmp[mid as usize], k);
        if comp == 0 {
            return Some(mid as usize);
        } else if comp == 1 {
            start = mid + 1;
        } else {
            end = mid - 1;
        }
    }
    return Some((end + 1) as usize);
}

fn is_literal(tmp : InfinityIntervals) -> bool {
    return match tmp {
        Literal(_, _) => true,
        Interval(_, _, _, _) => false,
        InfInterval(_, _) => false
    }
}

fn is_interval(tmp : InfinityIntervals) -> bool {
    return match tmp {
        Literal(_, _) => false,
        Interval(_, _, _, _) => true,
        InfInterval(_, _) => false
    }
}


#[derive(Hash, Eq, Clone, Debug, PartialEq, PartialOrd, Ord, Abomonation, Copy)]
pub enum InfinityIntervals {
    Literal(usize, usize),
    Interval(usize, usize, usize, usize),
    InfInterval(usize, usize)
}

#[derive(Hash, Eq, Clone, Debug, PartialEq, PartialOrd, Ord, Abomonation, Copy)]
pub enum InfinityIntervalsReturn {
    Literal(usize),
    Interval(usize, usize),
    Empty
}


pub trait TimeSeq {
    fn init() -> Self;
    fn insert(self: &mut Self, time_point : usize, time_stamp: usize);
    fn contains(&mut self, time_point: usize) -> Option<usize>;
    fn associated_interval_tp(&mut self, time_point : usize) -> InfinityIntervalsReturn;
    fn associated_interval_ts_exact(&mut self, lower_ts : usize, upper_ts : usize) -> InfinityIntervalsReturn;
    fn associated_interval_ts_inexact(&mut self, lower_ts: usize, upper_ts: usize) -> InfinityIntervalsReturn;
    fn associated_lower_bound_ts(&mut self, ts: usize) -> InfinityIntervalsReturn;
    fn associated_upper_bound_ts(&mut self, ts: usize) -> InfinityIntervalsReturn;
    fn zero_ts(&mut self) -> Option<usize>;
    fn lowest_ts(&mut self) -> Option<usize>;
    fn start_of_sequence(&mut self, tp: usize) -> bool;
    fn end_of_sequence(&mut self, tp: usize) -> bool;
    fn clean_up(&mut self, frontier_tp : usize, frontier_ts : usize);
}

pub struct ObservationSequence {
    pub(crate) observations: Vec<InfinityIntervals>,
    received_tps: HashSet<usize>,
    zero_ts : Option<usize>,
    lowest_ts : Option<usize>
}

impl TimeSeq for ObservationSequence {
    fn init() -> Self {
        let mut obs = Vec::with_capacity(64);
        obs.push(InfInterval(0,0));
        ObservationSequence {
            observations: obs,
            received_tps: HashSet::new(),
            zero_ts: None,
            lowest_ts: None
        }
    }

    fn insert(self: &mut Self, time_point: usize, time_stamp: usize) {
        if self.received_tps.contains(&time_point) {
            return
        } else {
           self.received_tps.insert(time_point);
        }

        if self.lowest_ts.is_none() {
            self.lowest_ts = Some(time_stamp);
        } else {
            self.lowest_ts = Some(min(self.lowest_ts.unwrap(), time_stamp));
        }

        if time_point == 0 {
            self.zero_ts = Some(time_stamp);
        }

        // initial interval
        if self.observations.len() == 1 {
            //println!("Init case");
            if time_point == 0 { // split initial InfInterval [0,inf] 0 -> [0] [0,inf]
                let left_tp = Literal(time_point, time_stamp);
                let right_tp = InfInterval(time_point+1, time_stamp);

                self.observations[0] = left_tp;
                self.observations.push(right_tp);
            } else { // split initial InfInterval [0,inf] tp -> [0,tp] [tp] [tp,inf]
                let left_tp = Interval(0, time_point-1, 0, time_stamp);
                let middle_tp = Literal(time_point, time_stamp);
                let right_tp = InfInterval(time_point+1, time_stamp);

                self.observations[0] = left_tp;
                self.observations.push(middle_tp);
                self.observations.push(right_tp);
            }
            return;
        }

        // max case
        let length = self.observations.len()-1;
        if inside_tp(&self.observations[length], time_point) {
            //println!("Max case");
            match self.observations[length] {
                InfInterval(a_tp,b_ts) => {
                    if time_point == a_tp { // new lower bound
                        //println!("     Left bound case");
                        let left_tp = Literal(time_point, time_stamp);
                        let right_tp = InfInterval(time_point+1, time_stamp);

                        self.observations[length] = left_tp;
                        self.observations.push(right_tp);
                    } else { // new mid element
                        //println!("     Right bound case");
                        let left_tp = Interval(a_tp, time_point-1, b_ts, time_stamp);
                        let middle_tp = Literal(time_point, time_stamp);
                        let right_tp = InfInterval(time_point+1, time_stamp);

                        self.observations[length] = left_tp;
                        self.observations.push(middle_tp);
                        self.observations.push(right_tp);
                    }
                }
                _ => {} // last element is always InfInterval
            }
            return;
        }

        // min case
        if inside_tp(&self.observations[0], time_point) {
            //println!("Min case");
            match self.observations[0] {
                Interval(a_tp, b_tp, a_ts, b_ts) => {
                    if time_point == a_tp {
                        //println!("     Left bound case");
                        let left_tp = Literal(time_point, time_stamp);
                        let right_tp = Interval(time_point+1, b_tp, time_stamp, b_ts);

                        if self.received_tps.contains(&(time_point+1)) {
                            self.observations[0] = left_tp;
                        } else {
                            self.observations[0] = right_tp;
                            self.observations.insert(0, left_tp);
                        }
                    } else if time_point == b_tp {
                        //println!("     Right bound case");
                        let left_tp = Interval(a_tp, time_point-1, a_ts, time_stamp);
                        let right_tp = Literal(time_point, time_stamp);

                        self.observations[0] = left_tp;
                        self.observations.insert(1, right_tp);
                    } else {
                        //println!("     Middle case");
                        let left_tp = Interval(a_tp, time_point-1, a_ts, time_stamp);
                        let middle_tp = Literal(time_point, time_stamp);
                        let right_tp = Interval(time_point+1, b_tp, time_stamp, b_ts);

                        self.observations[0] = left_tp;
                        self.observations.insert(1,middle_tp);
                        self.observations.insert(2,right_tp);
                    }
                }
                _ => {}
            }
            return;
        }

        // search case
        //println!("Search Case");
        let index = find_index(&self.observations, time_point, true).unwrap();
        match self.observations[index] {
            Interval(a_tp, b_tp, a_ts, b_ts) =>
                if time_point == a_tp {
                    //println!("   Left bound case");
                    let left_tp = Literal(time_point, time_stamp);
                    let right_tp = Interval(time_point+1, b_tp,time_stamp, b_ts);

                    if self.received_tps.contains(&(time_point+1)) {
                        self.observations[index] = left_tp;
                    } else {
                        self.observations[index] = left_tp;
                        self.observations.insert(index+1, right_tp);
                    }
                } else if time_point == b_tp {
                    //println!("   Right bound case");
                    let left_tp = Interval(a_tp, time_point-1, a_ts, time_stamp);
                    let right_tp = Literal(time_point, time_stamp);

                    self.observations[index] = left_tp;
                    self.observations.insert(index+1, right_tp);
                } else {
                    //println!("   Middle case");
                    let left_tp = Interval(a_tp, time_point-1, a_ts, time_stamp);
                    let middle_tp = Literal(time_point, time_stamp);
                    let right_tp = Interval(time_point+1, b_tp, time_stamp, b_ts);

                    self.observations[index] = left_tp;
                    self.observations.insert(index+1,middle_tp);
                    self.observations.insert(index+2,right_tp);
                },
            _ => {}
        }
    }

    fn contains(&mut self, time_point: usize) -> Option<usize> {
        let index = binary_search(&self.observations, time_point, true);
        return if let Some(ind) = index {
            match self.observations[ind] {
                Literal(_tp, ts) => {
                    Some(ts)
                }
                _ => {
                    None
                }
            }
        } else {
            None
        }
    }

    fn associated_interval_tp(&mut self, time_point: usize) -> InfinityIntervalsReturn {
        return if let Some(interval) = binary_search(&self.observations, time_point, true) {
            let ts_interval = self.observations[interval];
            match ts_interval {
                Literal(_, b) => InfinityIntervalsReturn::Literal(b),
                Interval(_, _, a, b) => InfinityIntervalsReturn::Interval(a,b),
                InfInterval(_, _) => Empty
            }
        } else {
           Empty
        }
    }

    fn associated_interval_ts_exact(&mut self, lower_ts: usize, upper_ts: usize) -> InfinityIntervalsReturn {
        //println!("associated_interval_ts:");
        let len = self.observations.len();

        let opt_ind_lower = find_index(&self.observations, lower_ts, false);
        let opt_ind_upper = find_index(&self.observations, upper_ts, false);
        let ind_lower = match opt_ind_lower { None => 0, Some(x) => x };
        let ind_upper = match opt_ind_upper {None => if len > 0 {self.observations.len()-1} else {0}, Some(x) => x};
        //println!("ind_lower {}", ind_lower);
        //println!("ind_upper {}", ind_upper);
        let new_ind_lower = search_lowest(&self.observations, ind_lower, lower_ts, upper_ts, true);
        let new_ind_upper = search_highest(&self.observations, ind_upper, lower_ts, upper_ts);

        if ind_upper == ind_lower && ind_upper+1 == self.observations.len() {
            return Empty
        }

        if new_ind_lower.is_some() && new_ind_upper.is_some() {
            //println!("{:?}", self.observations);
            //println!("Both index exist: {}  {}", new_ind_lower.unwrap(), new_ind_upper.unwrap());
            let (lower_bound_tp, _lower_bound_ts) = match self.observations[new_ind_lower.unwrap()] {
                Literal(a, b) => (a, b),
                _ => {(0,0)} // never reached
            };
            let (upper_bound_tp, _upper_bound_ts) = match self.observations[new_ind_upper.unwrap()] {
                Literal(a, b) => (a, b),
                _ => {(0,0)} // never reached
            };

            if lower_bound_tp != upper_bound_tp {
                InfinityIntervalsReturn::Interval(lower_bound_tp, upper_bound_tp)
            } else {
                InfinityIntervalsReturn::Literal(lower_bound_tp)
            }
        } else if new_ind_lower.is_some() && new_ind_upper.is_none() {
            //println!("Only lower: {}", new_ind_lower.unwrap());
            match self.observations[new_ind_lower.unwrap()] {
                Literal(a, _) => InfinityIntervalsReturn::Literal(a),
                Interval(a, b, _, _) => InfinityIntervalsReturn::Interval(a, b),
                InfInterval(_, _) => Empty
            }
        } else if new_ind_lower.is_none() && new_ind_upper.is_some() {
            //println!("Only upper: {}", new_ind_upper.unwrap());
            match self.observations[new_ind_upper.unwrap()] {
                Literal(a, _) => InfinityIntervalsReturn::Literal(a),
                Interval(a, b, _, _) => InfinityIntervalsReturn::Interval(a, b),
                InfInterval(_, _) => Empty
            }
        } else {
            Empty
        }
    }

    fn associated_interval_ts_inexact(&mut self, lower_ts: usize, upper_ts: usize) -> InfinityIntervalsReturn {
        //println!("associated_interval_ts:");
        let len = self.observations.len();

        let opt_ind_lower = find_index(&self.observations, lower_ts, false);
        let opt_ind_upper = find_index(&self.observations, upper_ts, false);
        let ind_lower = match opt_ind_lower { None => 0, Some(x) => x };
        let ind_upper = match opt_ind_upper {None => if len > 0 {self.observations.len()-1} else {0}, Some(x) => x};
        //println!("ind_lower {}", ind_lower);
        //println!("ind_upper {}", ind_upper);
        let new_ind_lower = search_lowest(&self.observations, ind_lower, lower_ts, upper_ts, false);
        let new_ind_upper = search_highest(&self.observations, ind_upper, lower_ts, upper_ts);

        if ind_upper == ind_lower && ind_upper+1 == self.observations.len() {
            return Empty
        }

        if new_ind_lower.is_some() && new_ind_upper.is_some() {
            //println!("{:?}", self.observations);
            //println!("Both index exist: {}  {}", new_ind_lower.unwrap(), new_ind_upper.unwrap());
            let (lower_bound_tp, _lower_bound_ts) = match self.observations[new_ind_lower.unwrap()] {
                Literal(a, b) => (a, b),
                _ => {(0,0)} // never reached
            };
            let (upper_bound_tp, _upper_bound_ts) = match self.observations[new_ind_upper.unwrap()] {
                Literal(a, b) => (a, b),
                _ => {(0,0)} // never reached
            };

            if lower_bound_tp != upper_bound_tp {
                InfinityIntervalsReturn::Interval(lower_bound_tp, upper_bound_tp)
            } else {
                InfinityIntervalsReturn::Literal(lower_bound_tp)
            }
        } else if new_ind_lower.is_some() && new_ind_upper.is_none() {
            //println!("Only lower: {}", new_ind_lower.unwrap());
            match self.observations[new_ind_lower.unwrap()] {
                Literal(a, _) => InfinityIntervalsReturn::Literal(a),
                Interval(a, b, _, _) => InfinityIntervalsReturn::Interval(a, b),
                InfInterval(_, _) => Empty
            }
        } else if new_ind_lower.is_none() && new_ind_upper.is_some() {
            //println!("Only upper: {}", new_ind_upper.unwrap());
            match self.observations[new_ind_upper.unwrap()] {
                Literal(a, _) => InfinityIntervalsReturn::Literal(a),
                Interval(a, b, _, _) => InfinityIntervalsReturn::Interval(a, b),
                InfInterval(_, _) => Empty
            }
        } else {
            Empty
        }
    }

    fn associated_lower_bound_ts(&mut self, ts: usize) -> InfinityIntervalsReturn {
        let opt_ind = find_index(&self.observations, ts, false);
        let ind = match opt_ind { None => 0, Some(x) => x };
        //println!("Ind {}", ind);
        let new_ind = search_lower_bound(&self.observations, ind, ts);

        if new_ind.is_some() {
            //println!("New ind {}", new_ind.unwrap());
            match self.observations[new_ind.unwrap()] {
                Literal(a, _) => InfinityIntervalsReturn::Literal(a),
                _ => Empty
                /*Interval(a, b, _, _) => InfinityIntervalsReturn::Interval(a, b),
                InfInterval(_, _) => Empty*/
            }
        } else {
            Empty
        }
    }

    fn associated_upper_bound_ts(&mut self, ts: usize) -> InfinityIntervalsReturn {
        let opt_ind = find_index(&self.observations, ts, false);
        let ind = match opt_ind { None => 0, Some(x) => x };
        //println!("Ind {}", ind);

        return match self.observations[ind] {
            Literal(a, _) => {
                //println!("Literal");
                InfinityIntervalsReturn::Literal(a)
            }
            Interval(_, _, low_ts, up_ts) => {
                //println!("Interval");
                if ts == low_ts {
                    //println!("  go left");
                    let mut bound_tp = 0;
                    let mut flagged = false;
                    for i in (0..ind+1).rev() {
                        //let comp = compare_ts(&self.observations[i], ts);
                        if compare_ts(&self.observations[i], ts) == 0 {
                            if is_literal(self.observations[i]) {
                                flagged = true;
                                match self.observations[i] {
                                    Literal(a, _) => { bound_tp = a;}
                                    _ => {}
                                }
                                break;
                            }
                        }else {
                            break
                        }
                    }
                    if flagged {
                        InfinityIntervalsReturn::Literal(bound_tp)
                    } else {
                        Empty
                    }
                } else if ts == up_ts {
                    //println!("  go right");
                    let mut bound_tp = 0;
                    let mut flagged = false;
                    for i in ind+1..self.observations.len() {
                        if compare_ts(&self.observations[i], ts) == 0 {
                            if is_literal(self.observations[i]) {
                                flagged = true;
                                match self.observations[i] {
                                    Literal(a, _) => { bound_tp = a;}
                                    _ => {}
                                }
                                break;
                            }
                        }else {
                            break
                        }
                    }
                    if flagged {
                        InfinityIntervalsReturn::Literal(bound_tp)
                    } else {
                        Empty
                    }
                } else {
                    //println!("  in between");
                    if ind > 0 {
                        match self.observations[ind-1] {
                            Literal(a, _) => InfinityIntervalsReturn::Literal(a),
                            _ => Empty
                        }
                    } else {
                        Empty
                    }
                }
            }
            InfInterval(_, _) => {
                //println!("InfInterval");
                if self.observations.len() > 0 {
                    // element to the left is the upper bound
                    match self.observations[ind-1] {
                        Literal(a, _) => {
                            InfinityIntervalsReturn::Literal(a)
                        }
                        _ => Empty
                    }
                } else {
                    Empty
                }
            }
        }
    }

    fn zero_ts(&mut self) -> Option<usize> {
        self.zero_ts
    }

    fn lowest_ts(&mut self) -> Option<usize> { self.lowest_ts }

    fn start_of_sequence(&mut self, tp: usize) -> bool {
        if self.observations.is_empty() {
            return false
        }

        return if tp == 0 {
            true
        } else {
            let tp_ts = self.associated_interval_tp(tp);
            let cutoff = self.associated_interval_tp(tp-1);

            match cutoff {
                InfinityIntervalsReturn::Literal(cut_off_ts) => {
                    match tp_ts {
                        InfinityIntervalsReturn::Literal(a) => {
                            cut_off_ts < a
                        }
                        _ => false
                    }
                }
                _ => false
            }
        };
    }

    fn end_of_sequence(&mut self, tp: usize) -> bool {
        if self.observations.is_empty() {
            return false
        }

        let tp_ts = self.associated_interval_tp(tp);
        let cutoff = self.associated_interval_tp(tp+1);

        match cutoff {
            InfinityIntervalsReturn::Literal(cut_off_ts) => {
                match tp_ts {
                    InfinityIntervalsReturn::Literal(a) => {
                        cut_off_ts > a
                    }
                    _ => false
                }
            }
            _ => false
        }

    }

    fn clean_up(&mut self, frontier_tp: usize, frontier_ts : usize) {
        if let Some(ind) = binary_search(&self.observations, frontier_tp, true) {
            let cut_off = match self.observations[ind] {
                Literal(_, _) => {
                    // remove all
                    ind as i64
                }
                Interval(_, b_tp, _, b_ts) => {
                    if frontier_tp == b_tp {
                        // remove all
                        ind as i64
                    } else {
                        self.observations[ind] = Interval(frontier_tp, b_tp, frontier_ts, b_ts);
                        ind as i64-1
                    }
                }
                InfInterval(_, _) => {
                    self.observations[ind] = InfInterval(frontier_tp, frontier_ts);
                    ind as i64-1
                }
            };

            let mut counter = 0;
            self.observations.retain(|_interval| {
                let f = counter <= cut_off;
                counter += 1;
                !f
            });
        };
    }
}


#[cfg(test)]
mod test_ds {
    use dataflow_constructor::observation_sequence::{inside_tp, compare, find_index, binary_search, ObservationSequence, TimeSeq, InfinityIntervalsReturn};
    use dataflow_constructor::observation_sequence::InfinityIntervals::{Interval, InfInterval, Literal};
    use dataflow_constructor::observation_sequence::InfinityIntervalsReturn::Empty;

    extern crate itertools;
    use dataflow_constructor::observation_sequence::test_ds::itertools::Itertools;

    #[test]
    fn test_associated_upper() {
        let intervals = vec![
            Literal(0, 0),                      //0
            Literal(1, 1),                      //1
            Interval(2, 2, 1, 3),   //2
            Literal(3, 3),                      //3
            Literal(4, 4),                      //4
            InfInterval(5, 4)];                 //5
        let mut obs = ObservationSequence::init();
        obs.observations = intervals.clone();

        assert_eq!(InfinityIntervalsReturn::Literal(0), obs.associated_upper_bound_ts(0));
        assert_eq!(InfinityIntervalsReturn::Literal(1), obs.associated_upper_bound_ts(1));
        assert_eq!(InfinityIntervalsReturn::Literal(1), obs.associated_upper_bound_ts(2));
        assert_eq!(InfinityIntervalsReturn::Literal(3), obs.associated_upper_bound_ts(3));
        assert_eq!(InfinityIntervalsReturn::Literal(4), obs.associated_upper_bound_ts(4));
        assert_eq!(InfinityIntervalsReturn::Literal(4), obs.associated_upper_bound_ts(10));
        assert_eq!(InfinityIntervalsReturn::Literal(4), obs.associated_upper_bound_ts(1000));

        println!("-----------------");

        let intervals = vec![
            Literal(0, 0),                                      // 0
            Literal(1, 1),                                      // 1
            Literal(2, 1),                                      // 2
            Literal(3, 1),                                      // 3
            Interval(4,99,1,100),                   // 4
            Literal(100, 100),                                  // 5
            Interval(101, 199,100, 200),            // 6
            Literal(200, 200),                                  // 7
            InfInterval(201, 200)];                             // 8
        let mut obs = ObservationSequence::init();
        obs.observations = intervals.clone();

        assert_eq!(InfinityIntervalsReturn::Literal(0), obs.associated_upper_bound_ts(0));
        assert_eq!(InfinityIntervalsReturn::Literal(3), obs.associated_upper_bound_ts(1));
        assert_eq!(InfinityIntervalsReturn::Literal(3), obs.associated_upper_bound_ts(2));
        assert_eq!(InfinityIntervalsReturn::Literal(3), obs.associated_upper_bound_ts(3));
        assert_eq!(InfinityIntervalsReturn::Literal(3), obs.associated_upper_bound_ts(4));
        assert_eq!(InfinityIntervalsReturn::Literal(3), obs.associated_upper_bound_ts(50));
        assert_eq!(InfinityIntervalsReturn::Literal(100), obs.associated_upper_bound_ts(100));
        assert_eq!(InfinityIntervalsReturn::Literal(100), obs.associated_upper_bound_ts(150));
        assert_eq!(InfinityIntervalsReturn::Literal(200), obs.associated_upper_bound_ts(200));
        assert_eq!(InfinityIntervalsReturn::Literal(200), obs.associated_upper_bound_ts(1000));

        println!("-----------------");

        let intervals = vec![
            Literal(0, 0),                                      // 0
            Literal(1, 1),                                      // 1
            Literal(2, 1),                                      // 2
            Interval(3, 4, 1, 1),                   // 3
            Literal(5,1),                                       // 4
            Interval(6,99,1,100),                   // 5
            Literal(100, 100),                                  // 6
            Interval(101, 199,100, 200),            // 7
            Literal(200, 200),                                  // 8
            InfInterval(201, 200)];                             // 9
        let mut obs = ObservationSequence::init();
        obs.observations = intervals.clone();

        assert_eq!(InfinityIntervalsReturn::Literal(0), obs.associated_upper_bound_ts(0));
        assert_eq!(InfinityIntervalsReturn::Literal(5), obs.associated_upper_bound_ts(1));
        assert_eq!(InfinityIntervalsReturn::Literal(5), obs.associated_upper_bound_ts(2));
        assert_eq!(InfinityIntervalsReturn::Literal(5), obs.associated_upper_bound_ts(3));
        assert_eq!(InfinityIntervalsReturn::Literal(5), obs.associated_upper_bound_ts(4));
        assert_eq!(InfinityIntervalsReturn::Literal(5), obs.associated_upper_bound_ts(50));
        assert_eq!(InfinityIntervalsReturn::Literal(100), obs.associated_upper_bound_ts(100));
        assert_eq!(InfinityIntervalsReturn::Literal(100), obs.associated_upper_bound_ts(150));
        assert_eq!(InfinityIntervalsReturn::Literal(200), obs.associated_upper_bound_ts(200));
        assert_eq!(InfinityIntervalsReturn::Literal(200), obs.associated_upper_bound_ts(1000));

        let intervals = vec![
            Literal(0, 0),
            Interval(1, 2, 0, 3),
            Literal(3, 3),
            Literal(4, 4),
            InfInterval(5, 4)];

        let mut obs = ObservationSequence::init();
        obs.observations = intervals.clone();

        assert_eq!(InfinityIntervalsReturn::Literal(0), obs.associated_upper_bound_ts(2));
    }

    #[test]
    fn test_associated_low() {
        let intervals = vec![
            Literal(0, 0),                                      // 0
            Literal(1, 1),                                      // 1
            Interval(2,99,1,100),                   // 2
            Literal(100, 100),                                  // 3
            Interval(101, 199,100, 200),            // 4
            Literal(200, 200),                                  // 5
            InfInterval(201, 200)];                             // 6
        let mut obs = ObservationSequence::init();
        obs.observations = intervals.clone();

        // TS 1 to 100 => TP 1 to 100
        // TS 1 to 150 => TP 1 to 100
        assert_eq!(InfinityIntervalsReturn::Literal(0), obs.associated_lower_bound_ts(0));
        assert_eq!(InfinityIntervalsReturn::Literal(1), obs.associated_lower_bound_ts(1));

        assert_eq!(InfinityIntervalsReturn::Literal(100), obs.associated_lower_bound_ts(80));
        assert_eq!(InfinityIntervalsReturn::Literal(100), obs.associated_lower_bound_ts(100));

        assert_eq!(InfinityIntervalsReturn::Literal(200), obs.associated_lower_bound_ts(200));

        assert_eq!(Empty, obs.associated_lower_bound_ts(251)); // empty
        assert_eq!(Empty, obs.associated_lower_bound_ts(201)); // empty

        let intervals = vec![
            Literal(0, 0),                                      // 0
            Literal(1, 1),
            Literal(2, 1),                                       // 1
            Literal(3, 1),
            Interval(4,99,1,100),                   // 2
            Literal(100, 100),                                  // 3
            Interval(101, 199,100, 200),            // 4
            Literal(200, 200),                                  // 5
            InfInterval(201, 200)];                             // 6
        let mut obs = ObservationSequence::init();
        obs.observations = intervals.clone();

        assert_eq!(InfinityIntervalsReturn::Interval(1,3), obs.associated_interval_ts_exact(1, 1));
    }

    #[test]
    fn test_start_of_sequence() {
        let intervals = vec![
            Literal(0, 0),
            Literal(1, 1),
            Interval(2, 98,1,99),
            Literal(99, 99),
            Literal(100, 100),
            Interval(101, 199,100, 200),
            Literal(200, 200),
            InfInterval(201, 200)];
        let mut obs = ObservationSequence::init();
        obs.observations = intervals.clone();

        assert!(obs.start_of_sequence(0)); //true
        assert!(obs.start_of_sequence(1)); //true

        assert!(!obs.start_of_sequence(3)); //false
        assert!(!obs.start_of_sequence(90));//false

        assert!(obs.start_of_sequence(100)); //true
        assert!(!obs.start_of_sequence(200)); //false
    }

    #[test]
    fn test_end_of_sequence() {
        let intervals = vec![
            Literal(0, 0),
            Literal(1, 1),
            Interval(2, 98,1,99),
            Literal(99, 99),
            Literal(100, 100),
            Interval(101, 199,100, 200),
            Literal(200, 200),
            InfInterval(201, 200)];
        let mut obs = ObservationSequence::init();
        obs.observations = intervals.clone();

        assert!(obs.end_of_sequence(0)); //true
        assert!(!obs.end_of_sequence(1)); //false

        assert!(!obs.end_of_sequence(3)); //false
        assert!(!obs.end_of_sequence(90));//false

        assert!(obs.end_of_sequence(99)); //true
        assert!(!obs.end_of_sequence(100)); //false
        assert!(!obs.end_of_sequence(200)); //false
    }

    #[test]
    fn test_associated_interval_ts() {
        let intervals = vec![
            Literal(0, 0),                                      // 0
            Literal(1, 1),                                      // 1
            Interval(2,99,1,100),                   // 2
            Literal(100, 100),                                  // 3
            Interval(101, 199,100, 200),            // 4
            Literal(200, 200),                                  // 5
            InfInterval(201, 200)];                             // 6
        let mut obs = ObservationSequence::init();
        obs.observations = intervals.clone();

        // TS 1 to 100 => TP 1 to 100
        // TS 1 to 150 => TP 1 to 100
        assert_eq!(InfinityIntervalsReturn::Interval(1, 100), obs.associated_interval_ts_exact(1, 100));
        assert_eq!(InfinityIntervalsReturn::Interval(1, 100), obs.associated_interval_ts_exact(1, 150));

        assert_eq!(InfinityIntervalsReturn::Literal(0), obs.associated_interval_ts_exact(0, 0));
        assert_eq!(InfinityIntervalsReturn::Literal(1), obs.associated_interval_ts_exact(1, 1));
        assert_eq!(InfinityIntervalsReturn::Literal(1), obs.associated_interval_ts_exact(1, 2));
        assert_eq!(InfinityIntervalsReturn::Literal(1), obs.associated_interval_ts_exact(1, 99));

        assert_eq!(InfinityIntervalsReturn::Literal(100), obs.associated_interval_ts_exact(80, 100));
        assert_eq!(InfinityIntervalsReturn::Literal(100), obs.associated_interval_ts_exact(100, 100));
        assert_eq!(InfinityIntervalsReturn::Literal(100), obs.associated_interval_ts_exact(100, 102));

        assert_eq!(InfinityIntervalsReturn::Interval(0, 200), obs.associated_interval_ts_exact(0, 250)); // 0 to 200
        assert_eq!(InfinityIntervalsReturn::Interval(0, 100), obs.associated_interval_ts_exact(0, 199)); // 0 to 100
        assert_eq!(InfinityIntervalsReturn::Interval(0, 100), obs.associated_interval_ts_exact(0, 101)); // 0 to 100
        assert_eq!(InfinityIntervalsReturn::Interval(0, 1), obs.associated_interval_ts_exact(0, 3)); // 0 to 100

        assert_eq!(InfinityIntervalsReturn::Literal(200), obs.associated_interval_ts_exact(199, 200)); // 200
        assert_eq!(InfinityIntervalsReturn::Literal(200), obs.associated_interval_ts_exact(200, 200)); // 200
        assert_eq!(InfinityIntervalsReturn::Literal(200), obs.associated_interval_ts_exact(200, 201));

        assert_eq!(Empty, obs.associated_interval_ts_exact(251, 255)); // empty
        assert_eq!(Empty, obs.associated_interval_ts_exact(201, 201)); // empty
        // fully contained in the query interval
    }

    #[test]
    fn clean_up_test() {
        let intervals = vec![InfInterval(200, 200)];
        let mut obs = ObservationSequence::init();
        obs.observations = intervals;

        obs.clean_up(500, 500);
        assert_eq!(obs.observations, vec![InfInterval(500, 500)]);

        let intervals = vec![Literal(0, 0),
            Literal(1, 1), Interval(2,99,1,100),
            Literal(100, 100), Interval(101, 199,100, 200),
            Literal(200, 200), InfInterval(201, 200)];
        obs.observations = intervals;

        obs.clean_up(100, 100);
        assert_eq!(obs.observations, vec![(Interval(101, 199,100, 200)), (Literal(200, 200)), (InfInterval(201, 200))]);

        let intervals = vec![Literal(0, 0),
                             Literal(1, 1), Interval(2,99,1,100),
                             Literal(100, 100), Interval(101, 199,100, 200),
                             Literal(200, 200), InfInterval(201, 200)];
        obs.observations = intervals;

        obs.clean_up(150, 150);
        assert_eq!(obs.observations, vec![(Interval(150, 199,150, 200)), (Literal(200, 200)), (InfInterval(201, 200))]);
    }

    #[test]
    fn associated_interval_tp_test() {
        let intervals = vec![
            (Literal(0, 0)),
            (Literal(1, 1)),
            (Interval(2,99,1,100)),
            (Literal(100, 100)),
            (Interval(101, 199,100, 200)),
            (Literal(200, 200)),
            (InfInterval(201, 200))
        ];

        let mut obs = ObservationSequence::init();
        obs.observations = intervals;

        assert_eq!(obs.associated_interval_tp(0), InfinityIntervalsReturn::Literal(0));
        assert_eq!(obs.associated_interval_tp(1), InfinityIntervalsReturn::Literal(1));
        assert_eq!(obs.associated_interval_tp(50), InfinityIntervalsReturn::Interval(1, 100));
        assert_eq!(obs.associated_interval_tp(100), InfinityIntervalsReturn::Literal(100));
        assert_eq!(obs.associated_interval_tp(150), InfinityIntervalsReturn::Interval(100, 200));
        assert_eq!(obs.associated_interval_tp(200), InfinityIntervalsReturn::Literal(200));
        assert_eq!(obs.associated_interval_tp(250), Empty);

        let intervals1 = vec![
            (Literal(0, 10)),
            (Literal(1, 11)),
            (Literal(2, 11)),
            (Literal(3, 12)),
            (Literal(4, 24)),
            (Literal(5, 100)),
            (InfInterval(6,100))
        ];

        obs.observations = intervals1;

        assert_eq!(obs.associated_interval_tp(0), InfinityIntervalsReturn::Literal(10));
        assert_eq!(obs.associated_interval_tp(1), InfinityIntervalsReturn::Literal(11));
        assert_eq!(obs.associated_interval_tp(2), InfinityIntervalsReturn::Literal(11));
        assert_eq!(obs.associated_interval_tp(3), InfinityIntervalsReturn::Literal(12));
        assert_eq!(obs.associated_interval_tp(4), InfinityIntervalsReturn::Literal(24));
        assert_eq!(obs.associated_interval_tp(5), InfinityIntervalsReturn::Literal(100));
        assert_eq!(obs.associated_interval_tp(250), Empty);
    }

    #[test]
    fn contains_test() {
        let intervals = vec![
            (Literal(0, 0)),
            (Literal(1, 1)),
            (Interval(2,99,1,100)),
            (Literal(100,100)),
            (Interval(101,199,100, 200)),
            (Literal(200, 200)),
            (InfInterval(201,200))
        ];

        let mut obs = ObservationSequence::init();
        obs.observations = intervals;

        assert_eq!(obs.contains(0), Some(0));
        assert_eq!(obs.contains(1), Some(1));
        assert_eq!(obs.contains(50), None);
        assert_eq!(obs.contains(100), Some(100));
        assert_eq!(obs.contains(150), None);
        assert_eq!(obs.contains(200), Some(200));
        assert_eq!(obs.contains(250), None);

        let intervals1 = vec![
            (Literal(0,10)),
            (Literal(1,11)),
            (Literal(2,11)),
            (Literal(3,12)),
            (Literal(4,24)),
            (Literal(5,100)),
            (InfInterval(6,100))
        ];

        obs.observations = intervals1;

        assert_eq!(obs.contains(0), Some(10));
        assert_eq!(obs.contains(1), Some(11));
        assert_eq!(obs.contains(2), Some(11));
        assert_eq!(obs.contains(3), Some(12));
        assert_eq!(obs.contains(4), Some(24));
        assert_eq!(obs.contains(5), Some(100));
        assert_eq!(obs.contains(250), None);
    }

    #[test]
    fn insert_property() {
        let correct = vec![
            (Literal(0,0)),
            (Literal(1,1)),
            (Interval(2,99,1,100)),
            (Literal(100,100)),
            (Interval(101, 199,100, 200)),
            (Literal(200,200)),
            (InfInterval(201,200))
        ];

        let items = vec![(0,0), (1,1), (100,100), (200,200)];
        for perm in items.iter().permutations(items.len()) {
            let mut obs_seq = ObservationSequence::init();
            for (tp,ts) in perm {
                obs_seq.insert(tp.clone(), ts.clone());
            }
            assert_eq!(obs_seq.observations, correct);
        }
    }

    #[test]
    fn insert_property1() {
        let correct = vec![
            (Literal(0,0)),
            (Literal(1,1)),
            (Literal(2,2)),
            (Literal(3,3)),
            (Literal(4,4)),
            (Literal(5,5)),
            (Literal(6,6)),
            (Literal(7,7)),
            (Literal(8,8)),
            (Literal(9,9)),
            (InfInterval(10,9))
        ];

        let items = vec![(0,0), (1,1), (2,2), (3,3), (4,4), (5,5), (6,6), (7,7), (8,8), (9,9)];
        for perm in items.iter().permutations(items.len()) {
            let mut obs_seq = ObservationSequence::init();
            for (tp,ts) in perm {
                obs_seq.insert(tp.clone(), ts.clone());
            }
            assert_eq!(obs_seq.observations, correct);
        }
    }

    #[test]
    fn insert_property2() {
        let correct = vec![
            (Literal(0,10)),
            (Literal(1,11)),
            (Literal(2,11)),
            (Literal(3,12)),
            (Literal(4,24)),
            (Literal(5,100)),
            (InfInterval(6,100))
        ];

        let items = vec![(0,10), (1,11), (2,11), (3,12), (4,24), (5,100)];
        for perm in items.iter().permutations(items.len()) {
            let mut obs_seq = ObservationSequence::init();
            for (tp,ts) in perm {
                obs_seq.insert(tp.clone(), ts.clone());
            }
            assert_eq!(obs_seq.observations, correct);
        }
    }

    #[test]
    fn insert_property4() {
        let correct = vec![
            (Literal(0,10)),
            (Literal(1,10)),
            (Interval(2,99,10,1000)),
            (Literal(100,1000)),
            (Interval(101, 199,1000, 2000)),
            (Literal(200,2000)),
            (InfInterval(201,2000))
        ];

        let items = vec![(0,10), (1,10), (100,1000), (200,2000)];
        for perm in items.iter().permutations(items.len()) {
            let mut obs_seq = ObservationSequence::init();
            for (tp,ts) in perm {
                obs_seq.insert(tp.clone(), ts.clone());
            }
            assert_eq!(obs_seq.observations, correct);
        }
    }

    #[test]
    fn obs_seq_init() {
        let obs_seq = ObservationSequence::init();
        assert_eq!(obs_seq.observations, vec![InfInterval(0,0)])
    }

    #[test]
    fn obs_seq_insert_init_case() {
        let mut obs_seq = ObservationSequence::init();
        obs_seq.insert(0, 1);
        assert_eq!(obs_seq.observations, vec![(Literal(0,1)), (InfInterval(1,1))]);

        obs_seq = ObservationSequence::init();
        obs_seq.insert(10, 11);
        assert_eq!(obs_seq.observations, vec![(Interval(0,9,0, 11)),(Literal(10,11)), (InfInterval(11,11))]);
    }

    #[test]
    fn test_binary_search() {
        let empty = vec![];
        let single_literal = vec![(Literal(10,10))];
        let single_interval  = vec![(Interval(10, 20,10, 20))];
        let single_infinterval  = vec![(InfInterval(10,10))];

        // lesser
        assert_eq!(binary_search(&empty, 0,true), None);
        assert_eq!(binary_search(&single_literal, 0,true), None);
        assert_eq!(binary_search(&single_interval, 0,true), None);
        assert_eq!(binary_search(&single_infinterval, 0,true), None);

        // equal
        assert_eq!(binary_search(&single_literal, 10,true), Some(0));
        assert_eq!(binary_search(&single_interval, 10,true), Some(0));
        assert_eq!(binary_search(&single_interval, 15,true), Some(0));
        assert_eq!(binary_search(&single_infinterval, 10,true), Some(0));

        // greater
        assert_eq!(binary_search(&single_literal, 100,true), None);
        assert_eq!(binary_search(&single_interval, 100,true), None);
        assert_eq!(binary_search(&single_interval, 150,true), None);

        let mixed_interval = vec![
            (Interval(0, 10,10, 100)),
            (Literal(11,110)),
            (Literal(12, 120)),
            (Interval(13, 99,130, 990)),
            (InfInterval(100,1000))];

        assert_eq!(binary_search(&mixed_interval, 0,true), Some(0));
        assert_eq!(binary_search(&mixed_interval, 5,true), Some(0));
        assert_eq!(binary_search(&mixed_interval, 10,true), Some(0));

        assert_eq!(binary_search(&mixed_interval, 11,true), Some(1));
        assert_eq!(binary_search(&mixed_interval, 12,true), Some(2));

        assert_eq!(binary_search(&mixed_interval, 13,true), Some(3));
        assert_eq!(binary_search(&mixed_interval, 50,true), Some(3));
        assert_eq!(binary_search(&mixed_interval, 99,true), Some(3));

        assert_eq!(binary_search(&mixed_interval, 100,true), Some(4));
        assert_eq!(binary_search(&mixed_interval, 100000,true), Some(4));
        assert_eq!(binary_search(&mixed_interval, 100000000,true), Some(4));
    }

    #[test]
    fn test_binary_search_ts() {
        let empty = vec![];
        let single_literal = vec![(Literal(10,10))];
        let single_interval = vec![(Interval(10, 20,10, 20))];
        let single_infinterval = vec![(InfInterval(10,10))];

        // lesser
        assert_eq!(binary_search(&empty, 0, false), None);
        assert_eq!(binary_search(&single_literal, 0, false), None);
        assert_eq!(binary_search(&single_interval, 0, false), None);
        assert_eq!(binary_search(&single_infinterval, 0, false), None);

        // equal
        assert_eq!(binary_search(&single_literal, 10,false), Some(0));
        assert_eq!(binary_search(&single_interval, 10,false), Some(0));
        assert_eq!(binary_search(&single_interval, 15,false), Some(0));
        assert_eq!(binary_search(&single_infinterval, 10,false), Some(0));

        // greater
        assert_eq!(binary_search(&single_literal, 100, false), None);
        assert_eq!(binary_search(&single_interval, 100, false), None);
        assert_eq!(binary_search(&single_interval, 150, false), None);

        let mixed_interval = vec![
            (Interval(0, 10,10, 100)),
            (Literal(11,110)),
            (Literal(12,120)),
            (Interval(13, 99,130, 990)),
            (InfInterval(100,1000))];

        assert_eq!(binary_search(&mixed_interval, 0, false), None);
        assert_eq!(binary_search(&mixed_interval, 5, false), None);

        assert_eq!(binary_search(&mixed_interval, 10, false), Some(0));
        assert_eq!(binary_search(&mixed_interval, 100, false), Some(0));
        assert_eq!(binary_search(&mixed_interval, 11, false), Some(0));
        assert_eq!(binary_search(&mixed_interval, 12, false), Some(0));

        assert_eq!(binary_search(&mixed_interval, 110, false), Some(1));
        assert_eq!(binary_search(&mixed_interval, 120, false), Some(2));

        assert_eq!(binary_search(&mixed_interval, 130, false), Some(3));
        assert_eq!(binary_search(&mixed_interval, 500, false), Some(3));
        assert_eq!(binary_search(&mixed_interval, 990, false), Some(3));

        assert_eq!(binary_search(&mixed_interval, 100000, false), Some(4));
        assert_eq!(binary_search(&mixed_interval, 100000000, false), Some(4));
    }

    #[test]
    fn test_find_index() {
        let empty  = vec![];
        let single_literal = vec![(Literal(10,10))];
        let single_interval = vec![(Interval(10, 20,10, 20))];
        let single_infinterval = vec![(InfInterval(10,10))];

        // lesser
        assert_eq!(find_index(&empty, 0, true), None);
        assert_eq!(find_index(&single_literal, 0, true), Some(0));
        assert_eq!(find_index(&single_interval, 0, true), Some(0));
        assert_eq!(find_index(&single_infinterval, 0,true), Some(0));

        // equal
        assert_eq!(find_index(&single_literal, 10,true), Some(0));
        assert_eq!(find_index(&single_interval, 10,true), Some(0));
        assert_eq!(find_index(&single_interval, 15,true), Some(0));
        assert_eq!(find_index(&single_infinterval, 10,true), Some(0));

        // greater
        assert_eq!(find_index(&single_literal, 100,true), Some(1));
        assert_eq!(find_index(&single_interval, 100,true), Some(1));
        assert_eq!(find_index(&single_interval, 150,true), Some(1));

        let mixed_interval = vec![
            (Interval(0, 10,10, 100)),
            (Literal(11,110)),
            (Literal(12,120)),
            (Interval(13, 99,130, 990)),
            (InfInterval(100,1000))];

        assert_eq!(find_index(&mixed_interval, 0,true), Some(0));
        assert_eq!(find_index(&mixed_interval, 5,true), Some(0));
        assert_eq!(find_index(&mixed_interval, 10,true), Some(0));

        assert_eq!(find_index(&mixed_interval, 11,true), Some(1));
        assert_eq!(find_index(&mixed_interval, 12,true), Some(2));

        assert_eq!(find_index(&mixed_interval, 13,true), Some(3));
        assert_eq!(find_index(&mixed_interval, 50,true), Some(3));
        assert_eq!(find_index(&mixed_interval, 99,true), Some(3));

        assert_eq!(find_index(&mixed_interval, 100,true), Some(4));
        assert_eq!(find_index(&mixed_interval, 100000,true), Some(4));
        assert_eq!(find_index(&mixed_interval, 100000000,true), Some(4));
    }

    #[test]
    fn test_compare() {
        // less
        assert_eq!(compare(&Literal(1,1), 0), -1);
        assert_eq!(compare(&Literal(100, 100), 99), -1);
        assert_eq!(compare(&Interval(10,20, 10, 20), 9), -1);
        assert_eq!(compare(&Interval(102,200, 102, 200), 21), -1);
        assert_eq!(compare(&InfInterval(1,1), 0), -1);
        assert_eq!(compare(&InfInterval(10,10), 9), -1);

        // equal
        assert_eq!(compare(&Literal(1,1), 1), 0);
        assert_eq!(compare(&Literal(100,100), 100), 0);
        assert_eq!(compare(&Interval(10,20, 10,20), 15), 0);
        assert_eq!(compare(&Interval(102,200, 102, 200), 121), 0);
        assert_eq!(compare(&InfInterval(1,1), 1), 0);
        assert_eq!(compare(&InfInterval(10,10), 19), 0);

        // greater
        assert_eq!(compare(&Literal(1,1), 2), 1);
        assert_eq!(compare(&Literal(100,100), 101), 1);
        assert_eq!(compare(&Interval(10,20, 10, 20), 115), 1);
        assert_eq!(compare(&Interval(102,200, 102, 200), 1121), 1);
        // no InfInterval cases since there is no timestamp greater inf
    }

    #[test]
    fn test_inside() {
        // not cases
        assert_eq!(inside_tp(&Literal(1,1), 0), false);
        assert_eq!(inside_tp(&Literal(100, 100), 101), false);
        assert_eq!(inside_tp(&Interval(10,20, 10, 20), 21), false);
        assert_eq!(inside_tp(&Interval(102,200, 102, 200), 21), false);
        assert_eq!(inside_tp(&InfInterval(1,1), 0), false);
        assert_eq!(inside_tp(&InfInterval(10,10), 9), false);

        // corner cases
        assert_eq!(inside_tp(&Literal(1,1), 1), true);
        assert_eq!(inside_tp(&Literal(100,100), 100), true);
        assert_eq!(inside_tp(&Interval(10,20, 10, 20), 20), true);
        assert_eq!(inside_tp(&Interval(102,200, 102, 200), 102), true);
        assert_eq!(inside_tp(&InfInterval(1,1), 1), true);
        assert_eq!(inside_tp(&InfInterval(10,10), 10), true);

        // inside cases
        assert_eq!(inside_tp(&Literal(10000, 10000), 10000), true);
        assert_eq!(inside_tp(&Literal(0,0), 0), true);
        assert_eq!(inside_tp(&Interval(10,20, 10, 20), 15), true);
        assert_eq!(inside_tp(&Interval(102,200, 102, 200), 151), true);
        assert_eq!(inside_tp(&InfInterval(1,1), 1), true);
        assert_eq!(inside_tp(&InfInterval(10,10), 10), true);
    }

    #[test]
    fn t() {
        let intervals = vec![Interval(0, 0, 0, 1), Literal(1, 1), Literal(2, 2), Literal(3, 3), Literal(4, 4), Literal(5, 5), Literal(6, 6), Literal(7, 7), Literal(8, 8), Literal(9, 9), Literal(10, 10), Literal(11, 11), Literal(12, 12), Literal(13, 13), Literal(14, 14), Literal(15, 15), Literal(16, 16), Literal(17, 17), Literal(18, 18), Literal(19, 19), Literal(20, 20), Literal(21, 21), Literal(22, 22), Literal(23, 23), Literal(24, 24), Literal(25, 25), Literal(26, 26), Literal(27, 27), Literal(28, 28), Literal(29, 29), Literal(30, 30), Literal(31, 31), Literal(32, 32), Literal(33, 33), Literal(34, 34), Literal(35, 35), Literal(36, 36), Literal(37, 37), Literal(38, 38), Literal(39, 39), Literal(40, 40), Literal(41, 41), Literal(42, 42), Literal(43, 43), Literal(44, 44), Literal(45, 45), Literal(46, 46), Literal(47, 47), Literal(48, 48), Literal(49, 49), InfInterval(50, 49)];
        let mut obs = ObservationSequence::init();
        obs.observations = intervals.clone();

        assert_eq!(InfinityIntervalsReturn::Interval(1, 8), obs.associated_interval_ts_exact(1, 8));
        obs.insert(0,0);
        assert_eq!(InfinityIntervalsReturn::Interval(1, 8), obs.associated_interval_ts_exact(1, 8));
    }
}