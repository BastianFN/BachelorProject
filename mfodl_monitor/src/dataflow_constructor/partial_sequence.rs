#![allow(dead_code)]

use std::collections::HashMap;
use std::fmt;
use std::fmt::Formatter;
use dataflow_constructor::partial_sequence::Intervals::{Interval, Literal};
use parser::formula_syntax_tree::{Constant};
use timeunits::TimeInterval;

pub trait SatisfactionDs {
    fn empty(since_or_until : bool) -> Self;
    fn clone(self: &mut Self) -> Self;
    fn add_subscriber(self: &mut Self, record: Vec<Constant>, bound: usize);
    fn insert(self: &mut Self, time_point : usize);
    fn slide_bound(&mut self, new_bound: usize);
    fn single_output(self: &mut Self, tuple: Vec<Constant>) -> Vec<(Vec<Constant>, Vec<usize>, usize)>;
    fn output(self: &mut Self) -> Vec<(Vec<Constant>, Vec<usize>, usize)>;
    fn clean_up(self: &mut Self, time_point: usize);
    fn safe_remove_subscriber(self: &mut Self, tp_to_ts: &mut HashMap<usize, usize>, interval: TimeInterval, gap_tp: usize);
    fn contains(self: &mut Self, tp: usize) -> bool;
}

#[derive(Hash, Eq, Clone, Debug, PartialEq, PartialOrd, Ord, Abomonation, Copy)]
pub enum Intervals {
    Literal(u64),
    Interval(u64, u64),
}

impl fmt::Display for Intervals {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Literal(i) => i.to_string(),
                Interval(a, b) => format!("({}, {})", a.to_string(), b.to_string()).to_string(),
            }
        )
    }
}

#[derive(Hash, Eq, Clone, Debug, PartialEq, PartialOrd, Ord, Abomonation)]
pub struct Subscriber {
    pub(crate) bound: u64,
    pub(crate) returned_satisfactions: u64,
}

#[derive(Default, Clone)]
pub struct PartialSequence {
    pub(crate) satisfactions: Vec<Intervals>,
    pub(crate) since_or_until: bool,
    pub(crate) subscriber: HashMap<Vec<Constant>, Vec<(usize, usize)>>,
}



pub fn compare(interval : &Intervals, time_point : usize) -> i8 {
    match interval {
        Interval(a, b) => if time_point < *a as usize { -1 } else if time_point > *b as usize { 1 } else { 0 },
        Literal(a) => if time_point < *a as usize { -1 } else if time_point > *a as usize { 1 } else { 0 },
    }
}

pub fn compare_subscriber(subs : &Subscriber, time_point : usize) -> i8 {
    if time_point < subs.bound as usize {-1} else if time_point > subs.bound as usize { 1 } else {0}
}


pub fn find_index_neg(tmp : &Vec<Intervals>, k: usize) -> Option<usize> {
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

pub fn find_index_subs(tmp : &Vec<Subscriber>, k: usize) -> Option<usize> {
    let mut start = 0i64;
    let mut end = tmp.len() as i64 - 1;
    // Traverse the search space
    while start <= end {
        let mid = (start + end) / 2;
        // If k is found
        if mid < 0 {
            return None
        }

        let comp = compare_subscriber(&tmp[mid as usize], k);

        if comp == 0 {
            return Some(mid as usize);
        } else if comp == 1 {
            start = mid + 1;
        } else {
            end = mid - 1;
        }
    }
    return Some((end+1) as usize);
}



impl SatisfactionDs for PartialSequence {
    fn empty(mode : bool) -> Self{
        let x: HashMap<Vec<Constant>, Vec<(usize, usize)>> = HashMap::with_capacity(8);
        let sats : Vec<Intervals> = Vec::with_capacity(8); // arbitrary value

        PartialSequence {
            subscriber: x,
            satisfactions: sats,
            since_or_until: mode
        }
    }

    fn clone(self: &mut Self) -> Self {
        PartialSequence {
            subscriber: self.subscriber.clone(),
            satisfactions: self.satisfactions.clone(),
            since_or_until: self.since_or_until.clone()
        }
    }

    fn add_subscriber(self: &mut Self, record: Vec<Constant>, bound: usize) {
        self.subscriber.entry(record).or_insert_with(|| Vec::new()).push((bound, bound));
    }

    // should remain functional
    fn insert(&mut self, time_point: usize) {
        // add directly if empty
        if self.satisfactions.is_empty() {
            self.satisfactions.push(Literal(time_point as u64));
            return;
        }

        let length = self.satisfactions.len() - 1;

        let prev_max = &&self.satisfactions[length];
        let max_elem = compare(prev_max, time_point);
        // if new tp is bigger than prev max
        // and new tp is prev_max+1 merge
        // else append literal at the end
        if max_elem == 1 {
            let new_elem = match prev_max {
                Literal(a) => {
                    if *a == (time_point - 1) as u64 {
                        Interval(*a, time_point as u64)
                    } else {
                        Literal(time_point as u64)
                    }
                }
                Interval(a, b) => {
                    if *b == (time_point - 1) as u64 {
                        Interval(*a, time_point as u64)
                    } else {
                        Literal(time_point as u64)
                    }
                }
            };
            match new_elem {
                Literal(_) => self.satisfactions.push(Literal(time_point as u64)),
                Interval(_, _) => self.satisfactions[length] = new_elem,
            }
            return;
        }

        let prev_min = &&self.satisfactions[0];
        let min_elem = compare(prev_min, time_point);
        // if new tp is smaller than curr_min
        // and new tp is prev_min-1 then merge
        // else insert literal
        if min_elem == -1 {
            let new_elem = match prev_min {
                Literal(a) => {
                    if *a == (time_point + 1) as u64 {
                        Interval(time_point as u64, *a)
                    } else {
                        Literal(time_point as u64)
                    }
                }
                Interval(a, b) => {
                    if *a == (time_point + 1) as u64 {
                        Interval(time_point as u64, *b)
                    } else {
                        Literal(time_point as u64)
                    }
                }
            };
            match new_elem {
                Literal(_) => self.satisfactions.insert(0, Literal(time_point as u64)),
                Interval(_, _) => self.satisfactions[0] = new_elem,
            }
            return;
        }

        // function fails for a number smaller than the current minimum but the case should be handled above [hopefully :)]

        // find the index where to enter new_i
        // case 1 no connection to left right insert literal
        // case 2 left connection merge new tp with new_i
        // case 3 left and right connection merge into new_i+1 and remove new_i-1
        // case 4 no connect add at new index

        // all index should be in the interval (0, n)
        //let index = find_index(self.satisfactions.clone(), time_point);
        let index = match find_index_neg(&self.satisfactions, time_point) {
            None => 0,
            Some(a) => a,
        };

        // element already exists
        if index == 0 {
            return;
        }

        let left_connect = compare(&self.satisfactions[index - 1], time_point - 1) == 0;
        let right_connect = compare(&self.satisfactions[index], time_point + 1) == 0;

        if left_connect && right_connect {
            let left_border = match &self.satisfactions[index - 1] {
                Literal(a) => *a,
                Interval(a, _) => *a,
            };
            let right_border = match &self.satisfactions[index] {
                Literal(a) => *a,
                Interval(_, a) => *a,
            };
            self.satisfactions[index] = Interval(left_border, right_border);
            self.satisfactions.remove(index - 1);
        } else if left_connect {
            let new_elem = match &self.satisfactions[index - 1] {
                Literal(a) => Interval(*a, time_point as u64),
                Interval(a, _) => Interval(*a, time_point as u64),
            };
            self.satisfactions[index - 1] = new_elem;
        } else if right_connect {
            let new_elem = match &self.satisfactions[index] {
                Literal(a) => Interval(time_point as u64, *a),
                Interval(_, b) => Interval(time_point as u64, *b),
            };
            self.satisfactions[index] = new_elem;
        } else {
            self.satisfactions.insert(index, Literal(time_point as u64));
        }
    }

    fn slide_bound(&mut self, new_bound: usize) {
        if self.since_or_until {
            slide_lower_bound(self, new_bound)
        }else {
            slide_upper_bound(self, new_bound)
        }
    }

    fn single_output(&mut self, tuple: Vec<Constant>) -> Vec<(Vec<Constant>, Vec<usize>, usize)> {
        if self.satisfactions.is_empty() { return vec![] };
        let length = self.satisfactions.len();
        let func = if self.since_or_until {output_since} else {output_until};
        let mut res = Vec::with_capacity(1);

        let subs = self.subscriber.entry(tuple.clone()).or_default();
        let mut sub = subs[subs.len()-1];
        if length == 1 {
            let elem = self.satisfactions[0].clone();
            let (result_range, new_returned_sats) = func(elem, sub.0 as u64, sub.1 as u64);
            sub.1 = new_returned_sats as usize;
            res.push((tuple.clone(), result_range, sub.0))
        }else {
            let b = if self.since_or_until {sub.0 + 1} else {if sub.0 > 1 { sub.0 - 1 } else { 0 }};
            if let Some(ind) = find_index_neg(&self.satisfactions, b) {
                if ind != length {
                    let (result_range, new_returned_sats) = func(self.satisfactions[ind].clone(), sub.0 as u64, sub.1 as u64);
                    sub.1 = new_returned_sats as usize;
                    res.push((tuple.clone(), result_range, sub.0))
                }
            }
        }

        return res;
    }

    fn output(&mut self) -> Vec<(Vec<Constant>, Vec<usize>, usize)> {
        if self.satisfactions.is_empty() { return vec![] };
        let length = self.satisfactions.len();
        let mut hash_map_length = 0;
        self.subscriber.iter().for_each(|(_, x)|
            hash_map_length += x.len()
        );
        let mut res = Vec::with_capacity(hash_map_length);
        let func = if self.since_or_until {output_since} else {output_until};

        //println!("Number of subs {}", hash_map_length);

        if length == 1 {
            let elem = self.satisfactions[0].clone();
            for (sub_rec, subs) in self.subscriber.iter_mut() {
                for sub in subs {
                    let (result_range, new_returned_sats) = func(elem, sub.0 as u64, sub.1 as u64);
                    sub.1 = new_returned_sats as usize;
                    res.push((sub_rec.clone(), result_range, sub.0));
                }
            }
        }else {
            for (sub_rec, subs) in self.subscriber.iter_mut() {
                for sub in subs {
                    let b = if self.since_or_until {sub.0 + 1} else {if sub.0 > 1 { sub.0 - 1 } else { 0 }};
                    if let Some(ind) = find_index_neg(&self.satisfactions, b) {
                        if ind != length {
                            let (result_range, new_returned_sats) = func(self.satisfactions[ind].clone(), sub.0 as u64, sub.1 as u64);
                            sub.1 = new_returned_sats as usize;
                            res.push((sub_rec.clone(), result_range, sub.0));
                        }
                    }
                }
            }
        }
        return res
    }

    fn clean_up(self: &mut Self, time_point: usize) {
        // function called only called on SatisfactionsDs used to store all alpha satisfactions
        // remove everything to the left of including time_point like sliding (but always left)
        let mut tmp_ds = PartialSequence::empty(true);
        tmp_ds.satisfactions = self.satisfactions.clone();
        tmp_ds.slide_bound(time_point);
        self.satisfactions = tmp_ds.satisfactions;
    }

    fn safe_remove_subscriber(self: &mut Self, tp_to_ts: &mut HashMap<usize, usize>, interval: TimeInterval, gap_tp: usize) {
        let mode = self.since_or_until;
        for (_, subs) in self.subscriber.iter_mut() {
            subs.retain(|sub| {
                let current_ts = *tp_to_ts.entry(gap_tp).or_default();
                let bound_ts = *tp_to_ts.entry(sub.0).or_default();

                let res = if mode {
                    match interval.get_end() {
                        None => false,
                        Some(b) => (bound_ts as i64) < (current_ts as i64) - (b as i64)
                    }
                } else {
                    bound_ts < current_ts
                };

                !res
            });
        }
    }

    fn contains(self: &mut Self, tp: usize) -> bool {
        if let Some(index) = find_index_neg(&self.satisfactions, tp) {
            if index == self.satisfactions.len() {
                false
            } else {
                match self.satisfactions[index] {
                    Literal(a) => {tp == a as usize }
                    Interval(a, b) => {tp >= a as usize && tp <= b as usize}
                }
            }
        } else { false }
    }
}

pub(crate) fn contains(satisfactions: Vec<Intervals>, tp : usize) -> bool{
    if satisfactions.is_empty() {
        return false
    }
    if let Some(index) = find_index_neg(&satisfactions, tp) {
        if index == satisfactions.len() {
            false
        } else {
            match satisfactions[index] {
                Literal(a) => {tp == a as usize }
                Interval(a, b) => {tp >= a as usize && tp <= b as usize}
            }
        }
    } else { false }
}


pub(crate) fn safe_to_remove_sub(mode : bool, bound : usize, returned_satisfactions : usize, gap_tp : usize) -> bool {
    // the passed value gap_tp is a gap or cut off, given that all values until the cut off are outputted the subscriber served it's purpose and can be discarded
    //println!("curr_tp {}       bound {}      diff {}", gap_tp, bound, (gap_tp - bound) as i64);
    if mode {
        // gap is to right of the bound
        let is_relevant = gap_tp >= bound;
        // produced output until gap
        let is_connected = if gap_tp > 0 {returned_satisfactions == (gap_tp - 1)} else {false};

        if is_relevant && is_connected {
            return true
        }
    }else {
        // gap is to left of the bound
        let is_relevant = gap_tp <= bound;
        // produced output until gap
        let is_connected = returned_satisfactions <= (gap_tp + 1);

        if is_relevant && is_connected {
            return true
        }
    }
    return false
}


pub(crate) fn output_until(tail :Intervals,  bound: u64, returned_satisfactions: u64) -> (Vec<usize>, u64){
    // check if the first element is connected to the lower bound in other words consecutive
    let last_elem_connect = match tail {
        Literal(a) => if a == bound {true} else {a+1 == bound },
        Interval(_, b) => if b >= bound  {true} else {b+1 >= bound},
    };

    // get the former smallest element
    let longest_seq = match tail {
        Literal(a) => a,
        Interval(a, _) => a,
    };

    // return an empty vector if there is no connection between upper bound and minimum
    // if the last element is connected and the new longest sequence is smaller than the smallest returned satisfaction
    return if last_elem_connect && (longest_seq < returned_satisfactions) {
        let lower_elem = if returned_satisfactions >= 1 {returned_satisfactions - 1} else {0};
        (vec![longest_seq as usize, lower_elem as usize], longest_seq)
    } else {
        (vec![], returned_satisfactions)
    };
}

pub(crate) fn output_since(head :Intervals, bound: u64, returned_satisfactions: u64) -> (Vec<usize>, u64) {
    let first_elem_connect = match head {
        // new case a-1 can be negative!
        Literal(a) => if a == bound {true} else { if a >= 1 {a-1 == bound } else {false}},
        Interval(a, b) => if a <= bound {true} else { if a >= 1 {a-1 == bound } else {(a < bound) && (bound <= b)}},
    };

    //println!("------------ First elem connect {} {:?}   ({},{})", first_elem_connect, head, bound, returned_satisfactions);

    // get the former largest element
    let longest_seq = match head {
        Literal(a) => a,
        Interval(_, b) => b,
    };

    // return an empty vector if there is no connection between lower bound and minimum
    // if the first element is connected and the new longest sequence is bigger than the highest returned satisfaction
    return if first_elem_connect && (longest_seq > returned_satisfactions) {
        let lower_elem = returned_satisfactions + 1;
        //println!("Output");
        (vec![lower_elem as usize, longest_seq as usize], longest_seq)
    } else {
        (vec![], returned_satisfactions)
    };
}

pub(crate) fn slide_lower_bound(ds: &mut PartialSequence, new_bound: usize) {
    // slide lower bound to the right
    if ds.satisfactions.is_empty() {
        return;
    }

    // if first element is bigger than new bound do no garbage collection
    let no_clean_flag = match ds.satisfactions[0] {
        Literal(a) => a > new_bound as u64,
        Interval(a, _) => a > new_bound as u64,
    };

    // clean satisfactions if non-empty
    if !no_clean_flag {
        // new bound +1 because we only want to consider values bigger than new_bound not equal
        let new_ind = match find_index_neg(&ds.satisfactions, new_bound + 1) {
            None => 0,
            Some(a) => a,
        };

        // if all the elements are smaller than the new_bound
        if new_ind >= ds.satisfactions.len() {
            ds.satisfactions = Vec::new();
        } else {
            let new_first = match ds.satisfactions[new_ind] {
                Literal(a) => Literal(a),
                // if new bound is in the middle -> adjust interval or
                // if new bound is the last element of the interval change to Literal
                Interval(a, b) => {
                    if (a < new_bound as u64) && ((new_bound as u64) < b) {
                        Interval((new_bound + 1) as u64, b)
                    } else if (new_bound as u64) + 1 == b {
                        Literal(b)
                    } else {
                        Interval(a, b)
                    }
                }
            };

            ds.satisfactions[new_ind] = new_first;
            let mut counter = 0;
            ds.satisfactions.retain(|_| {
                let t = counter < new_ind;
                counter += 1;
                !t
            });
        }
    }
}

pub(crate) fn slide_upper_bound(ds: &mut PartialSequence, new_bound: usize) {
    // slide upper bound to the left
    if ds.satisfactions.is_empty() {
        return;
    }

    let length = ds.satisfactions.len() - 1;

    // if last element is smaller than new bound do no garbage collection
    // if first element is bigger than new bound do no garbage collection
    let no_clean_flag = match ds.satisfactions[length] {
        Literal(a) => a < new_bound as u64,
        Interval(_, b) => b < new_bound as u64,
    };

    // clean satisfactions if non-empty
    if !no_clean_flag {
        let new_ind = match find_index_neg(&ds.satisfactions, if new_bound > 0 {new_bound - 1} else {0}) {
            None => 0,
            Some(a) => a,
        };

        let smaller_than_min = match ds.satisfactions[0] {
            Literal(a) => new_bound < a as usize,
            Interval(a, _) => new_bound < a as usize,
        };

        // if all the elements are bigger than the new_bound (index comparison not values)
        if new_ind == 0 && smaller_than_min {
            ds.satisfactions = Vec::new();
        } else {
            let new_first = match ds.satisfactions[new_ind] {
                Literal(a) => Literal(a),
                // if new bound is in the middle -> adjust interval or
                // if new bound is the last element of the interval change too
                Interval(a, b) =>
                // adapt interval if new bound in between
                    {
                        if (a < new_bound as u64) && ((new_bound as u64) < b) {
                            // a == new b
                            if a == (new_bound - 1) as u64 {
                                Literal(a)
                            } else {
                                Interval(a, (new_bound - 1) as u64)
                            }
                        }
                        // new bound is one bigger than the leftmost element, convert Interval to literal
                        else if (new_bound as u64) - 1 == a {
                            Literal(a)
                        }
                        // new bound is equal to b
                        else if (new_bound as u64) == b {
                            if a == (new_bound - 1) as u64 {
                                Literal(a)
                            } else {
                                Interval(a, b - 1)
                            }
                        }
                        // unscattered interval
                        else {
                            Interval(a, b)
                        }
                    }
            };

            ds.satisfactions[new_ind] = new_first;

            let mut counter = 0;
            ds.satisfactions.retain(|_| {
                let t = counter > new_ind;
                counter += 1;
                !t
            });
        }
    }
}

fn print_vec(vec1: Vec<Intervals>) {
    print!("[");
    for v in vec1.iter() {
        print!("{} ", v);
    }
    println!("]")
}

#[cfg(test)]
mod test_ds {
    
    use std::collections::HashMap;
    use std::vec;

    extern crate itertools;
    use dataflow_constructor::partial_sequence::test_ds::itertools::Itertools;
    use dataflow_constructor::partial_sequence::{Intervals, PartialSequence, SatisfactionDs};
    use dataflow_constructor::partial_sequence::Intervals::{Interval, Literal};
    use parser::formula_syntax_tree::{Constant};
    use parser::formula_syntax_tree::Constant::Int;

    fn interval_eq(int1: Intervals, int2: Intervals) -> bool {
        let res = match int1 {
            Literal(a) => match int2 {
                Literal(a_) => a == a_,
                Interval(_, _) => false,
            },
            Interval(a, b) => match int2 {
                Literal(_) => false,
                Interval(a_, b_) => a == a_ && b == b_,
            },
        };

        return res;
    }

    fn interval_vec_eq(vec1: Vec<Intervals>, vec2: Vec<Intervals>) -> bool {
        let l1 = vec1.len();
        let l2 = vec2.len();

        if l1 != l2 {
            return false;
        };

        for i in 0..l1 {
            if !interval_eq(vec1[i].clone(), vec2[i].clone()) {
                return false;
            }
        }

        return true;
    }

    fn print_vec(vec1: Vec<Intervals>) {
        print!("[");
        for v in vec1.iter() {
            print!("{} ", v);
        }
        println!("]")
    }

    #[test]
    fn interval_eq_test() {
        // 2 == 2
        assert!(interval_eq(Literal(2), Literal(2)));
        // 3 != 2
        assert!(!interval_eq(Literal(3), Literal(2)));

        // (2,3) != 2
        assert!(!interval_eq(Interval(2, 3), Literal(2)));
        // 3 != (2,4)
        assert!(!interval_eq(Literal(3), Interval(2, 4)));

        // (2,5) == (2,5)
        assert!(interval_eq(
            Interval(2, 5),
            Interval(2, 5)
        ));
        // (1,2) != (2,3)
        assert!(!interval_eq(
            Interval(1, 2),
            Interval(2, 3)
        ));
    }

    #[test]
    fn interval_vec_eq_test() {
        // [2,2] == [2,2]
        assert!(interval_vec_eq(
            vec![Literal(2), Literal(2)],
            vec![Literal(2), Literal(2)]
        ));
        // [2,3] != [2,2]
        assert!(!interval_vec_eq(
            vec![Literal(2), Literal(3)],
            vec![Literal(2), Literal(2)]
        ));

        // [(2,3), 5] == [(2,3), 5]
        assert!(interval_vec_eq(
            vec![Interval(2, 3), Literal(5)],
            vec![Interval(2, 3), Literal(5)]
        ));
        // [2, (4,12)] != [(4,12), 2]
        assert!(!interval_vec_eq(
            vec![Literal(2), Interval(4, 12)],
            vec![Interval(4, 12), Literal(2)]
        ));

        // [(2,3), (2,3), (2,3), (2,3), (2,3)] == [(2,3), (2,3), (2,3), (2,3), (2,3)]
        assert!(interval_vec_eq(
            vec![Interval(2, 3); 5],
            vec![Interval(2, 3); 5]
        ));
        // [(2,3), (2,3)] == [(2,3), (2,3), (2,3), (2,3), (2,3)]
        assert!(!interval_vec_eq(
            vec![Interval(2, 2); 2],
            vec![Interval(2, 2); 5]
        ));
    }

    #[test]
    fn insert_property_in_order_6() {
        let correct = vec![Interval(0,5)];

        let items = [0,1,2,3,4,5];
        for perm in items.iter().permutations(items.len()) {
            let mut ds = PartialSequence::empty(true);
            let mut ds1 = PartialSequence::empty(false);
            for i in perm {
                ds.insert(i.clone());
                ds1.insert(i.clone())
            }
            assert_eq!(ds.satisfactions, correct);
            assert_eq!(ds1.satisfactions, correct);
        }
    }

    #[test]
    fn insert_property_in_order_gap_6() {
        let correct = vec![Interval(0,2), Interval(4,6)];

        let items = [0,1,2,4,5,6];
        for perm in items.iter().permutations(items.len()) {
            let mut ds = PartialSequence::empty(true);
            let mut ds1 = PartialSequence::empty(false);
            for i in perm {
                ds.insert(i.clone());
                ds1.insert(i.clone())
            }
            assert_eq!(ds.satisfactions, correct);
            assert_eq!(ds1.satisfactions, correct);
        }
    }

    #[test]
    fn insert_property_in_order_gap2_6() {
        let correct = vec![Literal(0), Literal(2), Interval(4,6), Literal(10)];

        let items = [0,2,4,5,6,10];
        for perm in items.iter().permutations(items.len()) {
            let mut ds = PartialSequence::empty(true);
            let mut ds1 = PartialSequence::empty(false);
            for i in perm {
                ds.insert(i.clone());
                ds1.insert(i.clone())
            }
            assert_eq!(ds.satisfactions, correct);
            assert_eq!(ds1.satisfactions, correct);
        }
    }

    #[test]
    fn insert_property_in_order_8() {
        let correct = vec![Interval(0,7)];

        let items = [0,1,2,3,4,5,6,7];
        for perm in items.iter().permutations(items.len()) {
            let mut ds = PartialSequence::empty(true);
            let mut ds1 = PartialSequence::empty(false);
            for i in perm {
                ds.insert(i.clone());
                ds1.insert(i.clone())
            }
            assert_eq!(ds.satisfactions, correct);
            assert_eq!(ds1.satisfactions, correct);
        }
    }

    #[test]
    fn insert_property_in_order_gap_8() {
        let correct = vec![Interval(0,2), Interval(4,6), Interval(8,9)];

        let items = [0,1,2,4,5,6,8,9];
        for perm in items.iter().permutations(items.len()) {
            let mut ds = PartialSequence::empty(true);
            let mut ds1 = PartialSequence::empty(false);
            for i in perm {
                ds.insert(i.clone());
                ds1.insert(i.clone())
            }
            assert_eq!(ds.satisfactions, correct);
            assert_eq!(ds1.satisfactions, correct);
        }
    }

    #[test]
    fn insert_property_in_order_gap2_8() {
        let correct = vec![Literal(0), Literal(2), Interval(4,6), Interval(10, 12)];

        let items = [0,2,4,5,6,10, 11, 12];
        for perm in items.iter().permutations(items.len()) {
            let mut ds = PartialSequence::empty(true);
            let mut ds1 = PartialSequence::empty(false);
            for i in perm {
                ds.insert(i.clone());
                ds1.insert(i.clone())
            }
            assert_eq!(ds.satisfactions, correct);
            assert_eq!(ds1.satisfactions, correct);
        }
    }


    #[test]
    fn insert_property_in_order_10() {
        let correct = vec![Interval(0,9)];

        let items = [0,1,2,3,4,5,6,7,8,9];
        for perm in items.iter().permutations(items.len()) {
            let mut ds = PartialSequence::empty(true);
            let mut ds1 = PartialSequence::empty(false);
            for i in perm {
                ds.insert(i.clone());
                ds1.insert(i.clone())
            }
            assert_eq!(ds.satisfactions, correct);
            assert_eq!(ds1.satisfactions, correct);
        }
    }

    #[test]
    fn insert_property_in_order_gap_10() {
        let correct = vec![Interval(0,2), Interval(4,6), Interval(8,9), Interval(100, 101)];

        let items = [0,1,2,4,5,6,8,9,100,101];
        for perm in items.iter().permutations(items.len()) {
            let mut ds = PartialSequence::empty(true);
            let mut ds1 = PartialSequence::empty(false);
            for i in perm {
                ds.insert(i.clone());
                ds1.insert(i.clone())
            }
            assert_eq!(ds.satisfactions, correct);
            assert_eq!(ds1.satisfactions, correct);
        }
    }

    #[test]
    fn insert_property_in_order_gap2_10() {
        let correct = vec![Literal(0), Literal(2), Interval(4,6), Interval(10, 12), Literal(14), Literal(16)];

        let items = [0,2,4,5,6,10,11,12,14,16];
        for perm in items.iter().permutations(items.len()) {
            let mut ds = PartialSequence::empty(true);
            let mut ds1 = PartialSequence::empty(false);
            for i in perm {
                ds.insert(i.clone());
                ds1.insert(i.clone())
            }
            assert_eq!(ds.satisfactions, correct);
            assert_eq!(ds1.satisfactions, correct);
        }
    }

    // output test
    #[test]
    fn output_property_test_1_sub() {
        let items = [0,1,2,3,4,5];
        for perm in items.iter().permutations(items.len()) {
            let mut ds = PartialSequence::empty(true);
            let mut ds1 = PartialSequence::empty(false);

            ds.add_subscriber(vec![Int(1)], 0);
            ds1.add_subscriber(vec![Int(2)], 5);

            let mut ds_out = vec![];
            let mut ds1_out = vec![];

            for i in perm {
                ds.insert(i.clone());
                ds1.insert(i.clone());

                ds_out.append(&mut ds.output());
                ds1_out.append(&mut ds1.output());
            }

            assert_eq!(vec![1,2,3,4,5], order_output(ds_out).entry(vec![Int(1)]).or_default().clone());
            assert_eq!(vec![0,1,2,3,4], order_output(ds1_out).entry(vec![Int(2)]).or_default().clone());
        }
    }

    #[test]
    fn output_property_test_2_sub() {
        let items = [0,1,2,3,4,5];
        for perm in items.iter().permutations(items.len()) {
            let mut ds_1 = PartialSequence::empty(true);
            let mut ds_2 = PartialSequence::empty(true);
            let mut ds1_3 = PartialSequence::empty(false);
            let mut ds1_4 = PartialSequence::empty(false);

            ds_1.add_subscriber(vec![Int(1)], 0);
            ds_2.add_subscriber(vec![Int(2)], 2);
            ds1_3.add_subscriber(vec![Int(3)], 5);
            ds1_4.add_subscriber(vec![Int(4)], 3);

            let mut ds_1_out = vec![];
            let mut ds_2_out = vec![];
            let mut ds1_3_out = vec![];
            let mut ds1_4_out = vec![];

            for i in perm {
                ds_1.insert(i.clone());
                ds_2.insert(i.clone());
                ds1_3.insert(i.clone());
                ds1_4.insert(i.clone());


                ds_1_out.append(&mut ds_1.output());
                ds_2_out.append(&mut ds_2.output());
                ds1_3_out.append(&mut ds1_3.output());
                ds1_4_out.append(&mut ds1_4.output());
            }

            let mut res_since = order_output(ds_1_out);
            let mut res_since_1 = order_output(ds_2_out);
            let mut res_until = order_output(ds1_3_out);
            let mut res_until_1 = order_output(ds1_4_out);

            assert_eq!(vec![1,2,3,4,5], res_since.entry(vec![(Int(1))]).or_default().clone());
            assert_eq!(vec![3,4,5], res_since_1.entry(vec![(Int(2))]).or_default().clone());
            assert_eq!(vec![0,1,2,3,4], res_until.entry(vec![(Int(3))]).or_default().clone());
            assert_eq!(vec![0,1,2], res_until_1.entry(vec![(Int(4))]).or_default().clone());
        }
    }

    #[test]
    fn unzip_output_test() {
        let correct = vec![0,1,2,3,4,5];

        let example1 = unzip_output(vec![vec![1, 3], vec![0,0], vec![4,4], vec![], vec![5,5]]);
        assert_eq!(correct, example1);

        let _example2 = unzip_output(vec![vec![0, 5]]);
        assert_eq!(correct, example1);
    }

    #[test]
    fn order_output_test() {
        let correct = vec![0,1,2,3,4,5];
        let input = vec![
            (vec![Int(1)], vec![1,3], 0),
            (vec![Int(2)], vec![0, 5], 0),
            (vec![Int(1)], vec![0,0], 0),
            (vec![Int(2)], vec![], 0),
            (vec![Int(1)], vec![4,4], 0),
            (vec![Int(3)], vec![0,3], 0),
            (vec![Int(1)], vec![5,5], 0),
            (vec![Int(3)], vec![4,5], 0),
        ];

        let mut res = order_output(input);

        assert_eq!(correct, res.entry(vec![(Int(1))]).or_default().clone());
        assert_eq!(correct, res.entry(vec![(Int(2))]).or_default().clone());
        assert_eq!(correct, res.entry(vec![(Int(3))]).or_default().clone());
    }


    fn order_output(res : Vec<(Vec<Constant>, Vec<usize>, usize)>) -> HashMap<Vec<Constant>, Vec<usize>> {
        let mut hash : HashMap<Vec<Constant>, Vec<Vec<usize>>> = HashMap::new();
        for (valuation, vec, _bound) in res {
            hash.entry(valuation).or_default().push(vec);
        }

        let mut res_hash : HashMap<Vec<Constant>, Vec<usize>> = HashMap::new();
        for (k, vec) in hash {
            res_hash.insert(k, unzip_output(vec));
        }

        return res_hash;
    }


    fn unzip_output(res : Vec<Vec<usize>>) -> Vec<usize> {
        let mut new_res = vec![];
        for ve in res {
            if ve.len() > 1 {
                if ve[0] == ve[1] {
                    new_res.push(ve[0])
                }else {
                    for v in ve[0]..ve[1]+1 {
                        new_res.push(v);
                    }
                }
            }
        }
        new_res.sort();
        return new_res
    }

    #[test]
    fn contains_test() {
        let data = vec![Interval(1, 7), Literal(9), Interval(15, 56), Literal(58), Interval(60, 78)];
        let mut ds = PartialSequence::empty(true);
        ds.satisfactions = data.clone();

        assert!(ds.contains(1));
        assert!(ds.contains(5));
        assert!(!ds.contains(8));
        assert!(ds.contains(9));

        assert!(ds.contains(30));
        assert!(!ds.contains(57));
        assert!(ds.contains(58));

        assert!(ds.contains(65));
        assert!(ds.contains(78));

        assert!(!ds.contains(100));
    }


    #[test]
    fn slide_test_since() {
        let data = vec![Interval(1,7), Literal(9), Interval(15, 56), Literal(58), Interval(60,78)];
        let mut ds = PartialSequence::empty(true);
        ds.satisfactions = data.clone();

        // remove elements that fall out of the interval
        // clean all elements that are smaller or equal to 7
        ds.slide_bound(7);
        assert_eq!(
            ds.satisfactions,
            vec![Literal(9), Interval(15, 56), Literal(58), Interval(60, 78)]
        );

        ds.satisfactions = data.clone();
        ds.slide_bound(56);
        assert_eq!(ds.satisfactions, vec![Literal(58), Interval(60, 78)]);

        ds.satisfactions = data.clone();
        ds.slide_bound(57);
        assert_eq!(ds.satisfactions, vec![Literal(58), Interval(60,78)]);

        // remove all elements if the new bound is higher than all elements
        ds.satisfactions = data.clone();
        ds.slide_bound(100);
        assert_eq!(ds.satisfactions, vec![]);

        //let mut ds1 = NaiveSatisfactionDs { lower_bound: 0, satisfactions: vec![], returned_satisfactions : 0 };

        let mut ds1 = PartialSequence::empty(true);
        ds1.satisfactions = data.clone();
        ds1.slide_bound(78);
        assert_eq!(ds1.satisfactions, vec![]);

        // keep all elements
        let data1 = vec![Interval(4,7), Literal(9), Interval(15, 56), Literal(58), Interval(60,78)];
        let mut ds2 = PartialSequence::empty(true);
        ds2.satisfactions = data1.clone();
        ds2.slide_bound(2);
        assert_eq!(
            ds2.satisfactions,
            vec![
                Interval(4, 7),
                Literal(9),
                Interval(15, 56),
                Literal(58),
                Interval(60, 78)
            ]
        );

        let mut ds2 = PartialSequence::empty(true);
        ds2.satisfactions = data1.clone();
        ds2.slide_bound(0);
        assert_eq!(
            ds2.satisfactions,
            vec![
                Interval(4, 7),
                Literal(9),
                Interval(15, 56),
                Literal(58),
                Interval(60, 78)
            ]
        );

        let data2 = vec![Literal(0), Literal(3)];
        let mut ds3 = PartialSequence::empty(true);
        ds3.satisfactions = data2.clone();
        ds3.slide_bound(2);
        assert_eq!(ds3.satisfactions, vec![Literal(3)]);

        let data3 = vec![Literal(0), Literal(2)];
        let mut ds4 = PartialSequence::empty(true);
        ds4.satisfactions = data3.clone();
        ds4.slide_bound(1);
        assert_eq!(ds4.satisfactions, vec![Literal(2)]);
    }

    #[test]
    fn slide_test_until() {
        let data = vec![Interval(1,7), Literal(9), Interval(15, 56), Literal(58), Interval(60,78)];
        let mut ds = PartialSequence::empty(false);
        ds.satisfactions = data.clone();

        // remove elements that fall out of the interval
        // clean all elements that are smaller or equal to 7
        ds.slide_bound(57);
        assert_eq!(ds.satisfactions, vec![Interval(1,7), Literal(9), Interval(15, 56)]);
        ds.satisfactions = data.clone();

        ds.slide_bound(56);
        assert_eq!(
            ds.satisfactions,
            vec![Interval(1, 7), Literal(9), Interval(15, 55)]
        );

        ds.satisfactions = data.clone();
        ds.slide_bound(100);
        assert_eq!(ds.satisfactions, data.clone());

        // remove all elements if the new bound is higher than all elements
        ds.satisfactions = data.clone();
        ds.slide_bound(16);
        assert_eq!(
            ds.satisfactions,
            vec![Interval(1, 7), Literal(9), Literal(15)]
        );

        ds.slide_bound(15);
        assert_eq!(
            ds.satisfactions,
            vec![Interval(1, 7), Literal(9), Literal(15)]
        );

        let mut ds1 = PartialSequence::empty(false);
        ds1.satisfactions = data.clone();
        ds1.slide_bound(78);
        assert_eq!(
            ds1.satisfactions,
            vec![
                Interval(1, 7),
                Literal(9),
                Interval(15, 56),
                Literal(58),
                Interval(60, 77)
            ]
        );

        // keep all elements
        let data1 = vec![Interval(4,7), Literal(9), Interval(15, 56), Literal(58), Interval(60,78)];
        let mut ds2 = PartialSequence::empty(false);
        ds2.satisfactions = data1.clone();
        ds2.slide_bound(10);
        assert_eq!(ds2.satisfactions, vec![Interval(4, 7), Literal(9)]);

        let mut ds2 = PartialSequence::empty(false);
        ds2.satisfactions = data1.clone();
        ds2.slide_bound(7);
        assert_eq!(ds2.satisfactions, vec![Interval(4, 6)]);
    }

    #[test]
    fn clean_up_test() {
        // given a time point of an advanced frontier or gap remove every satisfaction <= the given time point
        let data = vec![Interval(1,7), Literal(9), Interval(15, 56), Literal(58), Interval(60,78)];
        let mut ds_since = PartialSequence::empty(true);
        ds_since.satisfactions = data.clone();
        let mut ds_until = PartialSequence::empty(false);
        ds_until.satisfactions = data.clone();
        ds_until.slide_bound(58);

        // both receive it is safe to drop everything including 0
        ds_since.clean_up(0);
        ds_until.clean_up(0);

        assert_eq!(ds_since.satisfactions, data.clone());
        assert_eq!(ds_until.satisfactions, vec![Interval(1,7), Literal(9), Interval(15, 56), Literal(58)]);

        assert_eq!(ds_since.satisfactions, data.clone());
        assert_eq!(ds_until.satisfactions, vec![Interval(1,7), Literal(9), Interval(15, 56), Literal(58)]);

        // both receive the notification that time point 8 is a gap
        ds_since.clean_up(8);
        ds_until.clean_up(8);

        assert_eq!(ds_since.satisfactions, vec![Literal(9), Interval(15, 56), Literal(58), Interval(60,78)]);
        assert_eq!(ds_until.satisfactions, vec![Literal(9), Interval(15, 56), Literal(58)]);

        // both receive it is safe to drop everything including 20
        ds_since.clean_up(20);
        ds_until.clean_up(20);

        assert_eq!(ds_since.satisfactions, vec![Interval(21, 56), Literal(58), Interval(60,78)]);
        assert_eq!(ds_until.satisfactions, vec![Interval(21, 56), Literal(58)]);

        // both receive it is safe to drop everything including 100
        ds_since.clean_up(100);
        ds_until.clean_up(100);

        assert_eq!(ds_since.satisfactions, vec![]);
        assert_eq!(ds_until.satisfactions, vec![]);
    }
}
