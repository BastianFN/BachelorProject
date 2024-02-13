#![allow(unused_macros)]
#![allow(dead_code)]

use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::iter::FromIterator;
// use timely::dataflow::channels::pact::Exchange;
use Formula::Fact;
use ::{Formula, TS};
// use dataflow_constructor::types::Record as Record;
// use dataflow_constructor::types::FlowValues as klowValues;
use parser::formula_syntax_tree::Arg::Cst;
use parser::formula_syntax_tree::build_fact_int;
use parser::formula_syntax_tree::Constant::Str;

/*
use dataflow_constructor::types::{FlowValues::Data as Data, FlowValues::MetaData as MetaData};
*/

pub fn default_start_time() -> TS {
    TS::new(0)
}

pub fn default_end_time() -> TS {
    TS::INFINITY
}

pub fn start_time() -> TS {
    TS::new(0)
}

pub fn end_time() -> TS {
    TS::new(1)
}

pub fn test_formula(input : Option<Vec<i32>>) -> Formula {
    match input {
        None => Fact("p".into(), vec![Cst(Str("foo".into()))]),
        Some(x) => build_fact_int("x", x)
    }
}

macro_rules! key_exchange_non_temporal {
    ($indices:expr) => {{
        let is = $indices.clone();
        PactExchange::new(move |event: &Record| match event {
            Data(_, rec) => calculate_hash(&split_keys_ref(rec, &is)),
            _ => 0,
        })
    }};
}

pub const NUM_WORKERS: usize = 4;

pub const TEST_FACT: &'static str = "p(\'foo\')";

pub const CONJ_NEG_ERROR: &str = "Negation on left hand side of conjunction is not allowed";
pub const TYPES_PRINT_ERROR: &str = "Error while trying to print string for formula";

pub fn get_diff<T: Eq + Hash + Clone>(fst: Vec<T>, snd: Vec<T>) -> usize {
    let expected: HashSet<T> = HashSet::from_iter(fst.iter().cloned());
    let actual: HashSet<T> = HashSet::from_iter(snd.iter().cloned());
    expected.symmetric_difference(&actual).count()
}

pub fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}
