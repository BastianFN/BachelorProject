use std::fmt::{Formatter, self};
use parser::formula_syntax_tree::Constant;

#[derive(Abomonation, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FlowValues {
    Data(bool, Vec<Constant>),
    // meta data is bool, bool; first indicates if it is a timestamp (true) or end of stream (false),
    // second bool is either true for True stream or false for False false stream
    MetaData(bool, bool),
    // Output(ts, data)
}

#[derive(Abomonation, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TimeFlowValues {
    Timestamp(usize),
    EOS
}

pub type Record = FlowValues;

pub struct Profiling {
    pub(crate) op_name: String,
    pub(crate) worker_id: u64,
    pub(crate) aggregated_time: u128,
    pub(crate) received_events: u64,
}

impl fmt::Display for Profiling {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", format!("\nOperation {} at worker {}: \naggregated time in micros {} \nreceived events {}\n", self.op_name.to_string(), self.worker_id.to_string(), self.aggregated_time.to_string(), self.received_events.to_string()).to_string())
    }
}

pub fn default_options() -> OperatorOptions {
    let mut ops = OperatorOptions::new();
    ops.set_output_mode(0);
    ops.set_deduplication(false);
    ops
}

pub struct OperatorOptions {
    workers: usize,
    deduplication: bool,
    step: usize,
    output_mode: usize,
    output_file: Option<String>,
    output_batch: usize
}

impl OperatorOptions {
    // Empty constructor
    pub fn new() -> Self {
        Self {
            workers: 1,
            deduplication: false,
            step: 1000,
            output_mode: 1,
            output_file: None,
            output_batch: 1
        }
    }

    pub fn clone(&self) -> OperatorOptions {
        Self {
            workers: self.workers,
            deduplication: self.deduplication,
            step: self.step,
            output_mode: self.output_mode,
            output_file: self.output_file.clone(),
            output_batch: self.output_batch
        }
    }

    // Getters
    pub fn get_workers(&self) -> usize {
        self.workers
    }

    pub fn get_output_batch(&self) -> usize {
        self.output_batch
    }

    pub fn get_deduplication(&self) -> bool {
        self.deduplication
    }

    pub fn get_step(&self) -> usize {
        self.step
    }

    pub fn get_output_mode(&self) -> usize {
        self.output_mode
    }

    pub fn get_output_file(&self) -> Option<String> {
        self.output_file.clone()
    }

    // Setters
    pub fn set_workers(&mut self, workers: usize) {
        self.workers = workers;
    }

    pub fn set_output_batch(&mut self, output_batch: usize) {
        self.output_batch = output_batch;
    }

    pub fn set_deduplication(&mut self, deduplication: bool) {
        self.deduplication = deduplication;
    }

    pub fn set_step(&mut self, step: usize) {
        self.step = step;
    }

    pub fn set_output_mode(&mut self, output_mode: usize) {
        self.output_mode = output_mode;
    }

    pub fn set_output_file(&mut self, output_file: Option<String>) {
        self.output_file = output_file;
    }
}