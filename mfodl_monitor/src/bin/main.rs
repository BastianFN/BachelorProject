#![allow(dead_code)]

extern crate jq_rs;
extern crate mfodl_monitor;
extern crate nom;
extern crate rand;
extern crate structopt;
extern crate timely;

use std::cmp::max;
use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader, Write};
use std::{println, writeln};

use mfodl_monitor::dataflow_constructor::types::FlowValues::Data;
use mfodl_monitor::parser::formula_syntax_tree::Constant;
use mfodl_monitor::{create_dataflow, parse_formula};
use std::path::PathBuf;

use mfodl_monitor::dataflow_constructor::types::TimeFlowValues::Timestamp;
use mfodl_monitor::dataflow_constructor::types::{OperatorOptions, TimeFlowValues};
use structopt::StructOpt;
use timely::dataflow::operators::capture::Capture;
use timely::dataflow::operators::capture::Extract;
use timely::dataflow::operators::{Probe, UnorderedInput};

use mfodl_monitor::parser::csv_parser::{
    parse_file_to_segments, parser_extended_wrapper, ParserReturn, Segment,
};

use mfodl_monitor::parser::json_parser::find_timestamp;

// const MODE_VALS: &[&str] = &["order", "out_of_order"];

#[derive(Debug, StructOpt)]
#[structopt(
    name = "Timelymon",
    about = "Timelymon is a First-Order Monitoring tool which transparently supports parallel workers."
)]
pub struct ProgArgs {
    /// Policy string. Example: "((Once [0, 7] A(x,y)) && B(y,z)) && (~Eventually [0, 7] C(z, x))"
    policy: String,

    /// Data set file. Example: data/50K50TS.csv. If not set Timelymon will expect a data stream from stdin
    file: Option<PathBuf>,

    ///Number of workers
    #[structopt(short, long, default_value = "1")]
    workers: usize,

    /// Save output to file
    #[structopt(short, long)]
    output_file: Option<String>,

    /// write to file (0), print to stdin (1) or disregard input (2)
    #[structopt(short, long, default_value = "0")]
    mode_out_put: usize,

    /// Step size of workers
    /// TODO Different results depending on size?
    #[structopt(short, long, default_value = "1")]
    step: usize,

    /// Duplication: default deduplication (0), or allows for duplicates (1)
    #[structopt(short, long)]
    deduplication: bool,

    /// Batched output: number of outputs grouped together when -o 2 is set
    #[structopt(short, long, default_value = "1")]
    batch_output: usize,

    /// Timestamp to filter JSON data by
    #[structopt(short = "t", long = "timestamp")]
    timestamp: Option<u64>,

    /// File type
    #[structopt(short = "f", long = "filetype")]
    file_type: Option<String>,
}

fn main() {
    let args = ProgArgs::from_args();
    let mut options = OperatorOptions::new();

    let policy = args.policy;
    let some_path_data = args.file;

    options.set_workers(args.workers);
    options.set_output_file(args.output_file);
    options.set_step(args.step);
    options.set_output_batch(args.batch_output);
    options.set_deduplication(args.deduplication);

    let out_put_mode = match args.mode_out_put {
        0 | 1 | 2 => args.mode_out_put,
        _ => {
            println!("Passed out put mode is invalid!");
            3
        }
    };
    options.set_output_mode(out_put_mode);

    // Check if policy is a file
    let new_policy = if let Ok(f) = File::open(policy.clone()) {
        let mut res = "".to_string();
        for line in BufReader::new(&f).lines() {
            res = res + &*line.unwrap()
        }
        res.to_string()
    } else {
        policy
    };

    let mut tp_to_ts: HashMap<usize, usize> = HashMap::with_capacity(8);

    // println!("{} {:?}", policy.clone(), path_data.clone());
    let res = if let Some(path_data) = some_path_data {
        let (r, tpts) = execute_from_file(new_policy.clone(), path_data.clone(), options.clone());
        tp_to_ts = tpts;
        r
    } else {
        let (r, tpts) = execute_from_stdin(new_policy.clone(), options.clone(), args.file_type);
        tp_to_ts = tpts;
        r
    };

    // support multiple output formats
    if !res.is_empty() {
        if let Some(file_name) = options.get_output_file() {
            match File::create(file_name) {
                Ok(mut file) => {
                    //println!("{:?}", res);
                    res.iter().for_each(|(tp, flow)| {
                        //println!("@{tp} {:?}\n\n", flow);
                        let mut out = format!("");
                        flow.iter().for_each(|d| {
                            let ver = d.clone();
                            match ver {
                                Data(true, vals) => {
                                    if !vals.is_empty() {
                                        if vals.len() == 1 {
                                            match vals[0] {
                                                Constant::Int(x) => {
                                                    out += &format!(" ({:?}) ", x);
                                                }
                                                _ => {}
                                            }
                                        } else if vals.len() == 2 {
                                            match vals[0] {
                                                Constant::Int(x) => match vals[1] {
                                                    Constant::Int(y) => {
                                                        out += &format!(" ({:?},{:?}) ", x, y);
                                                    }
                                                    _ => {}
                                                },
                                                _ => {}
                                            }
                                        } else {
                                            out += "(";
                                            for v in 0..vals.len() - 1 {
                                                match vals[v] {
                                                    Constant::Int(x) => out += &format!("{:?},", x),
                                                    _ => {}
                                                }
                                            }
                                            match vals[vals.len() - 1] {
                                                Constant::Int(x) => {
                                                    out += &format!("{:?})", x);
                                                }
                                                Constant::Str(_) => {}
                                                Constant::JSONValue(_) => {}
                                            }
                                        }
                                    }
                                }
                                _ => (),
                            }
                        });
                        let ts = *tp_to_ts.entry(*tp).or_default();
                        match writeln!(file, "@{ts} (time point {tp}): {}", out) {
                            Err(err) => println!("Error writing: {}", err),
                            _ => (),
                        };
                    });
                }
                Err(e) => println!("Error {:?}", e),
            }
        }
    }
}

fn execute_from_stdin(
    policy: String,
    options: OperatorOptions,
    file_type: Option<String>,
) -> (
    Vec<(
        usize,
        Vec<mfodl_monitor::dataflow_constructor::types::FlowValues>,
    )>,
    HashMap<usize, usize>,
) {
    let (send, recv) = std::sync::mpsc::channel();
    let send = std::sync::Arc::new(std::sync::Mutex::new(send));

    let (tp_send, tp_recv) = std::sync::mpsc::channel();
    let tp_send = std::sync::Arc::new(std::sync::Mutex::new(tp_send));

    let options_ = options.clone();

    timely::execute(
        timely::Config::process(options.get_workers()),
        move |worker| {
            let mut threshold = 0;
            let mut tp_to_ts: HashMap<usize, usize> = HashMap::with_capacity(8);
            let send = send.lock().unwrap().clone();
            let tp_send = tp_send.lock().unwrap().clone();
            let (mut input, mut cap, mut time_input, mut time_cap, _probe) = worker
                .dataflow::<usize, _, _>(|scope| {
                    let ((time_input, time_cap), time_stream) =
                        scope.new_unordered_input::<TimeFlowValues>();
                    let ((input, input_cap), stream) = scope.new_unordered_input::<String>();

                    let (_attrs, output) = create_dataflow(
                        parse_formula(&policy),
                        stream,
                        time_stream,
                        options.clone(),
                    );

                    let probe = output.probe();
                    output.capture_into(send);

                    (input, input_cap, time_input, time_cap, probe)
                });

            let file_type = file_type.clone();

            if worker.index() == 0 {
                let mut current = 0;
                let mut current_is_set = false;

                let mut current_segment = Vec::with_capacity(options.get_step());

                match file_type {
                    Some(ft) => {
                        if ft == "json" {
                            for line in io::stdin().lines() {
                                let line = line.expect("Error reading line from stdin");
                                match serde_json::from_str::<serde_json::Value>(&line) {
                                    Ok(json_value) => {
                                        if let Some(timestamp) = find_timestamp(&json_value) {
                                            let ts = timestamp as usize;
                                            time_input
                                                .session(time_cap.delayed(&ts))
                                                .give(Timestamp(ts));
                                            if !current_is_set {
                                                current = ts;
                                                current_is_set = true;
                                            }
                                            let val = json_value.to_string();
                                            if options.get_step() == 1 {
                                                input.session(cap.delayed(&ts)).give(val);
                                                worker.step();
                                            } else {
                                                threshold = threshold + 1;
                                                input.session(cap.delayed(&ts)).give(val);
                                                if threshold >= options.get_step() {
                                                    worker.step();
                                                    threshold = 0;
                                                }
                                            }
                                        }
                                    }
                                    Err(e) => println!("JSON Parsing Error: {}", e),
                                }
                            }
                        }
                    }
                    // Default to CSV for now
                    None => {
                        for line in io::stdin().lines() {
                            match parser_extended_wrapper(line.unwrap()) {
                                ParserReturn::Data(tp, ts, val) => {
                                    tp_to_ts.entry(tp).or_insert(ts);
                                    if current_is_set {
                                        if tp == current {
                                            if options.get_step() == 1 {
                                                input
                                                    .session(cap.delayed(&tp))
                                                    .give(val.to_string());
                                                worker.step();
                                            } else {
                                                // current_segment.push(val.to_string())
                                                threshold = threshold + 1;
                                                input
                                                    .session(cap.delayed(&ts))
                                                    .give(val.to_string());
                                                if threshold >= options.get_step() {
                                                    worker.step();
                                                    threshold = 0;
                                                }
                                            }
                                        } else {
                                            time_input
                                                .session(time_cap.delayed(&tp))
                                                .give(Timestamp(ts));
                                            threshold = threshold + current_segment.len();

                                            if options.get_step() == 1 {
                                                input
                                                    .session(cap.delayed(&tp))
                                                    .give(val.to_string());
                                            } else {
                                                threshold = threshold + 1;
                                                input
                                                    .session(cap.delayed(&ts))
                                                    .give(val.to_string());
                                                if threshold >= options.get_step() {
                                                    worker.step();
                                                    threshold = 0;
                                                }
                                            }
                                            // current_segment.clear();
                                            current = tp;
                                            // current_segment.push(val.to_string());
                                            worker.step();
                                        }
                                    } else {
                                        current = tp;
                                        current_is_set = true;
                                        time_input
                                            .session(time_cap.delayed(&current))
                                            .give(Timestamp(ts));

                                        if options.get_step() == 1 {
                                            input.session(cap.delayed(&tp)).give(val.to_string());
                                            worker.step();
                                        } else {
                                            threshold = threshold + 1;
                                            input.session(cap.delayed(&ts)).give(val.to_string());
                                            if threshold >= options.get_step() {
                                                worker.step();
                                                threshold = 0;
                                            }
                                        }
                                    }
                                }
                                ParserReturn::Watermark(wm) => {
                                    threshold = threshold + current_segment.len();
                                    input
                                        .session(cap.delayed(&current))
                                        .give_iterator(current_segment.clone().into_iter());
                                    let t = if wm < 0 { 0 } else { wm as usize };
                                    cap.downgrade(&t);
                                    time_cap.downgrade(&t);
                                    worker.step();
                                    current_segment.clear();
                                }
                                ParserReturn::Error(s) => {
                                    println!("Parser Error: {}", s);
                                }
                            }

                            if threshold >= options.get_step() {
                                worker.step();
                                threshold = 0;
                            }
                        }
                    }
                }

                // input.session(cap.delayed(&current)).give_iterator(
                //     current_segment.clone().into_iter(),
                // );

                worker.step();

                let new_prod = current + 1;
                time_input
                    .session(cap.delayed(&new_prod))
                    .give(TimeFlowValues::EOS);
                input
                    .session(cap.delayed(&new_prod))
                    .give("<eos>".parse().unwrap());

                for tp_ts in tp_to_ts {
                    let _ = tp_send.send(tp_ts);
                }
            }
        },
    )
    .unwrap();

    if options_.get_output_mode() == 0 {
        let mut tp_to_ts = HashMap::with_capacity(8);
        for (tp, ts) in tp_recv.iter() {
            tp_to_ts.entry(tp).or_insert(ts);
        }

        let res: Vec<_> = recv
            .extract()
            .iter()
            .cloned()
            .map(|(time, tuples)| (time, tuples))
            .collect();
        return (res, tp_to_ts.clone());
    }

    (vec![], HashMap::new())
}

fn execute_from_file(
    policy: String,
    path_data: PathBuf,
    options: OperatorOptions,
) -> (
    Vec<(
        usize,
        Vec<mfodl_monitor::dataflow_constructor::types::FlowValues>,
    )>,
    HashMap<usize, usize>,
) {
    let (send, recv) = std::sync::mpsc::channel();
    let send = std::sync::Arc::new(std::sync::Mutex::new(send));

    let (tp_send, tp_recv) = std::sync::mpsc::channel();
    let tp_send = std::sync::Arc::new(std::sync::Mutex::new(tp_send));

    let options_ = options.clone();

    timely::execute(
        timely::Config::process(options.get_workers()),
        move |worker| {
            let mut threshold = 0;
            let mut tp_to_ts: HashMap<usize, usize> = HashMap::with_capacity(8);
            let send = send.lock().unwrap().clone();
            let tp_send = tp_send.lock().unwrap().clone();

            let (mut input, mut cap, mut time_input, mut time_cap, _probe) = worker
                .dataflow::<usize, _, _>(|scope| {
                    let ((time_input, time_cap), time_stream) =
                        scope.new_unordered_input::<TimeFlowValues>();
                    let ((input, input_cap), stream) = scope.new_unordered_input::<String>();

                    let (_attrs, output) = create_dataflow(
                        parse_formula(&policy),
                        stream,
                        time_stream,
                        options.clone(),
                    );

                    let probe = output.probe();
                    output.capture_into(send);

                    (input, input_cap, time_input, time_cap, probe)
                });

            // Send data and step the workers
            if worker.index() == 0 {
                let segments = parse_file_to_segments(path_data.clone());
                let mut max_wm = 0;
                let mut max_tp = 0;
                for segs in segments {
                    match segs {
                        Segment::Epoch(wm) => {
                            let t = if wm < 0 { 0 } else { wm as usize };
                            time_cap.downgrade(&t);
                            cap.downgrade(&t);
                            worker.step();
                            max_wm = t;
                        }
                        Segment::Seg(tp, ts, val) => {
                            tp_to_ts.entry(tp).or_insert(ts);
                            max_tp = max(max_tp, tp);
                            time_input
                                .session(time_cap.delayed(&tp))
                                .give(Timestamp(ts));
                            threshold = threshold + val.len();
                            input
                                .session(cap.delayed(&tp))
                                .give_iterator(val.into_iter());
                            worker.step();
                        }
                    }

                    if threshold >= options.get_step() {
                        worker.step();
                        threshold = 0;
                    }
                }

                let new_prod = max(max_tp, max_wm) + 1;
                time_input
                    .session(cap.delayed(&new_prod))
                    .give(TimeFlowValues::EOS);
                input
                    .session(cap.delayed(&new_prod))
                    .give("<eos>".parse().unwrap());
                worker.step();

                for tp_ts in tp_to_ts {
                    let _ = tp_send.send(tp_ts);
                }
            }
        },
    )
    .unwrap();

    if options_.get_output_mode() == 0 {
        let mut tp_to_ts = HashMap::with_capacity(8);
        for (tp, ts) in tp_recv.iter() {
            tp_to_ts.entry(tp).or_insert(ts);
        }
        // Vector holding a tuple (timepoint, vector with data for each timestamp)
        let res: Vec<_> = recv
            .extract()
            .iter()
            .cloned()
            .map(|(time, tuples)| (time, tuples))
            .collect();
        return (res, tp_to_ts.clone());
    }

    return (vec![], HashMap::new());
}
