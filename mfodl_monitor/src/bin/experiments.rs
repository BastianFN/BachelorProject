#![allow(dead_code)]

extern crate mfodl_monitor;
extern crate rand;
extern crate timely;

use std::collections::{BTreeMap, HashMap};

use mfodl_monitor::{create_dataflow, parse_formula};
use rand::{Rng, StdRng};
use std::path::PathBuf;
use std::time::Instant;
use timely::dataflow::operators::capture::Extract;
use timely::dataflow::operators::{Capture, Probe, UnorderedInput};

use mfodl_monitor::dataflow_constructor::types::FlowValues::{Data, MetaData};
use mfodl_monitor::dataflow_constructor::types::{default_options, TimeFlowValues};
use mfodl_monitor::dataflow_constructor::types::TimeFlowValues::Timestamp;
use mfodl_monitor::parser::csv_parser::{parse_csv_file_to_vec, parse_delay_csv_file_to_vec, parse_verimon_results, };
use mfodl_monitor::parser::formula_syntax_tree::{Arg, Constant};

fn main() {
    println!("Benchmark:");
    let policy = "((Once [0, 7] A(x,y)) && B(y,z)) && (~Eventually [0, 7] C(z, x))".to_string();

    let functions = vec![(
        "in_order".to_string(),
        in_order_input as fn(String, String, String, usize),
    )]; /*,
        ("reverse".to_string(), reverse_order_input as fn(String, String, String, usize)),
        ("out_order".to_string(), out_order_input as fn(String, String, String, usize)),
        ("out_order_tuple".to_string(), out_order_tuple_input as fn(String, String, String, usize))];*/

    let data_specs = vec!["250K250TS".to_string()]; //, "20K".to_string(), "100K".to_string()];
    let num_workers = vec![8];
    let num_runs = 1;

    let mut res_fun: HashMap<String, Vec<String>> = HashMap::new();
    let mut res_hash: HashMap<String, Vec<String>> = HashMap::new();
    for (name, func) in functions {
        for path in data_specs.clone() {
            let path_data = format!("./data/{}.csv", path);
            let path_result = format!("./data/triangle_result_{}", path);

            for nm_workers in num_workers.clone() {
                println!(
                    "Dataset {} with {} workers and {} runs",
                    path.clone(),
                    nm_workers.clone(),
                    num_runs.clone()
                );
                let mut time_per_run = Vec::new();
                for _ in 0..num_runs {
                    let full_time = Instant::now();
                    func(
                        policy.clone(),
                        path_data.clone(),
                        path_result.clone(),
                        nm_workers.clone(),
                    );
                    time_per_run.push(full_time.elapsed().as_micros());
                }
                let mut tmp_time = 0;
                for t in time_per_run {
                    tmp_time += t;
                }
                tmp_time /= num_runs;

                let key = format!(
                    "Dataset {} with {} workers and {} runs",
                    path.clone(),
                    nm_workers.clone(),
                    num_runs.clone()
                );
                let value = format!(
                    "    Avg. Time in micros: {}   Function [{}]  ",
                    tmp_time.clone(),
                    name.clone()
                );
                res_hash.entry(key.clone()).or_default().push(value);

                let key = format!(
                    "Dataset {}, Function {}, Runs {}",
                    path.clone(),
                    name.clone(),
                    num_runs.clone()
                );
                //let value = format!("    micros: {}   worker {}", tmp_time.clone(), nm_workers.clone());
                let value = format!("{}", tmp_time.clone());
                res_fun.entry(key.clone()).or_default().push(value);
            }
        }
    }

    println!("------------------In order---------------------:");
    for (key, val) in res_hash.clone() {
        println!("{}", key);
        for v in val {
            println!("{}", v);
        }
    }
    println!();
    for (key, val) in res_fun.clone() {
        println!("#{}", key);
        print!("[");
        for v in val {
            print!(" {}, ", v);
        }
        println!("]");
    }
}

fn delay_input(policy: String, path_data: String, path_result: String, workers: usize) {
    let start = Instant::now();
    let (tps, result) = parse_delay_csv_file_to_vec(path_data);
    println!("Size res: {}", result.len());

    let expected = parse_verimon_results(path_result, (false, 0));
    println!("Time elapsed preparation is: {:?}", start.elapsed());

    let start_overhead = Instant::now();
    let (send, recv) = std::sync::mpsc::channel();
    let send = std::sync::Arc::new(std::sync::Mutex::new(send));

    let time_execute = Instant::now();
    timely::execute(timely::Config::process(workers), move |worker| {
        let send = send.lock().unwrap().clone();
        let (mut input, cap, mut time_input, time_cap,_probe) = worker.dataflow::<usize, _, _>(|scope| {
            let ((time_input, time_cap), time_stream) = scope.new_unordered_input::<TimeFlowValues>();
            let ((input, input_cap), stream) = scope.new_unordered_input::<String>();

            let (_attrs, output) = create_dataflow(parse_formula(&policy), stream, time_stream, default_options());

            let probe = output.probe();
            output.capture_into(send);

            (input, input_cap, time_input, time_cap, probe)
        });

        println!("Time elapsed for overhead: {:?}", start_overhead.elapsed());
        // Send data and step the workers
        let start_computation = Instant::now();
        if worker.index() == 0 {
            let mut prods = HashMap::new();
            for i in 0..tps {
                prods.insert(i, i);
            }

            let mut c = 0;
            let mut flag = false;
            for (tmp_tp, tmp_ts, tmp_data) in result.clone() {
                if !flag {
                    time_input.session(time_cap.delayed(&tmp_tp)).give(Timestamp(tmp_ts));
                    flag = true;
                }

                let new_prod = prods.entry(tmp_tp).or_default();
                let mut data = Vec::with_capacity(tmp_data.len());
                for t in tmp_data.clone() {
                    data.push(t);
                }

                c += tmp_data.len();
                input
                    .session(cap.delayed(new_prod))
                    .give_iterator(data.into_iter());
                if c > 500 {
                    worker.step();
                    c = 0;
                }
            }
            worker.step();
            let new_prod = result.len();
            time_input.session(cap.delayed(&new_prod)).give(TimeFlowValues::EOS);
            input.session(cap.delayed(&new_prod)).give("<eos>".parse().unwrap());
            worker.step();
        }

        println!("Time elapsed for computation: {:?}", start_computation.elapsed());
    }).unwrap();
    println!("Time execute block: {:?}", time_execute.elapsed());

    let start_post = Instant::now();
    // Vector holding a tuple (timepoint, vector with data for each timestamp)
    let actual_res: Vec<_> = recv
        .extract()
        .iter()
        .cloned()
        .map(|(time, tuples)| (time, tuples))
        .collect();

    let mut btree_res = BTreeMap::new();
    actual_res.iter().for_each(|(tp, data)| {
        let mut tmp_vec = Vec::new();
        data.iter().for_each(|rec| match rec {
            Data(_, rec) => tmp_vec.push(rec.clone()),
            MetaData(_, _) => {}
        });

        btree_res.insert(tp.clone(), tmp_vec);
    });

    let verdict = verify_results(btree_res, expected.clone());
    assert!(verdict);
    println!("-------------------- finsihed -------------------");
    println!("Time elapsed for post-processing and verification: {:?}", start_post.elapsed());
}

fn out_order_tuple_input(policy: String, path_data: String, path_result: String, workers: usize) {
    let start = Instant::now();
    let result = parse_csv_file_to_vec(PathBuf::from(path_data));

    let mut res_hash = HashMap::new();
    result.iter().for_each(|((tp, ts), tuples)| {
        res_hash.insert((*tp, *ts), tuples.clone());
    });

    let mut timed_data = Vec::new();
    res_hash.iter().for_each(|((tp, ts), tuples)| {
        for tup in tuples {
            timed_data.push((*tp, *ts, tup.clone()));
        }
    });

    let mut rng = StdRng::new().unwrap();
    rng.shuffle(&mut timed_data);

    let expected = parse_verimon_results(path_result, (false, 0));
    println!("Time elapsed preparation is: {:?}", start.elapsed());

    let start_overhead = Instant::now();
    let (send, recv) = std::sync::mpsc::channel();
    let send = std::sync::Arc::new(std::sync::Mutex::new(send));

    let time_execute = Instant::now();
    timely::execute(timely::Config::process(workers), move |worker| {
        let send = send.lock().unwrap().clone();
        let (mut input, cap, mut time_input, time_cap,_probe) = worker.dataflow::<usize, _, _>(|scope| {
            let ((time_input, time_cap), time_stream) = scope.new_unordered_input::<TimeFlowValues>();
            let ((input, input_cap), stream) = scope.new_unordered_input::<String>();

            let (_attrs, output) = create_dataflow(parse_formula(&policy), stream, time_stream, default_options());

            let probe = output.probe();
            output.capture_into(send);

            (input, input_cap, time_input, time_cap, probe)
        });

        println!("Time elapsed for overhead: {:?}", start_overhead.elapsed());
        // Send data and step the workers
        let start_computation = Instant::now();
        if worker.index() == 0 {
            let mut prods = HashMap::new();
            for i in 0..result.len() {
                prods.insert(i, i);
            }

            let mut flag = timed_data[0].0;
            time_input.session(time_cap.delayed(&flag)).give(Timestamp(timed_data[0].1));
            let mut c = 0;
            for (tmp_tp, tmp_ts, tmp_data) in timed_data.clone() {
                if flag != tmp_tp {
                    time_input.session(time_cap.delayed(&tmp_tp)).give(Timestamp(tmp_ts));
                    flag = tmp_tp;
                }

                let new_prod = prods.entry(tmp_tp).or_default();
                input
                    .session(cap.delayed(new_prod))
                    .give(tmp_data);

                if c % 500 == 0 {
                    worker.step();
                }
                c += 1;
            }
            worker.step();

            let new_prod = result.len();
            time_input.session(cap.delayed(&new_prod)).give(TimeFlowValues::EOS);
            input.session(cap.delayed(&new_prod)).give("<eos>".parse().unwrap());
            worker.step();
        }

        println!(
            "Time elapsed for computation: {:?}",
            start_computation.elapsed()
        );
    })
    .unwrap();
    println!("Time execute block: {:?}", time_execute.elapsed());

    let start_post = Instant::now();
    // Vector holding a tuple (timepoint, vector with data for each timestamp)
    let actual_res: Vec<_> = recv
        .extract()
        .iter()
        .cloned()
        .map(|(time, tuples)| (time, tuples))
        .collect();

    let mut btree_res = BTreeMap::new();
    actual_res.iter().for_each(|(tp, data)| {
        let mut tmp_vec = Vec::new();
        data.iter().for_each(|rec| match rec {
            Data(_, rec) => tmp_vec.push(rec.clone()),
            MetaData(_, _) => {}
        });

        btree_res.insert(*tp, tmp_vec);
    });

    let verdict = verify_results(btree_res, expected.clone());
    assert!(verdict);
    println!("-------------------- finsihed -------------------");
    println!(
        "Time elapsed for post-processing and verification: {:?}",
        start_post.elapsed()
    );
}

fn out_order_input(policy: String, path_data: String, path_result: String, workers: usize) {
    let start = Instant::now();
    let result = parse_csv_file_to_vec(PathBuf::from(path_data));

    let mut res_hash = HashMap::new();
    result.iter().for_each(|((tp, ts), tuples)| {
        res_hash.insert((*tp, *ts), tuples.clone());
    });

    let mut times = Vec::new();
    let mut datas = Vec::new();

    res_hash.iter().for_each(|((tp, ts), tuples)| {
        times.push((*tp, *ts));
        datas.push(tuples.clone());
    });

    let expected = parse_verimon_results(path_result, (false, 0));
    println!("Time elapsed preparation is: {:?}", start.elapsed());

    let start_overhead = Instant::now();
    let (send, recv) = std::sync::mpsc::channel();
    let send = std::sync::Arc::new(std::sync::Mutex::new(send));

    let input_data: Vec<Vec<String>> = datas
        .into_iter()
        .map(|data| data.into_iter().collect())
        .collect();

    let time_execute = Instant::now();
    timely::execute(timely::Config::process(workers), move |worker| {
        let send = send.lock().unwrap().clone();
        let (mut input, cap, mut time_input, time_cap,_probe) = worker.dataflow::<usize, _, _>(|scope| {
            let ((time_input, time_cap), time_stream) = scope.new_unordered_input::<TimeFlowValues>();
            let ((input, input_cap), stream) = scope.new_unordered_input::<String>();

            let (_attrs, output) = create_dataflow(parse_formula(&policy), stream, time_stream, default_options());

            let probe = output.probe();
            output.capture_into(send);

            (input, input_cap, time_input, time_cap, probe)
        });

        println!("Time elapsed for overhead: {:?}", start_overhead.elapsed());
        // Send data and step the workers
        let start_computation = Instant::now();
        if worker.index() == 0 {
            for round in 0usize..times.len() {
                let tp = times[round].0;
                let ts = times[round].1;

                time_input.session(time_cap.delayed(&tp)).give(Timestamp(ts));

                let data = input_data.get(round).unwrap_or(&Vec::new()).clone();

                let mut send_data = Vec::new();
                for d in data {
                    send_data.push(d);
                }

                input.session(cap.delayed(&tp)).give_iterator(send_data.into_iter());
                worker.step();
            }
            let new_prod = times.len();
            time_input.session(cap.delayed(&new_prod)).give(TimeFlowValues::EOS);
            input.session(cap.delayed(&new_prod)).give("<eos>".parse().unwrap());
            worker.step();
        }

        println!(
            "Time elapsed for computation: {:?}",
            start_computation.elapsed()
        );
    }).unwrap();
    println!("Time execute block: {:?}", time_execute.elapsed());

    let start_post = Instant::now();
    // Vector holding a tuple (timepoint, vector with data for each timestamp)
    let actual_res: Vec<_> = recv
        .extract()
        .iter()
        .cloned()
        .map(|(time, tuples)| (time, tuples))
        .collect();

    let mut btree_res = BTreeMap::new();
    actual_res.iter().for_each(|(tp, data)| {
        let mut tmp_vec = Vec::new();
        data.iter().for_each(|rec| match rec {
            Data(_, rec) => tmp_vec.push(rec.clone()),
            MetaData(_, _) => {}
        });

        btree_res.insert(*tp, tmp_vec);
    });

    //print_operator_result(btree_res.clone());
    let verdict = verify_results(btree_res, expected.clone());
    assert!(verdict);
    println!("-------------------- finsihed -------------------");
    println!(
        "Time elapsed for post-processing and verification: {:?}",
        start_post.elapsed()
    );
}

fn reverse_order_input(policy: String, path_data: String, path_result: String, workers: usize) {
    let start = Instant::now();
    let result = parse_csv_file_to_vec(PathBuf::from(path_data));

    let mut times = Vec::new();
    let mut datas = Vec::new();

    result.iter().for_each(|((tp, ts), tuples)| {
        times.push((*tp, *ts));
        datas.push(tuples.clone());
    });

    times.reverse();
    datas.reverse();

    let expected = parse_verimon_results(path_result, (false, 0));
    println!("Time elapsed preparation is: {:?}", start.elapsed());

    let start_overhead = Instant::now();
    let (send, recv) = std::sync::mpsc::channel();
    let send = std::sync::Arc::new(std::sync::Mutex::new(send));

    let input_data: Vec<Vec<String>> = datas
        .clone()
        .into_iter()
        .map(|data| data.into_iter().collect())
        .collect();

    let time_execute = Instant::now();
    timely::execute(timely::Config::process(workers), move |worker| {
        let send = send.lock().unwrap().clone();
        let (mut input, cap, mut time_input, time_cap,_probe) = worker.dataflow::<usize, _, _>(|scope| {
            let ((time_input, time_cap), time_stream) = scope.new_unordered_input::<TimeFlowValues>();
            let ((input, input_cap), stream) = scope.new_unordered_input::<String>();

            let (_attrs, output) = create_dataflow(parse_formula(&policy), stream, time_stream, default_options());

            let probe = output.probe();
            output.capture_into(send);

            (input, input_cap, time_input, time_cap, probe)
        });

        println!("Time elapsed for overhead: {:?}", start_overhead.elapsed());
        // Send data and step the workers
        let start_computation = Instant::now();
        if worker.index() == 0 {
            for round in 0usize..times.len() {
                let tp = times[round].0;
                let ts = times[round].1;

                time_input.session(time_cap.delayed(&tp)).give(Timestamp(ts));

                let data = input_data.get(round).unwrap_or(&Vec::new()).clone();

                let mut send_data = Vec::new();
                for d in data {
                    send_data.push(d);
                }

                input
                    .session(cap.delayed(&tp))
                    .give_iterator(send_data.into_iter());
                worker.step();
            }
            let new_prod = times.len();
            time_input.session(cap.delayed(&new_prod)).give(TimeFlowValues::EOS);
            input.session(cap.delayed(&new_prod)).give("<eos>".parse().unwrap());
            worker.step();
        }

        println!("Time elapsed for computation: {:?}", start_computation.elapsed());
    })
    .unwrap();
    println!("Time execute block: {:?}", time_execute.elapsed());

    let start_post = Instant::now();
    // Vector holding a tuple (timepoint, vector with data for each timestamp)
    let actual_res: Vec<_> = recv
        .extract()
        .iter()
        .cloned()
        .map(|(time, tuples)| (time, tuples))
        .collect();

    let mut btree_res = BTreeMap::new();
    actual_res.iter().for_each(|(tp, data)| {
        let mut tmp_vec = Vec::new();
        data.iter().for_each(|rec| match rec {
            Data(_, rec) => tmp_vec.push(rec.clone()),
            MetaData(_, _) => {}
        });

        btree_res.insert(*tp, tmp_vec);
    });

    //print_operator_result(btree_res.clone());
    let verdict = verify_results(btree_res, expected.clone());
    assert!(verdict);
    println!("-------------------- finsihed -------------------");
    println!(
        "Time elapsed for post-processing and verification: {:?}",
        start_post.elapsed()
    );
}

fn in_order_input(policy: String, path_data: String, path_result: String, workers: usize) {
    let start = Instant::now();
    let result = parse_csv_file_to_vec(PathBuf::from(path_data));

    println!("Length result: {}", result.len());

    let mut times = Vec::new();
    let mut datas = Vec::new();

    result.iter().for_each(|((tp, ts), tuples)| {
        times.push((*tp, *ts));
        datas.push(tuples.clone());
    });

    let expected = parse_verimon_results(path_result, (false, 0));
    println!("Time elapsed preparation is: {:?}", start.elapsed());

    let start_overhead = Instant::now();
    let (send, recv) = std::sync::mpsc::channel();
    let send = std::sync::Arc::new(std::sync::Mutex::new(send));

    let input_data: Vec<Vec<String>> = datas
        .into_iter()
        .map(|data| data.into_iter().collect())
        .collect();

    let time_execute = Instant::now();
    timely::execute(timely::Config::process(workers), move |worker| {
        let send = send.lock().unwrap().clone();
        let (mut input, cap, mut time_input, time_cap,_probe) = worker.dataflow::<usize, _, _>(|scope| {
            let ((time_input, time_cap), time_stream) = scope.new_unordered_input::<TimeFlowValues>();
            let ((input, input_cap), stream) = scope.new_unordered_input::<String>();

            let (_attrs, output) = create_dataflow(parse_formula(&policy), stream, time_stream, default_options());

            let probe = output.probe();
            output.capture_into(send);

            (input, input_cap, time_input, time_cap, probe)
        });

        println!("Time elapsed for overhead: {:?}", start_overhead.elapsed());
        // Send data and step the workers
        let start_computation = Instant::now();
        if worker.index() == 0 {
            for round in 0usize..times.len() {
                let tp = times[round].0;
                let ts = times[round].1;

                time_input.session(time_cap.delayed(&tp)).give(Timestamp(ts));

                let data = input_data.get(round).unwrap_or(&Vec::new()).clone();

                let mut send_data = Vec::new();
                for d in data {
                    send_data.push(d);
                }

                input
                    .session(cap.delayed(&tp))
                    .give_iterator(send_data.into_iter());
                worker.step();
            }
            let new_prod = times.len();
            time_input.session(cap.delayed(&new_prod)).give(TimeFlowValues::EOS);
            input.session(cap.delayed(&new_prod)).give("<eos>".parse().unwrap());
            worker.step();
        }

        println!(
            "Time elapsed for computation: {:?}",
            start_computation.elapsed()
        );
    }).unwrap();
    println!("Time execute block: {:?}", time_execute.elapsed());

    let start_post = Instant::now();
    // Vector holding a tuple (timepoint, vector with data for each timestamp)
    let actual_res: Vec<_> = recv
        .extract()
        .iter()
        .cloned()
        .map(|(time, tuples)| (time, tuples))
        .collect();

    let mut btree_res = BTreeMap::new();
    actual_res.iter().for_each(|(tp, data)| {
        let mut tmp_vec = Vec::new();
        data.iter().for_each(|rec| match rec {
            Data(_, rec) => tmp_vec.push(rec.clone()),
            MetaData(_, _) => {}
        });

        btree_res.insert(*tp, tmp_vec);
    });

    //print_operator_result(btree_res.clone());
    // println!("Result set size {}", btree_res.len());
    let verdict = verify_results(btree_res, expected.clone());
    assert!(verdict);
    println!("-------------------- finsihed -------------------");
    println!(
        "Time elapsed for post-processing and verification: {:?}",
        start_post.elapsed()
    );
}

fn verify_results(
    actual: BTreeMap<usize, Vec<Vec<Constant>>>,
    expected: BTreeMap<usize, Vec<Vec<Constant>>>,
) -> bool {
    if actual.len() != expected.len() {
        println!(
            "Unequal length actual {} != expected {}",
            actual.len(),
            expected.len()
        );
        return false;
    }

    for (key, values) in actual {
        if let Some(exp_values) = expected.get(&key) {
            if values.len() != exp_values.len() {
                println!(
                    "Unequal number of records at Keys: ({})   actual {} != expected {}",
                    key.clone(),
                    values.len(),
                    exp_values.len()
                );
                return false;
            }

            for i in 0..values.len() {
                let act_val = values[i].clone();
                let exp_val = exp_values[i].clone();

                if act_val != exp_val {
                    println!(
                        "Value miss match at Keys: ({}) and index {}",
                        key.clone(),
                        i
                    );
                    return false;
                }
            }
        } else {
            println!("Miss matching Keys: ({})", key.clone());
            return false;
        }
    }
    println!("Results Verified succesfully");
    true
}

fn print_operator_result(res: BTreeMap<usize, Vec<Vec<Arg>>>) {
    res.iter().for_each(|(time, tuples)| {
        println!("Tp {}: ", time);
        for tuple in tuples {
            print!("    ");
            for v_ in tuple {
                print!(" {}  ", v_)
            }
            println!();
        }
    });
}
