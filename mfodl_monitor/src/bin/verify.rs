#![allow(dead_code)]

extern crate mfodl_monitor;
extern crate rand;
extern crate timely;

use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{BufRead, BufReader};

use mfodl_monitor::{create_dataflow, parse_formula};
use rand::{Rng, StdRng};
use std::path::PathBuf;
use timely::dataflow::operators::capture::Extract;
use timely::dataflow::operators::{Capture, Probe, UnorderedInput};

use mfodl_monitor::dataflow_constructor::types::FlowValues::{Data, MetaData};
use mfodl_monitor::dataflow_constructor::types::{default_options, Record, TimeFlowValues};
use mfodl_monitor::dataflow_constructor::types::TimeFlowValues::Timestamp;
use mfodl_monitor::parser::csv_parser::{parse_csv_file_to_vec, parse_delay_csv_file_to_vec, parse_verimon_results};
use mfodl_monitor::parser::formula_syntax_tree::Constant;

fn load_formula(file_name : String) -> String {
    let mut result = "".to_string();
    match File::open(file_name) {
        Ok(file) => {
            let f = BufReader::new(&file);
            for line in f.lines() {
                result = line.unwrap();
            }
        }
        Err(e) => println!("Error {:?}", e),
    }
    result
}

fn build_btree(actual_res: Vec<(usize, Vec<Record>)>, fv: Vec<usize>) -> BTreeMap<usize, Vec<Vec<Constant>>> {
    let mut btree_res = BTreeMap::new();
    if fv.is_empty() {
        actual_res.iter().for_each(|(tp, data)| {
            let mut tmp_vec = Vec::new();
            data.iter().for_each(|rec| match rec {
                Data(_, rec) => tmp_vec.push(rec.clone()),
                MetaData(_, _) => {}
            });

            btree_res.insert(tp.clone(), tmp_vec);
        });
    } else {
        actual_res.iter().for_each(|(tp, data)| {
            let mut tmp_vec = Vec::new();
            data.iter().for_each(|rec| match rec.clone() {
                Data(_, mut r) => {
                    let tmp = r.remove(fv[0]);
                    r.insert(0, tmp);
                    tmp_vec.push(r.clone())
                },
                _ => {}
            });

            btree_res.insert(tp.clone(), tmp_vec);
        });
    }
    btree_res
}

fn main() {
    let num_workers = 6;
    let data_specs = vec![50, 100, 250];
    // Star result is ordered (a,b,c,d) but timely returns (b,c,a,d)
    for formula_name in vec!["Star", "Negated_Triangle", "Triangle", "Linear"] {
        let formula =  load_formula(format!("./data/{}/{}_Formula.txt", formula_name, formula_name));
        println!("{}", formula);

        // delayed
        let delay_function = vec![("delay_input".to_string(), delay_input as fn(String, String, String, Vec<usize>, usize))];
        // in and out of order
        let functions = vec![("".to_string(), in_order_input as fn(String, String, String, Vec<usize>, usize)),
                             ("reverse_".to_string(), reverse_order_input as fn(String, String, String, Vec<usize>, usize)),
                             ("reorder_epoch_".to_string(), out_order_input as fn(String, String, String, Vec<usize>, usize))];
                             /*,
                             ("reorder_tuple_".to_string(), out_order_tuple_input as fn(String, String, String, Vec<usize>, usize))];*/

        for _ in 0..5 {
            for (name, func) in functions.clone() {
                for num in data_specs.clone() {
                    let path_data = format!("./data/{}/{}{}K{}TS.csv", formula_name, name, num, num);
                    println!("{}", path_data);
                    let path_result = format!("./data/{}/result_{}K", formula_name, num);
                    /*if formula_name == "Star" {
                        func(formula.clone(), path_data.clone(), path_result.clone(), vec![2], num_workers);
                    } else if formula_name == "Triangle" {
                        func(formula.clone(), path_data.clone(), path_result.clone(), vec![1], num_workers);
                    } else {
                        func(formula.clone(), path_data.clone(), path_result.clone(), vec![], num_workers);
                    }*/
                    func(formula.clone(), path_data.clone(), path_result.clone(), vec![], num_workers);
                }
            }

            for (_, func) in delay_function.clone() {
                for num in data_specs.clone() {
                    for md in vec![2,5] {
                        for sd in vec![5, 15, 25] {
                            let path_data = format!("./data/{}/{}K{}md{}sd_out_of_order.csv", formula_name, num, md, sd);
                            println!("{}", path_data);
                            let path_result = format!("./data/{}/result_{}K", formula_name, num);
                            /*if formula_name == "Star" {
                                func(formula.clone(), path_data.clone(), path_result.clone(), vec![2], num_workers);
                            } else if formula_name == "Triangle" {
                                func(formula.clone(), path_data.clone(), path_result.clone(), vec![1], num_workers);
                            }else {
                                func(formula.clone(), path_data.clone(), path_result.clone(), vec![], num_workers);
                            }*/
                            func(formula.clone(), path_data.clone(), path_result.clone(), vec![], num_workers);
                        }
                    }
                }
            }
        }

        /*for (_, func) in delay_function {
            for num in data_specs.clone() {
                for md in vec![2,5] {
                    for sd in vec![5, 15, 25] {
                        let path_data = format!("./data/{}/{}K{}md{}sd_out_of_order.csv", formula_name, num, md, sd);
                        println!("{}", path_data);
                        let path_result = format!("./data/{}/result_{}K", formula_name, num);
                        if formula_name == "Star" {
                            func(formula.clone(), path_data.clone(), path_result.clone(), vec![2], num_workers);
                        } else if formula_name == "Triangle" {
                            func(formula.clone(), path_data.clone(), path_result.clone(), vec![1], num_workers);
                        }else {
                            func(formula.clone(), path_data.clone(), path_result.clone(), vec![], num_workers);
                        }
                    }
                }
            }
        }*/
    }
}

fn delay_input(policy: String, path_data: String, path_result: String, fv: Vec<usize>, workers: usize) {
    let (tps, result) = parse_delay_csv_file_to_vec(path_data);
    let expected = parse_verimon_results(path_result, (false, 0));

    let (send, recv) = std::sync::mpsc::channel();
    let send = std::sync::Arc::new(::std::sync::Mutex::new(send));

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

        if worker.index() == 0 {
            let mut prods = HashMap::new();
            for i in 0..tps {
                prods.insert(i, i);
            }

            let mut c = 0;
            let mut flag = result[0].0;
            for (tmp_tp, tmp_ts, tmp_data) in result.clone() {
                if flag != tmp_tp {
                    time_input.session(time_cap.delayed(&tmp_tp)).give(Timestamp(tmp_ts));
                    flag = tmp_tp;
                }

                let new_prod = prods.entry(tmp_tp).or_default();
                c += tmp_data.len();
                input.session(cap.delayed(new_prod)).give_iterator(tmp_data.into_iter());
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
    }).unwrap();

    // Vector holding a tuple (timepoint, vector with data for each timestamp)
    let actual_res: Vec<_> = recv
        .extract()
        .iter()
        .cloned()
        .map(|(time, tuples)| (time, tuples))
        .collect();


    let verdict = verify_results(build_btree(actual_res, fv), expected.clone());
    assert!(verdict);
    println!("-------------------- finsihed -------------------");
}

fn out_order_tuple_input(policy: String, path_data: String, path_result: String, fv: Vec<usize>, workers: usize) {
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

    let (send, recv) = std::sync::mpsc::channel();
    let send = std::sync::Arc::new(::std::sync::Mutex::new(send));

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

        if worker.index() == 0 {
            let mut prods = HashMap::new();
            for i in 0..result.len() {
                prods.insert(i, i);
            }


            let mut c = 0;
            let mut flag = timed_data[0].0;
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
    })
        .unwrap();

    // Vector holding a tuple (timepoint, vector with data for each timestamp)
    let actual_res: Vec<_> = recv
        .extract()
        .iter()
        .cloned()
        .map(|(time, tuples)| (time, tuples))
        .collect();


    let verdict = verify_results(build_btree(actual_res, fv), expected.clone());
    assert!(verdict);
    println!("-------------------- finsihed -------------------");
}

fn out_order_input(policy: String, path_data: String, path_result: String, fv: Vec<usize>, workers: usize) {
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

    let (send, recv) = ::std::sync::mpsc::channel();
    let send = ::std::sync::Arc::new(::std::sync::Mutex::new(send));

    let input_data: Vec<Vec<String>> = datas
        .into_iter()
        .map(|data| data.into_iter().collect())
        .collect();

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

        if worker.index() == 0 {
            for round in 0usize..times.len() {
                let next_epoch = times[round].0;
                let new_prod = next_epoch;
                time_input.session(time_cap.delayed(&next_epoch)).give(Timestamp(times[round].1));

                let data = input_data.get(round).unwrap_or(&Vec::new()).clone();
                let mut send_data = Vec::new();
                for d in data {
                    send_data.push(d);
                }

                input
                    .session(cap.delayed(&new_prod))
                    .give_iterator(send_data.into_iter());
                worker.step();
            }
            let new_prod = times.len();
            time_input.session(cap.delayed(&new_prod)).give(TimeFlowValues::EOS);
            input.session(cap.delayed(&new_prod)).give("<eos>".parse().unwrap());
            worker.step();
        }
    }).unwrap();
    // Vector holding a tuple (timepoint, vector with data for each timestamp)
    let actual_res: Vec<_> = recv
        .extract()
        .iter()
        .cloned()
        .map(|(time, tuples)| (time, tuples))
        .collect();

    //print_operator_result(build_btree(actual_res.clone(), fv.clone()));
    let verdict = verify_results(build_btree(actual_res, fv), expected.clone());
    assert!(verdict);
    println!("-------------------- finsihed -------------------");
}

fn reverse_order_input(policy: String, path_data: String, path_result: String, fv: Vec<usize>, workers: usize) {
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
    let (send, recv) = ::std::sync::mpsc::channel();
    let send = ::std::sync::Arc::new(::std::sync::Mutex::new(send));

    let input_data: Vec<Vec<String>> = datas
        .clone()
        .into_iter()
        .map(|data| data.into_iter().collect())
        .collect();

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

        if worker.index() == 0 {
            for round in 0usize..times.len() {
                let next_epoch = times[round].0;
                let new_prod = next_epoch;

                time_input.session(time_cap.delayed(&next_epoch)).give(Timestamp(times[round].1));

                let data = input_data.get(round).unwrap_or(&Vec::new()).clone();
                let mut send_data = Vec::new();
                for d in data {
                    send_data.push(d);
                }

                input
                    .session(cap.delayed(&new_prod))
                    .give_iterator(send_data.into_iter());
                worker.step();
            }
            let new_prod = times.len();
            time_input.session(cap.delayed(&new_prod)).give(TimeFlowValues::EOS);
            input.session(cap.delayed(&new_prod)).give("<eos>".parse().unwrap());
            worker.step();
        }

    }).unwrap();

    // Vector holding a tuple (timepoint, vector with data for each timestamp)
    let actual_res: Vec<_> = recv
        .extract()
        .iter()
        .cloned()
        .map(|(time, tuples)| (time, tuples))
        .collect();

    //print_operator_result(build_btree(actual_res.clone(), fv.clone()));
    let verdict = verify_results(build_btree(actual_res, fv), expected.clone());
    assert!(verdict);
    println!("-------------------- finsihed -------------------");
}

fn in_order_input(policy: String, path_data: String, path_result: String, fv: Vec<usize>, workers: usize) {
    let result = parse_csv_file_to_vec(PathBuf::from(path_data));

    let mut times = Vec::new();
    let mut datas = Vec::new();

    result.iter().for_each(|((tp, ts), tuples)| {
        times.push((*tp, *ts));
        datas.push(tuples.clone());
    });

    let expected = parse_verimon_results(path_result, (false, 0));
    let (send, recv) = ::std::sync::mpsc::channel();
    let send = ::std::sync::Arc::new(::std::sync::Mutex::new(send));

    let input_data: Vec<Vec<String>> = datas
        .into_iter()
        .map(|data| data.into_iter().collect())
        .collect();


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

        // Send data and step the workers
        if worker.index() == 0 {
            for round in 0usize..times.len() {
                let next_epoch = times[round].0;
                let new_prod = next_epoch;
                time_input.session(time_cap.delayed(&next_epoch)).give(Timestamp(times[round].1));

                let data = input_data.get(round).unwrap_or(&Vec::new()).clone();
                let mut send_data = Vec::new();
                for d in data {
                    send_data.push(d);
                }

                input.session(cap.delayed(&new_prod)).give_iterator(send_data.into_iter());
                worker.step();
            }
            let new_prod = times.len();
            time_input.session(cap.delayed(&new_prod)).give(TimeFlowValues::EOS);
            input.session(cap.delayed(&new_prod)).give("<eos>".parse().unwrap());
            worker.step();
        }
    }).unwrap();

    // Vector holding a tuple (timepoint, vector with data for each timestamp)
    let actual_res: Vec<_> = recv
        .extract()
        .iter()
        .cloned()
        .map(|(time, tuples)| (time, tuples))
        .collect();

    //print_operator_result(build_btree(actual_res.clone(), fv.clone()));
    let verdict = verify_results(build_btree(actual_res, fv), expected.clone());
    assert!(verdict);
    println!("-------------------- finsihed -------------------");
}

fn verify_results(actual: BTreeMap<usize, Vec<Vec<Constant>>>, mut expected: BTreeMap<usize, Vec<Vec<Constant>>>, ) -> bool {
    if actual.len() != expected.len() {
        println!(
            "Unequal length actual {} != expected {}",
            actual.len(),
            expected.len()
        );
        return false;
    }

    for (key, mut values) in actual {
        if let Some(exp_values) = expected.get_mut(&key) {
            if values.len() != exp_values.len() {
                println!(
                    "Unequal number of records at Keys: ({})   actual {} != expected {}",
                    key,
                    values.len(),
                    exp_values.len()
                );
                return false;
            }

            values.sort();
            exp_values.sort();

            for i in 0..values.len() {
                let act_val = values[i].clone();
                let exp_val = exp_values[i].clone();

                if act_val != exp_val {
                    println!("Value miss match at Keys: ({}) and index {}    act {:?}  =  {:?}", key, i, act_val, exp_val);
                    return false;
                }
            }
        } else {
            println!("Miss matching Keys: ({})", key);
            return false;
        }
    }
    println!("Results Verified succesfully");
    true
}

fn print_operator_result(res: BTreeMap<usize, Vec<Vec<Constant>>>) {
    res.iter().for_each(|(time,  tuples)| {
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
