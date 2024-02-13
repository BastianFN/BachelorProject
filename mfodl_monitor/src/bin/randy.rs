extern crate mfodl_monitor;
extern crate timely;
extern crate clap;
extern crate nom;

use std::fs::{DirBuilder, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path};
use mfodl_monitor::parser::csv_parser::{parse_gen_fma_wrapper, parse_timelymon_results, parse_verimon_results, parser_extended_wrapper, ParserReturn};

use std::env::{set_current_dir};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::process::{Command, Output};
use std::time::Instant;

use mfodl_monitor::parser::formula_syntax_tree::Constant;

static PATH_TO_RESULTS: &str = "/Users/krq770/CLionProjects/timelymon/mfodl_monitor/data/Experiments/Results/results.csv";
static PATH_TO_EXPERIMENTS: &str = "/Users/krq770/CLionProjects/timelymon/mfodl_monitor/data/Experiments";
static PATH_TO_TIMELYMON: &str = "/Users/krq770/CLionProjects/timelymon/mfodl_monitor/target/release/timelymon";
static PATH_TO_FORMULA_GENERATOR: &str = "/Users/krq770/Desktop/Experiments_Stream_Monitor/monpoly/_build/default/tools/gen_fma.exe";
static PATH_TO_VERIMON: &str= "/Users/krq770/Desktop/Experiments_Stream_Monitor/monpoly/monpoly";
static PATH_TO_TRACE_GENERATOR: &str = "/Users/krq770/CLionProjects/timelymon/mfodl_monitor/tools/Generator/Generator.jar";
static PATH_TO_REPLAYER: &str = "/Users/krq770/CLionProjects/timelymon/mfodl_monitor/tools/Replayer/Replayer.jar";
static MAX_TS: usize = 500;

// todo two modes, a fixes n formula experiment and a randomized testing framework

// todo allow user to fix an operation and try to generate those operators fx generate formula with PREVIOUS [0, *)

fn infuse_watermarks(file_name: String) {
    let file = File::open(file_name.clone());
    let mut res : String = "".to_string();
    if let Ok(wf) = file {
        let mut current_is_set = false;
        let mut current_tp = 0;
        for line in BufReader::new(&wf).lines() {
            let resstr = line.unwrap();
            match parser_extended_wrapper(resstr.clone()) {
                ParserReturn::Data(tp, _, _) => {
                    if current_is_set {
                        if tp == current_tp {
                            res.push_str(&format!("{}\n", resstr))
                        } else {
                            res.push_str(&format!(">WATERMARK {current_tp}<\n"));
                            current_tp = tp;
                            res.push_str(&format!("{}\n", resstr))
                        }
                    } else {
                        current_tp = tp;
                        current_is_set = true;
                        res.push_str(&format!("{}\n", resstr))
                    }
                }
                _ => {}
            }
        }
    }

    let file = File::create(format!("wm_{}", file_name));
    if let Ok(wf) = file {
        let _ = BufWriter::new(&wf).write_all(res.as_ref());
    }
}

fn create_base_data_set(output_name: String, t: i32) {
    // Check the result
    match Command::new("java")
        .arg("-cp").arg(PATH_TO_TRACE_GENERATOR).arg("org.example.CsvStreamGenerator")
        .arg("-sig").arg(&format!("{output_name}.sig"))
        .arg("-x").arg("1.0")
        .arg("-e").arg("1000")
        .arg(&format!("{t}")).output() {
        Ok(x) => {
            let data_file = format!("{output_name}_{t}");
            write_data_file(x, data_file, ".csv".to_string(), false);
        }
        Err(_) => {}
    }

    // replayer
    match Command::new("java")
        .arg("-cp").arg(PATH_TO_REPLAYER).arg("org.example.Replayer")
        .arg("-f").arg("verimon")
        .arg("-a").arg("0")
        .arg(&format!("{output_name}_{t}.csv")).output() {
        Ok(x) => {
            let data_file = format!("verimon_{output_name}_{t}");
            write_data_file(x, data_file, "".to_string(), true);
        }
        Err(_) => {}
    }

    // create verification set
    match Command::new(PATH_TO_VERIMON).arg("-sig")
        .arg(&format!("{output_name}.sig")).arg("-formula")
        .arg(&format!("{output_name}.mfotl")).arg("-verified").arg("-log")
        .arg(&format!("verimon_{output_name}_{t}")).output() {
        Ok(x) => {
            let data_file = format!("result_verimon_{output_name}_{t}");
            write_data_file(x, data_file, "".to_string(), false);
        }
        Err(_) => {}
    }

    // infuse watermarks
    infuse_watermarks(format!("{output_name}_{t}.csv"));


    /*let arg3 = "-et";
    let arg4 = "-md";
    let val5 = "5";
    let arg5 = "-s";
    let val6 = "25";
    let arg6 = "-wp";
    let val7 = "1";*/


    // todo treat it like delayed data for now
    match Command::new("java")
        .arg("-cp").arg(PATH_TO_TRACE_GENERATOR).arg("org.example.CsvStreamGenerator")
        .arg("-sig").arg(&format!("{output_name}.sig"))
        .arg("-e").arg("1000")
        .arg(&format!("{t}")).output() {
        Ok(x) => {
            let data_file = format!("{output_name}_{t}_delayed");
            write_data_file(x, data_file, ".csv".to_string(), false);
        }
        Err(_) => {}
    }
}

fn parse_gen_fma_output(out: Output) -> (String, String) {
    let output_str = String::from_utf8_lossy(&out.stdout).to_string();
    parse_gen_fma_wrapper(output_str)
}

fn replace_pattern(input: String) -> String {
    // Define the regular expression pattern
    let pattern = regex::Regex::new(r"\((\d+),(\d+)\)").unwrap();

    // Replace matches using the provided closure
    let result = pattern.replace_all(&*input, |caps: &regex::Captures| {
        // Extract the captured numbers
        let x1 = caps[1].to_string().parse::<usize>().unwrap();
        let x2 = caps[2].to_string().parse::<usize>().unwrap();

        // Construct the replacement string
        if x1+1 == x2 {
            format!("[{},{})", x1, x2)
        } else {
            format!("({},{})", x1, x2)
        }

    });

    // Return the final result as a String
    result.into()
}

fn write_gen_fma_output(sig: String, mfotl: String, name: String) {
    let sig_name = format!("{name}.sig");
    let file = File::create(sig_name);
    if let Ok(mut wf) = file {
        let _ = wf.write_all(sig.as_ref());
    }

    let mfotl_name = format!("{name}.mfotl");
    let file = File::create(mfotl_name);
    if let Ok(mut wf) = file {
        let _ = wf.write_all(mfotl.as_ref());
    }
}

fn write_data_file(out: Output, name: String, format: String, add_max_ts : bool) {
    let mut output_str = String::from_utf8_lossy(&out.stdout).to_string();
    if add_max_ts {
        output_str.push_str(&format!("@{MAX_TS};"));
    }
    let new_name = format!("{name}{format}");
    let file = File::create(new_name);
    if let Ok(mut wf) = file {
        let _ = wf.write_all(output_str.as_ref());
    }
}

fn data_generation(operators: Vec<i32>, free_variables: Vec<i32>, trace_len: Vec<i32>, num_formulas: i32, max_lb: i32, max_rb: i32) {
    // generate formula and signature
    let _ = set_current_dir(PATH_TO_EXPERIMENTS);
    for op in operators {
        println!("Initialize: Operator {op}");
        let name = format!("Operators_{}", op);
        let dir_name = Path::new(&name);
        if !Path::exists(dir_name) {
            let _ = DirBuilder::new().create(dir_name);
        }

        //set_current_dir(dir_name);
        //println!("{:?}", current_dir());
        for fv in free_variables.clone() {
            println!("      Generate data for Free Variables {fv}");
            let _ = set_current_dir(dir_name);
            //println!("   Free vars {fv} {:?}", current_dir());
            let name = format!("Free_variables_{fv}");
            let sub_dir_name = Path::new(&name);
            if !Path::exists(sub_dir_name) {
                let _ = DirBuilder::new().create(sub_dir_name);
            }

            let _ = set_current_dir(sub_dir_name);
            //println!("      in fv {fv} {:?}", current_dir());

            for i in 0..num_formulas {
                let name = format!("op{op}fv{fv}i{i}");
                let sub_dir_name1 = Path::new(&name);
                println!("              Generate formulas {}/{num_formulas}", i+1);
                if !Path::exists(sub_dir_name1) {
                    let _ = DirBuilder::new().recursive(true).create(sub_dir_name1);
                    let _ = set_current_dir(&sub_dir_name1);

                    let output_name = format!("op{}fv{}i{}", op, fv, i);
                    // populate file if either non-existent or empty
                    if !Path::exists(Path::new(&output_name.clone())) {
                        match Command::new(PATH_TO_FORMULA_GENERATOR).arg("-nolet").arg("-size").arg(&format!("{op}"))
                            .arg("-free_vars").arg(&format!("{fv}")).arg("-max_lb").arg(&format!("{max_lb}")).arg("-max_interval")
                            .arg(&format!("{max_rb}")).arg(&format!("op{}fv{}i{}", op, fv, i)).output() {
                            Ok(x) => {
                                let (sig, mfotl) = parse_gen_fma_output(x);
                                let new_mfotl = replace_pattern(mfotl);
                                write_gen_fma_output(sig, new_mfotl, output_name.clone());

                                let mut is_empty = false;
                                let file = File::open(output_name.clone());
                                if let Ok(wf) = file {
                                    is_empty = wf.metadata().unwrap().len() == 0;
                                }

                                for t in trace_len.clone() {
                                    println!("                      Generate data sets {t}");
                                    if !Path::exists(Path::new(&output_name.clone())) || (Path::exists(Path::new(&output_name.clone())) && is_empty) {
                                        create_base_data_set(output_name.clone(), t);
                                    }
                                }
                            }
                            Err(x) => { println!("err {:?}", x) }
                        };
                    }
                }

                let _ = set_current_dir(sub_dir_name1);
                let setback_path = format!("{PATH_TO_EXPERIMENTS}/Operators_{op}/Free_variables_{fv}");
                let _ = set_current_dir(setback_path);
            }

            let setback_path = format!("{PATH_TO_EXPERIMENTS}/Operators_{op}");
            let _ = set_current_dir(setback_path);
        }
        let _ = set_current_dir(PATH_TO_EXPERIMENTS);
    }
}

fn timely_benchmark(worker_num: Vec<i32>, operators: Vec<i32>, free_variables: Vec<i32>, trace_len: Vec<i32>, num_formulas: i32, runs: i32, result: &mut HashMap<(String, i32, i32, i32, i32, i32), u128>) {
    let _ = set_current_dir(PATH_TO_EXPERIMENTS);
    for op in operators {
        let name = format!("Operators_{}", op);
        let dir_name = Path::new(&name);

        for fv in free_variables.clone() {
            let _ = set_current_dir(dir_name);
            let name = format!("Free_variables_{fv}");
            let sub_dir_name = Path::new(&name);
            let _ = set_current_dir(sub_dir_name);

            for i in 0..num_formulas {
                let name = format!("op{op}fv{fv}i{i}");
                let sub_dir_name1 = Path::new(&name);
                let _ = set_current_dir(&sub_dir_name1);

                let mfotl = format!("{name}.mfotl");
                let formula = File::open(mfotl);
                if let Ok(file) = formula {
                    let mut form = "".to_string();
                    for f in BufReader::new(file).lines() {
                        form.push_str(&f.unwrap());
                    }

                    let path_to_folder = format!("{PATH_TO_EXPERIMENTS}/Operators_{op}/Free_variables_{fv}/{name}");

                    // populate file if either non-existent or empty
                    for worker in worker_num.clone() {
                        for t in trace_len.clone() {
                            let mut running_sum = 0;
                            println!("{}", format!("Operators_{op}/Free_variables_{fv}/{name} Worker {worker} trace {t}"));
                            for _ in 0..runs {
                                let _ = fs::remove_file("res.txt");
                                let start = Instant::now();
                                match Command::new(PATH_TO_TIMELYMON).arg(&form)
                                    .arg("-m").arg("0")
                                    .arg("-o").arg("res.txt")
                                    .arg("-w").arg(&format!("{worker}"))
                                    .arg(&format!("{path_to_folder}/wm_{name}_{t}.csv"))
                                    .output() {
                                    Ok(_output) => {
                                        /*println!("status: {}", output.status);
                                        println!("stdout: {}", String::from_utf8_lossy(&output.stdout));
                                        println!("stderr: {}", String::from_utf8_lossy(&output.stderr));*/
                                    }
                                    Err(_) => {}
                                }
                                // run command
                                let end = start.elapsed().as_micros();
                                running_sum += end;
                                println!("TimelyMon End {:?}", end);

                                if !verify_timely_result(format!("result_verimon_{name}_{t}")) {
                                    println!("----------------------------------------------------------------------------- False");
                                    let msg = format!("ERROR in :Operators {op} fv {fv} trace {t}");
                                    //panic!("{}", msg)
                                }
                                //println!("-----------------------------------------------{:?}", verify_result(format!("result_verimon_{name}_{t}")));
                                //assert!(verify_result(format!("result_verimon_{name}_{t}")));

                            }

                            let avg = running_sum / runs as u128;
                            let key = (format!("Timely"), op, fv, i, worker, t);

                            result.insert(key.clone(), avg);
                        }
                    }

                    /*
                    for worker in worker_num.clone() {
                        for t in trace_len.clone() {
                            let mut running_sum = 0;
                            println!("{}", format!("Operators_{op}/Free_variables_{fv}/{name} {worker} trace {t} Timely Worker"));
                            for _ in 0..runs {
                                let _ = fs::remove_file("res.txt");
                                let start = Instant::now();
                                match Command::new(PATH_TO_TIMELYMON).arg(&form)
                                    .arg("-m").arg("0")
                                    .arg("-o").arg("res.txt")
                                    .arg("-w").arg(&format!("{worker}"))
                                    .arg(&format!("{path_to_folder}/{name}_{t}_delayed.csv"))
                                    .output() {
                                    Ok(_output) => {
                                        //println!("status: {}", output.status);
                                        //println!("stdout: {}", String::from_utf8_lossy(&output.stdout));
                                        //println!("stderr: {}", String::from_utf8_lossy(&output.stderr));
                                    }
                                    Err(_) => {}
                                }
                                // run command
                                let end = start.elapsed().as_micros();
                                running_sum += end;
                                //println!("End {:?}", end);

                                // eventually assert!() and break if error
                                if !verify_result(format!("result_verimon_{name}_{t}")) {
                                    println!("----------------------------------------------------------------------------- False")
                                }
                                //println!("-----------------------------------------------{:?}", verify_result(format!("result_verimon_{name}_{t}")));
                                //assert!(verify_result(format!("result_verimon_{name}_{t}")));
                            }

                            let avg = running_sum / runs as u128;
                            let key = (format!("OOO-Timely"), op, fv, i, worker, t);
                            //println!("Average: {:?}", avg);

                            result.insert(key.clone(), avg);
                        }
                    }*/

                    // monpoly
                    /*for t in trace_len.clone() {
                        let mut running_sum = 0;
                        println!("{}", format!("Operators_{op}/Free_variables_{fv}/{name} trace {t} MonPoly"));
                        for _ in 0..runs {
                            let start = Instant::now();
                            match Command::new(PATH_TO_VERIMON).arg("-sig")
                                .arg(&format!("{name}.sig")).arg("-formula")
                                .arg(&format!("{name}.mfotl")).arg("-log")
                                .arg(&format!("verimon_{name}_{t}")).output() {
                                Ok(x) => {
                                    let data_file = format!("result_monpoly_{name}_{t}");
                                    write_data_file(x, data_file, "".to_string(), false);
                                }
                                Err(_) => {}
                            }
                            // run command
                            let end = start.elapsed().as_micros();
                            running_sum += end;
                            println!("MonPoly End {:?}", end);
                        }

                        if !verify_monpoly_result(format!("result_verimon_{name}_{t}"), format!("result_monpoly_{name}_{t}")) {
                            println!("----------------------------------------------------------------------------- False");
                            let msg = format!("ERROR in :Operators {op} fv {fv} trace {t}");
                            //panic!("{}", msg)
                        }

                        let avg = running_sum / runs as u128;
                        let key = (format!("MonPoly"), op, fv, i, 1, t);
                        //println!("Average: {:?}", avg);

                        result.insert(key.clone(), avg);

                        println!("\n");
                    }*/

                    /*for t in trace_len.clone() {
                        let mut running_sum = 0;
                        println!("{}", format!("Operators_{op}/Free_variables_{fv}/{name} trace {t} VeriMon"));
                        for _ in 0..runs {
                            let start = Instant::now();
                            match Command::new(PATH_TO_VERIMON).arg("-sig")
                                .arg(&format!("{name}.sig")).arg("-formula")
                                .arg(&format!("{name}.mfotl")).arg("-verified")
                                .arg("-log").arg(&format!("verimon_{name}_{t}")).output() {
                                Ok(_output) => {
                                    /*println!("status: {}", output.status);
                                    println!("stdout: {}", String::from_utf8_lossy(&output.stdout));
                                    println!("stderr: {}", String::from_utf8_lossy(&output.stderr));*/
                                }
                                Err(_) => {}
                            }
                            // run command
                            let end = start.elapsed().as_micros();
                            running_sum += end;
                            //println!("Verimon End {:?}", end);
                        }

                        let avg = running_sum / runs as u128;
                        let key = (format!("Verimon"), op, fv, i, 1, t);
                        //println!("Average: {:?}", avg);

                        result.insert(key.clone(), avg);
                    }*/
                } else {
                    println!("{op} {fv} {i} is missing formula");
                }

                let _ = set_current_dir(sub_dir_name1);
                let setback_path = format!("{PATH_TO_EXPERIMENTS}/Operators_{op}/Free_variables_{fv}");
                let _ = set_current_dir(setback_path);
            }

            let setback_path = format!("{PATH_TO_EXPERIMENTS}/Operators_{op}");
            let _ = set_current_dir(setback_path);
        }
        let _ = set_current_dir(PATH_TO_EXPERIMENTS);
    }
}

fn verify_results(actual: BTreeMap<usize, Vec<Vec<Constant>>>, mut expected: BTreeMap<usize, Vec<Vec<Constant>>>, ) -> bool {
    if actual.len() != expected.len() {
        println!("Unequal length actual {} != expected {}", actual.len(), expected.len());
        return false;
    }

    for (key, values1) in actual {
        return if let Some(exp_values1) = expected.get_mut(&key) {
            let mut values = HashSet::with_capacity(values1.len());
            for v in values1 { values.insert(v); }
            let mut exp_values = HashSet::with_capacity(exp_values1.len());
            for v in exp_values1 { exp_values.insert(v.clone()); }

            if values.len() != exp_values.len() {
                println!("Unequal number of records at Keys: ({})   actual {} != expected {}", key, values.len(), exp_values.len());
                return false;
            }

            values.iter().all(|x| if exp_values.contains(x) { true } else {
                println!("Tuple {:?} is not in expected results", x);
                false
            })
        } else {
            println!("Miss matching Keys: ({})", key);
            false
        }
    }
    println!("Results Verified succesfully");
    true
}

fn verify_monpoly_result(ver_res: String, mon_res: String) -> bool {
    let verimon_results = parse_verimon_results(ver_res, (true, MAX_TS));
    let monpoly_res = parse_verimon_results(mon_res, (true, MAX_TS));
    // we need to include a amx_tp to force verimon and monpoly to evaluate, if it is included then btree is trace length + 1

    if verimon_results.is_empty() && monpoly_res.is_empty() {
        return true
    }

    return verify_results(monpoly_res, verimon_results);
}

fn verify_timely_result(ver_res: String) -> bool {
    let verimon_results = parse_verimon_results(ver_res, (true, MAX_TS));
    let timelymon_res = parse_timelymon_results(format!("res.txt"));
    // we need to include a amx_tp to force verimon and monpoly to evaluate, if it is included then btree is trace length + 1

    if verimon_results.is_empty() && timelymon_res.is_empty() {
        return true
    }

    return verify_results(timelymon_res, verimon_results);
}

fn main() {
    let operators = vec![10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150];//, 55, 60, 65, 70, 75, 80, 85,
                         //90, 95, 100, 105, 110, 115, 120, 125, 130, 135, 140, 145, 150];//vec![5, 10, 15, 20, 25, 30, 35, 40, 45, 50];//,6,7,8,9,10];//, 5, 10, 15]; //4, 5, 10, 15, 20, 25, 30];//, 35, 40, 45 50];//, 10, 20, 30, 40, 50];//, 60, 70, 80, 90, 100];
    let free_variables = vec![4];//2, 4, 6, 8, 10];
    let trace_len = vec![10];//, 150, 200];//, 100];//, 250];
    let num_formulas = 10;
    let worker_num = vec![6];//1,2,4,6];
    let runs = 1;
    let max_lb = 1;
    let max_rb = 25;

    let mut result: HashMap<(String, i32, i32, i32, i32, i32), u128> = HashMap::new();
    data_generation(operators.clone(), free_variables.clone(), trace_len.clone(), num_formulas, max_lb, max_rb);

    // in-order, maximum delay
        // 5 runs, 1,2,4,6
    timely_benchmark(worker_num, operators.clone(), free_variables.clone(), trace_len.clone(), num_formulas, runs, &mut result);

    // store result of each run as a vector of points
    // the vector is stored in a hashmap
    // the trend line is computed as an average also in a hashmap
    results_to_csv(&mut result);

    // store and provide results
    for (k,v) in result {
        println!("{:?}: {:?}", k, v);
    }
}

fn results_to_csv(result: &mut HashMap<(String, i32, i32, i32, i32, i32), u128>) {
    let file = File::create(PATH_TO_RESULTS);
    if let Ok(wf) = file {
        let mut r = "".to_string();
        // trace -> ops -> fvs -> formula -> tool -> worker -> average
        for ((tool, ops, fv, num_formual, worker, trace), v) in result {
            r.push_str(&format!("{:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}\n", trace, ops, fv, num_formual, tool, worker, v));
        }
        let _ = BufWriter::new(&wf).write_all(r.as_ref());
    } else {
        println!("File not found {:?}", PATH_TO_RESULTS)
    }
}

#[cfg(test)]
mod tests {
    use replace_pattern;

    #[test]
    fn t1() {
        let act = format!("(EVENTUALLY(0,23] (((17 = 38) AND (NOT P0())) AND (NEXT[0,4) (((PREVIOUS(0,1) (P0() AND (ONCE(0,*) ((NOT (ONCE[0,*) P0())) SINCE[0,*) (34 = 26))))) AND ((NOT (P1() OR (21 = 16))) UNTIL(0,6] P1())) AND (((32 = 4) AND P1()) OR P2())))))");
        let exp = format!("(EVENTUALLY(0,23] (((17 = 38) AND (NOT P0())) AND (NEXT[0,4) (((PREVIOUS[0,1) (P0() AND (ONCE(0,*) ((NOT (ONCE[0,*) P0())) SINCE[0,*) (34 = 26))))) AND ((NOT (P1() OR (21 = 16))) UNTIL(0,6] P1())) AND (((32 = 4) AND P1()) OR P2())))))");

        let res = replace_pattern(act);
        assert_eq!(res, exp);
    }

    #[test]
    fn t2() {
        let act = format!("((NOT ((NOT (ONCE(0,4] (9 = 39))) SINCE(0,*) (PREVIOUS(0,10) (EVENTUALLY(0,10] (((P0() SINCE[0,*) (P0() AND (NOT ((3 = 24) SINCE[0,*) (42 = 17))))) AND (NOT (ONCE[0,*) (28 = 38)))) AND (NOT (9 = 42))))))) SINCE[0,0] (((((NOT P1()) SINCE[0,22] P1()) AND P1()) SINCE[0,20] P2()) AND (NOT P1())))");
        let exp = format!("((NOT ((NOT (ONCE(0,4] (9 = 39))) SINCE(0,*) (PREVIOUS(0,10) (EVENTUALLY(0,10] (((P0() SINCE[0,*) (P0() AND (NOT ((3 = 24) SINCE[0,*) (42 = 17))))) AND (NOT (ONCE[0,*) (28 = 38)))) AND (NOT (9 = 42))))))) SINCE[0,0] (((((NOT P1()) SINCE[0,22] P1()) AND P1()) SINCE[0,20] P2()) AND (NOT P1())))");

        let res = replace_pattern(act);
        assert_eq!(res, exp);
    }
}