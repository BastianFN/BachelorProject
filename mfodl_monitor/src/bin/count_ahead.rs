extern crate mfodl_monitor;
extern crate timely;

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use timely::dataflow::operators::{Capture, Probe, UnorderedInput};
use timely::dataflow::operators::capture::Extract;
use mfodl_monitor::{create_dataflow, parse_formula};
use mfodl_monitor::dataflow_constructor::types::{default_options, TimeFlowValues};
use mfodl_monitor::dataflow_constructor::types::TimeFlowValues::Timestamp;
use mfodl_monitor::parser::csv_parser::{parse_file_to_segments, parser_extended_wrapper, ParserReturn, Segment};
use mfodl_monitor::parser::csv_parser::Segment::{Epoch, Seg};

fn main() {
    let num_workers = 6;
    let path = "./data/Negated_Triangle/50K50TS.csv";
    let policy = "((Once[0,7] A(a,b)) && B(b,c)) && (~Eventually[0,7] C(c,a))";

    let mut watermarks = Vec::new();
    // get lines of watermarks
    match File::open(path.clone()) {
        Ok(f) => {
            let file = BufReader::new(&f);
            for (line_count, line) in file.lines().enumerate() {
                let l = line.unwrap();
                match parser_extended_wrapper(l) {
                    ParserReturn::Watermark(_) => {
                        watermarks.push(line_count+1)
                    }
                    _ => {}
                }
            }
        }
        Err(e) => {println!("Error: {}", e)}
    }

    let mut output_results : Vec<usize> = Vec::new();
    let segments = parse_file_to_segments(PathBuf::from(path));

    for wm in watermarks {
        let mut counter = 0;
        let mut relevant_segs : Vec<Segment> = Vec::new();
        for seg in segments.iter() {
            match seg {
                Epoch(w) => {
                    relevant_segs.push(Epoch(w.clone()));
                    counter += 1;
                    if counter == wm {
                        break
                    }
                }
                Seg(tp, ts, v) => {
                    if !v.is_empty() {
                        relevant_segs.push(Seg(*tp, *ts, v.clone()));
                        counter += v.len();
                        if counter == wm {
                            break
                        }
                    }
                }
            }
        }

        let (send, recv) = std::sync::mpsc::channel();
        let send = std::sync::Arc::new(std::sync::Mutex::new(send));

        timely::execute(timely::Config::process(num_workers), move |worker| {
            let mut threshold = 0;
            let send = send.lock().unwrap().clone();
            let (mut input, mut cap, mut time_input, mut time_cap,_probe) = worker.dataflow::<usize, _, _>(|scope| {
                let ((time_input, time_cap), time_stream) = scope.new_unordered_input::<TimeFlowValues>();
                let ((input, input_cap), stream) = scope.new_unordered_input::<String>();

                let (_attrs, output) = create_dataflow(parse_formula(&policy), stream, time_stream, default_options());

                let probe = output.probe();
                output.capture_into(send);

                (input, input_cap, time_input, time_cap, probe)
            });

            // Send data and step the workers
            if worker.index() == 0 {
                let mut max_wm = 0;
                for segs in relevant_segs.iter() {
                    match segs {
                        Epoch(wm) => {
                            let t = if *wm < 0 { 0 } else { *wm as usize };
                            time_cap = time_cap.delayed(&t);
                            cap = cap.delayed(&t);
                            max_wm = t
                        }
                        Seg(tp, ts, val) => {
                            time_input.session(time_cap.delayed(&tp)).give(Timestamp(ts.clone()));
                            threshold = threshold + val.len();
                            input
                                .session(cap.delayed(&tp))
                                .give_iterator(val.clone().into_iter());
                        }
                    }
                }

                let new_prod = max_wm + 1;
                time_input.session(cap.delayed(&new_prod)).give(TimeFlowValues::EOS);
                input.session(cap.delayed(&new_prod)).give("<eos>".parse().unwrap());
                worker.step();
            }
        }).unwrap();

        // Vector holding a tuple (timepoint, vector with data for each timestamp)
        let res: Vec<_> = recv
            .extract()
            .iter()
            .cloned()
            .map(|(time, tuples)| (time, tuples))
            .collect();

        let mut c = 0;
        for r in res.clone() {
            c += r.1.len();
        }

        //println!("WM {wm} length {}", c);
        println!("{c}");
        output_results.push(c);
    }

    //println!("{:?}", output_results);
}