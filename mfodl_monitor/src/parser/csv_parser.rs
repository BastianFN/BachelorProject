use nom::IResult::*;
use nom::{alphanumeric, digit, IResult};
use std::collections::{BTreeMap, HashSet};
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

use parser::formula_syntax_tree::{Formula, Constant, build_fact_const};
use std::fs::File;
use Formula::CstFact;

use parser::csv_parser::ParserReturn::Watermark;
use parser::csv_parser::Segment::{Epoch, Seg};

use parser::formula_syntax_tree::Constant::Str;
use parser::formula_syntax_tree::Constant::Int;

pub enum Segment {
    Epoch(i64),
    Seg(usize, usize, Vec<String>)
}

//TODO use indicies to reduce copying/memory footprint
pub fn parse_file_to_segments(path: PathBuf) -> Vec<Segment> {
    let mut result : Vec<Segment> = Vec::with_capacity(1000);
    let mut current_is_set = false;
    let mut current_tp = 0;
    let mut current_ts = 0;

    if let Ok(f) = File::open(path.clone()) {
        let mut current_segment = Vec::with_capacity(1000);
        for line in BufReader::new(&f).lines() {
            let l = line.unwrap().clone();
            match parser_extended_wrapper(l.clone()) {
                ParserReturn::Data(tp, ts, val) => {
                    if current_is_set {
                        if tp == current_tp {
                            current_segment.push(val.to_string())
                        } else {
                            if !current_segment.is_empty() {
                                result.push(Seg(current_tp, current_ts, current_segment.clone()));
                                current_segment.clear();
                            }
                            current_tp = tp;
                            current_ts = ts;
                            current_segment.push( val.to_string())
                        }
                    } else {
                        current_tp = tp;
                        current_ts = ts;
                        current_is_set = true;
                        current_segment.push(val.to_string())
                    }
                }
                Watermark(wm) => {
                    result.push(Seg(current_tp, current_ts, current_segment.clone()));
                    result.push(Epoch(wm));
                    current_segment.clear();
                }
                _ => {}
            }
        }

        result.push(Seg(current_tp, current_ts, current_segment));
    }
    result
}

pub fn parse_line_to_triple(line: String) -> Option<(Formula, usize, usize)> {
    let result = parse_command(&line);
    match result {
        Done(_a, tri) => Some(tri),
        _ => None,
    }
}

pub fn parser_extended_wrapper(line: String) -> ParserReturn {
    let mut res = ParserReturn::Error("".to_string());

    match parser_extended(&line) {
        Done(_, p) => {res = p}
        _ => {}
    }

    return res;
}

pub fn parse_csv_file_to_vec(path: PathBuf) -> BTreeMap<(usize, usize), Vec<String>> {
    let mut result: BTreeMap<(usize, usize), Vec<String>> = BTreeMap::new();
    match File::open(path.clone()) {
        Ok(f) => {
            let file = BufReader::new(&f);
            for line in file.lines() {
                let l = line.unwrap();
                match parse_command(&l) {
                    Done(_a, (f, tp, ts)) => {
                        result
                            .entry((tp.clone(), ts.clone()))
                            .or_default()
                            .push(f.to_string());
                    }
                    _ => {}
                }
            }
        }
        Err(e) => {println!("Error: {}", e)}
    }
    result
}

pub fn parse_delay_csv_file_to_vec(path: String) -> (usize, Vec<(usize, usize, Vec<String>)>) {
    let mut result: Vec<(usize, usize, String)> = Vec::new();
    let mut tps: HashSet<usize> = HashSet::new();
    if let Ok(f) = File::open(path.clone()) {
        let file = BufReader::new(&f);
        for line in file.lines() {
            let l = line.unwrap();
            match parse_delay_command(&l) {
                Done(_a, (f, tp, ts)) => {
                    result.push((tp.clone(), ts.clone(), f.to_string()));
                    tps.insert(tp);
                }
                _ => {
                }
            }
        }
    }

    // group as much as possible to use give iterator
    // aggregate all connected elements
    let mut agg_res: Vec<(usize, usize, Vec<String>)> = Vec::new();
    let mut tmp_res = Vec::new();

    for i in 0..(result.len() - 2) {
        let (tp, ts, res) = result[i].clone();
        let curr = result[i].0;
        let next = result[i + 1].0;

        if curr != next {
            // end of segment
            tmp_res.push(res);
            agg_res.push((tp, ts, tmp_res.clone()));
            tmp_res.clear();
        } else {
            // inside segment
            tmp_res.push(res);
        }
    }

    let ind = result.len() - 1;
    tmp_res.push(result[ind].2.clone());
    agg_res.push((result[ind].0, result[ind].1, tmp_res.clone()));

    (tps.len(), agg_res)
}

pub fn parse_verimon_results(path: String, (flag, max_ts) : (bool, usize)) -> BTreeMap<usize, Vec<Vec<Constant>>> {
    let mut result: BTreeMap<usize, Vec<Vec<Constant>>> = BTreeMap::new();
    if let Ok(f) = File::open(path) {
        let file = BufReader::new(&f);
        for line in file.lines() {
            let l = line.unwrap();
            match parse_result(&l) {
                Done(_a, (tp, ts, mut vec)) => {
                    //println!("tp {} and ts {} length {}", tp.clone(), ts.clone(), vec.len());

                    if flag {
                        if ts != max_ts {
                            result.entry(tp.clone()).or_default().append(&mut vec);
                        }
                    } else {
                        result.entry(tp.clone()).or_default().append(&mut vec);
                    }
                }
                _ => { }
            }
        }
    }

    result
}

pub fn parse_timelymon_results(path: String) -> BTreeMap<usize, Vec<Vec<Constant>>> {
    let mut result: BTreeMap<usize, Vec<Vec<Constant>>> = BTreeMap::new();
    if let Ok(f) = File::open(path) {
        let file = BufReader::new(&f);
        for line in file.lines() {
            let l = line.unwrap();
            match parse_timely_result(&l) {
                Done(_a, (tp, mut vec)) => {
                    //println!("tp {} and ts {} length {}", tp.clone(), ts.clone(), vec.len());
                    result
                        .entry(tp.clone())
                        .or_default()
                        .append(&mut vec);
                }
                _ => {
                    println!("-------------------- error  while parsing timelymon result file ------------------")
                }
            }
        }
    }

    result
}

pub fn parse_gen_fma_wrapper(input: String) -> (String, String) {
    match parse_gen_fma(&input) {
        Done(_, (sig, mft)) => {
            (sig.to_string(), mft.to_string())
        }
        _ => ("".to_string(), "".to_string())
    }
}

named!(parse_gen_fma<&str, (String, String)>,
   ws!(do_parse!(
        take_until_and_consume!("SIGNATURE:") >>
        sig: take_until!("MFOTL FORMULA:") >>
        tag!("MFOTL FORMULA:") >>
        formula: take_while!(|x| true) >>
        (sig.to_string(), formula.to_string())
    )
   )
);

// todo parse signature
named!(parse_result<&str, (usize, usize, Vec<Vec<Constant>>)>,
    ws!(do_parse!(
        //tag!("@1 (time point 1): ") >>
        tag!("@") >>
        ts: digit >>
        tag!("(time point ") >>
        //tag!(" (time point ") >>
        tp: digit >>
        tag!("): ") >>
        vecs: tuples >>
        (tp.parse::<usize>().unwrap(), ts.parse::<usize>().unwrap(), vecs)
    ))
);

named!(parse_timely_result<&str, (usize, Vec<Vec<Constant>>)>,
    ws!(do_parse!(
        //tag!("@1 (time point 1): ") >>
        tag!("@") >>
        ts: digit >>
        tag!("(time point ") >>
        tp: digit >>
        tag!("): ") >>
        vecs: tuples >>
        (tp.parse::<usize>().unwrap(), vecs)
    ))
);

fn tuples(input: &str) -> IResult<&str, Vec<Vec<Constant>>> {
    let mut res = Vec::with_capacity(2);
    let mut str = input;
    // repeat calling tuple
    loop {
        match tuple(str) {
            Done(st, r) => {
                res.push(r);
                str = st;
            }
            _ => break,
        }
    }

    return Done(str, res);
}

// TODO new parser with all more than 3 or 4 formats

named!(tuple<&str, Vec<Constant>>,
    ws!(alt_complete!(
         tuple3 | tuple2 | tuple4 | ntuple
    ))
);

named!(tuple2<&str, Vec<Constant>>,
    ws!(do_parse!(
        tag!("(") >>
        t0: arg >>
        tag!(",") >>
        t1: arg >>
        tag!(")") >>
        (vec![t0, t1])
    ))
);

named!(tuple3<&str, Vec<Constant>>,
    ws!(do_parse!(
        tag!("(") >>
        t0: arg >>
        tag!(",") >>
        t1: arg >>
        tag!(",") >>
        t2: arg >>
        tag!(")") >>
        (vec![t0, t1, t2])
    ))
);

named!(tuple4<&str, Vec<Constant>>,
    ws!(do_parse!(
        tag!("(") >>
        t0: arg >>
        tag!(",") >>
        t1: arg >>
        tag!(",") >>
        t2: arg >>
        tag!(",") >>
        t3: arg >>
        tag!(")") >>
        (vec![t0, t1, t2, t3])
    ))
);

named!(ntuple<&str, Vec<Constant>>,
    ws!(do_parse!(
        tag!("(") >>
        args: separated_list_complete!(tag!(","), arg) >>
        tag!(")") >>
        (args)
    ))
);

named!(parse_delay_command<&str, (Formula, usize, usize)>,
    ws!(do_parse!(
        digit >> tag!("'") >>
        command: take_until!(",") >>
        char!(',') >>
        tag!("tp") >>
        char!('=') >>
        tp: digit >>
        char!(',') >>
        tag!("ts") >>
        char!('=') >>
        ts: digit >>
        char!(',') >>
        alphanumeric >>
        char!('=') >>
        args: separated_list_complete!(attr_name, arg) >>
        (build_fact_const(command, args), tp.parse::<usize>().unwrap(), ts.parse::<usize>().unwrap())
    ))
);

named!(parse_command<&str, (Formula, usize, usize)>,
    ws!(do_parse!(
        command: take_until!(",") >>
        char!(',') >>
        tag!("tp") >>
        char!('=') >>
        tp: digit >>
        char!(',') >>
        tag!("ts") >>
        char!('=') >>
        ts: digit >>
        char!(',') >>
        alphanumeric >>
        char!('=') >>
        args: separated_list_complete!(attr_name, arg) >>
        (build_fact_const(command, args), tp.parse::<usize>().unwrap(), ts.parse::<usize>().unwrap())
    ))
);


#[derive(PartialEq, Debug)]
pub enum ParserReturn {
    Data(usize, usize, Formula),
    Watermark(i64),
    Error(String)
}

named!(parser_extended<&str, ParserReturn>,
    ws!(alt!(
        parse_command_extended | parse_watermark_extended | error_extended
    ))
);


fn error_extended(input: &str) -> IResult<&str, ParserReturn> {
    let err = format!("Unable to parse: {}", input);
    return Done("", ParserReturn::Error(err));
}


named!(parse_watermark_extended<&str, ParserReturn>,
    ws!(do_parse!(
        opt!(take_until!("W")) >>
        opt!(take_until!("w")) >>
        tag_no_case!("watermark") >>
        sign : opt!(tag!("-")) >>
        watermark : digit >>
        opt!(tag!("<")) >>
        (Watermark(transform_number(sign, watermark)))
    ))
);

named!(parse_delay_command_extended<&str, ParserReturn>,
    ws!(do_parse!(
        digit >> tag!("'") >> command: take_until!(",") >>
        char!(',') >> tag!("tp") >> char!('=') >> tp: digit >>
        char!(',') >> tag!("ts") >> char!('=') >> ts: digit >>
        char!(',') >> alphanumeric >> char!('=') >> args: separated_list_complete!(attr_name, arg) >>
        (ParserReturn::Data(tp.parse::<usize>().unwrap(), ts.parse::<usize>().unwrap(), build_fact_const(command, args)))
    ))
);

named!(parse_command_extended<&str, ParserReturn>,
    ws!(do_parse!(
        opt!(digit) >> opt!(tag!("'")) >> command: take_until!(",") >>
        char!(',') >> tag!("tp") >> char!('=') >> tp: digit >>
        char!(',') >> tag!("ts") >> char!('=') >> ts: digit >>
        opt_args: opt!(complete!(tailing_arguments)) >>
        //char!(',') >> alphanumeric >> char!('=') >> args: separated_list_complete!(attr_name, arg) >>
        (match opt_args {
            Some(args) => ParserReturn::Data(tp.parse::<usize>().unwrap(), ts.parse::<usize>().unwrap(), build_fact_const(command, args)),
            None => ParserReturn::Data(tp.parse::<usize>().unwrap(), ts.parse::<usize>().unwrap(), build_fact_const(command, vec![]))
        })
    ))
);

named!(tailing_arguments<&str, Vec<Constant>>,
   ws!(do_parse!(
        char!(',') >> alphanumeric >> char!('=') >>
        args: separated_list_complete!(attr_name, arg) >>
        (args)
    ))
);

named!(parse_empty_command<&str, ParserReturn>,
    ws!(do_parse!(
        opt!(digit) >> opt!(tag!("'")) >> command: take_until!(",") >>
        char!(',') >> tag!("tp") >> char!('=') >> tp: digit >>
        char!(',') >> tag!("ts") >> char!('=') >> ts: digit >>
        (ParserReturn::Data(tp.parse::<usize>().unwrap(), ts.parse::<usize>().unwrap(), CstFact(command.to_string(), vec![])))
    ))
);

fn transform_number(opt: Option<&str>, num: &str) -> i64 {
    let mut n = num.parse::<i64>().unwrap();
    match opt {
        None => {}
        Some(_t) => {n = n * -1}
    }

    return n.clone();
}

named!(attr_name<&str, &str>,
    ws!(do_parse!(
        tag!(",") >>
        name: alphanumeric >>
        tag!("=") >>
        (name)
    ))
);

named!(arg<&str, Constant>,
    ws!(alt!(
        number | string | unknown
    ))
);

named!(unknown<&str, Constant>,
    ws!(do_parse!(
        tag!("[unknown]") >>
        (Str("[unknown]".to_string()))
    ))
);

named!(single_qoutes<&str, Constant>,
    ws!(do_parse!(
        tag!("'") >>
        s : take_until!("'") >>
        tag!("'") >>
        (Str(s.to_string()))
    ))
);

named!(double_qoutes<&str, Constant>,
    ws!(do_parse!(
        tag!("\"") >>
        s : take_until!("\"") >>
        tag!("\"") >>
        (Str(s.to_string()))
    ))
);

named!(no_qoutes<&str, Constant>,
    ws!(do_parse!(
        s: alphanumeric >>
        (Str(s.to_string()))
    ))
);

named!(string<&str, Constant>,
    ws!(alt!(
        single_qoutes | double_qoutes | no_qoutes
    ))
);

named!(number<&str, Constant>,
    ws!(do_parse!(
        n: digit >>
        (Int(n.parse::<i32>().unwrap()))
    ))
);

#[cfg(test)]
mod tests {
    
    use parser::csv_parser::{parse_result, tuple, tuples, parse_watermark_extended, parser_extended, ParserReturn};
    use parser::csv_parser::ParserReturn::{Data, Watermark};
    use parser::formula_syntax_tree::Constant::Int;
    use parser::formula_syntax_tree::Formula::*;

    #[test]
    fn test_parser_ext() {
        let t = parser_extended("A, tp=7, ts=7, x0=111613425, x1=2").unwrap();
        assert_eq!(Data(7,7, CstFact("A".to_string(), vec![(Int(111613425)), (Int(2))])), t.1);
        let t1 = parser_extended("11'B, tp=11, ts=11, x0=789708455, x1=1").unwrap();
        assert_eq!(Data(11,11, CstFact("B".to_string(), vec![(Int(789708455)), (Int(1))])), t1.1);
        let mut t2 = parser_extended("12'>WATERMARK 6<").unwrap();
        assert_eq!(Watermark(6), t2.1);
        t2 = parser_extended("12'>WATERMARK -6<").unwrap();
        assert_eq!(Watermark(-6), t2.1);
        t2 = parser_extended(">WATERMARK -6<").unwrap();
        assert_eq!(Watermark(-6), t2.1);
        t2 = parser_extended(">WATERMARk -6<").unwrap();
        assert_eq!(Watermark(-6), t2.1);
        let t3 = parser_extended("gsfdg343fsdfsdf").unwrap();
        println!("Remainder: {}      ParserReturn: {}", t3.0.len(), match t3.1 {
            ParserReturn::Error(e) => {e},
            _ => {"".to_string()}
        });
    }

    #[test]
    fn test_parser_ext1() {
        let t = parser_extended("A, tp=7, ts=7").unwrap();
        assert_eq!(Data(7, 7, CstFact("A".to_string(), vec![])), t.1);
    }

    #[test]
    fn tuple_test() {
        let tmp = tuple("(1,2,3)").unwrap().1;
        assert_eq!(vec![Int(1), (Int(2)), (Int(3))], tmp);

        let tmp = tuple("(10,20,30)").unwrap().1;
        assert_eq!(vec![(Int(10)), (Int(20)), (Int(30))], tmp);
    }

    #[test]
    fn tuples_test() {
        let tmp = tuples("(1,2,3) (10,20,30)").unwrap().1;
        assert_eq!(
            vec![
                vec![(Int(1)), (Int(2)), (Int(3))],
                vec![(Int(10)), (Int(20)), (Int(30))]
            ],
            tmp
        );

        let tmp = tuples("(4,626199108,3) (8,177376714,1)").unwrap().1;
        assert_eq!(
            vec![
                vec![(Int(4)), (Int(626199108)), (Int(3))],
                vec![(Int(8)), (Int(177376714)), (Int(1))]
            ],
            tmp
        );
    }

    #[test]
    fn parse_result_test() {
        let tmp = parse_result("@1 (time point 1): (4,626199108,3) (8,177376714,1)")
            .unwrap()
            .1;
        assert_eq!(tmp.0, 1);
        assert_eq!(tmp.1, 1);
        assert_eq!(
            vec![
                vec![(Int(4)), (Int(626199108)), (Int(3))],
                vec![(Int(8)), (Int(177376714)), (Int(1))]
            ],
            tmp.2
        );
    }

    #[test]
    fn water() {
        let mut tmp = parse_watermark_extended("12'>WATERMARK 6<").unwrap();
        assert_eq!(tmp.1, Watermark(6));

        tmp = parse_watermark_extended("12'>WATERMARK -1<").unwrap();
        assert_eq!(tmp.1, Watermark(-1));

        let mut tmp1 = parse_watermark_extended(">WATERMARK 12<").unwrap();
        assert_eq!(tmp1.1, Watermark(12));

        tmp1 = parse_watermark_extended(">WATErMARK 100<").unwrap();
        assert_eq!(tmp1.1, Watermark(100));

        tmp1 = parse_watermark_extended("dasdfafagfsfdsad>WATErMARK 100<").unwrap();
        assert_eq!(tmp1.1, Watermark(100));
    }
}
