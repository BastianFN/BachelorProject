#![allow(dead_code)]
#![allow(unused_doc_comments)]
use nom::IResult::Done;

use nom::{alphanumeric, digit, IResult};
use Formula::FormulaError;

use parser::formula_parser::Constant::{Int, Str};
use parser::formula_syntax_tree::*;
use timeunits::*;

/// formula       --> iff
/// iff           --> implication '<->' implication
/// implication   --> conj {'->' conj}
/// conj          --> disj {'&&' conj}
/// disj          --> temp_or_since {'||' disj}
/// temp_or_since --> temp_or_until {'<S>' temp_or_until}
/// temp_or_until --> formula_l1 {'<U>' formula_l1}
/// formula_l1    --> '~' formula_l1  |
///                   formula_l2
/// formula_l2    --> once            |
///                   always          |
///                   historically    |
///                   prev            |
///                   next            |
///                   '(' formula ')' |
///                   'E' var '.' formula_l1 |
///                   'A' var '.' formula_l1 |
///                   p(arg+) | p() | eos

// This is the public method to parse a formula.
pub fn parse_fact(s: &str) -> (String, Vec<Constant>) {
    match cl(s) {
        Done(_i, o) => o,
        _ => ("".to_string(), vec![]),
    }
}

named!(cl<&str, (String, Vec<Constant>)>,
    ws!(alt_complete!(
            clean_fact | clean_empty_fact
    ))
);

named!(clean_fact<&str, (String, Vec<Constant>)>,
    ws!(do_parse!(
        name: take_while!(|c| {
            let forbidden_fact_name_chars = ['(', ')', '<', '|', '[', ']', '@','.', '+', '?', '='];
            !forbidden_fact_name_chars.contains(&c)
        }) >>
        args: delimited!(
            char!('('),
            separated_list_complete!(char!(','), clean_number),
             char!(')')
        ) >>
        (name.to_string(), args)
    ))
);

named!(clean_number<&str, Constant>,
    ws!(do_parse!(
        n: digit >>
        (Int(n.parse::<i32>().unwrap()))
    ))
);

named!(clean_empty_fact<&str, (String, Vec<Constant>)>,
    ws!(do_parse!(
        name: take_while!(|c| {
            let forbidden_fact_name_chars = ['(', ')', '<', '|', '[', ']', '@','.', '+', '?', '='];
            !forbidden_fact_name_chars.contains(&c)
        }) >> args: tag!("()") >>
        ((name.to_string(), vec![]))
    ))
);

pub fn parse_formula(s: &str) -> Formula {
    let tmp = s.clone();
    match formula(s) {
        Done(_i, o) => o,
        IResult::Error(x) => {println!("Parser Error: {:?}   {:?}", x, tmp); FormulaError("Error".to_string())}
        IResult::Incomplete(c) => {println!("Incomplete: {:?}", c); FormulaError("Incomplete".to_string())}
    }
}

named!(formula<&str, Formula>,
    ws!(do_parse!(
        f: iff >>
        (f)
    ))
);

// iff = implication {'<->' implication}
named!(iff<&str, Formula>,
    ws!(do_parse!(
        lhs: implication >>
        rhs: opt!(complete!(ws!(do_parse!(
            tag!("EQUIV") >>
            f: implication >>
            (f)
        )))) >>
        (
            match rhs {
                Some(f) => build_iff(lhs, f),
                None    => lhs
            }
        )
    ))
);

// implication = conj {'->' conj}
named!(implication<&str, Formula>,
    ws!(do_parse!(
        lhs: conj >>
        rhs: opt!(complete!(ws!(do_parse!(
            tag!("IMPLIES") >>
            f: conj >>
            (f)
        )))) >>
        (
            match rhs {
                Some(f) => build_implication(lhs, f),
                None    => lhs
            }
        )
    ))
);

// conj = disj {'&&' conj}
named!(conj<&str, Formula>,
    ws!(do_parse!(
        lhs: disj >>
        rhs: opt!(complete!(ws!(do_parse!(
                 tag!("AND") >>
                 e: alt!(conj) >>
                 (e)
            )))) >>
        (
            match rhs {
                Some(v) => build_conj(lhs, v),
                None    => lhs
            }
        )
    ))
);

// disj = possibly {'||' disj}
named!(disj<&str, Formula>,
    ws!(do_parse!(
        lhs: temp_or_since >>
        rhs: opt!(complete!(ws!(do_parse!(
                 tag!("OR") >>
                 e: disj >>
                 (e)
            )))) >>
        (
            match rhs {
                Some(v) => build_disj(lhs, v),
                None    => lhs
            }
        )
    ))
);

named!(temp_or_since<&str, Formula>,
    ws!(do_parse!(
        lhs: temp_or_until >>
        rhs: opt!(complete!(ws!(do_parse!(
                 tag!("SINCE") >>
                 time_frame: interval >>
                 e: temp_or_until >>
                 ((e, time_frame))
            )))) >>
        (match lhs.clone() {
            Formula::Not(x) => {
                match rhs {
                    Some((v, interval)) => build_neg_since(*x, v, interval),
                    None => lhs
                }
            }
            /*Formula::NotEquals(x, y) => {
                match rhs {
                    Some((v, interval)) => build_neg_since(Equals(x,y), v, interval),
                    None => lhs
                }
            }*/
            _ => {
                match rhs {
                    Some((v, interval)) => build_since(lhs, v, interval),
                    None => lhs
                }
            }
        })
    ))
);

// temp_or_until = formula_l1 {'<U>' formula_l1}
named!(temp_or_until<&str, Formula>,
    ws!(do_parse!(
        lhs: formula_l1 >>
        rhs: opt!(complete!(ws!(do_parse!(
                 tag!("UNTIL") >>
                 time_frame: interval >>
                 e: formula_l1 >>
                 ((e, time_frame))
            )))) >>

        (match lhs.clone() {
            Formula::Not(x) => {
                match rhs {
                    Some((v, interval)) => build_neg_until(*x, v, interval),
                    None => lhs
                }
            }
            /*Formula::NotEquals(x, y) => {
                match rhs {
                    Some((v, interval)) => build_neg_until(Equals(x,y), v, interval),
                    None => lhs
                }
            }*/
            _ => {
                match rhs {
                    Some((v, interval)) => build_until(lhs, v, interval),
                    None => lhs
                }
            }
        })
    ))
);

// until = formula_l1 {'<U>' formula_l1}
named!(equals<&str, Formula>,
    ws!(do_parse!(
        var: variable >>
        tag!("=") >>
        val: ws!(alt_complete!(
                string | number | variable
            )) >>
        ({
            match var {
                Arg::Var(v) => build_equals(&v, val),
                _ => panic!("Parser error. Lhs of equals should be a variable name.")
            }
        })
    ))
);

named!(num_equals<&str, Formula>,
    ws!(do_parse!(
        var: digit >>
        tag!("=") >>
        val: digit >>
        ({
            let x = var.parse::<i64>().unwrap();
            let y = val.parse::<i64>().unwrap();

            if x == y {
                build_true()
            } else {
                build_false()
            }
        })
    ))
);

// formula_l1 = not | formula_l2
named!(formula_l1<&str, Formula>,
    ws!(alt_complete!(
        not | formula_l2
    ))
);

// formula_l2 = temporal | once | historically | always | prev | bracketted_formula |
//              exists | forall | fact
named!(formula_l2<&str, Formula>,
    ws!(alt_complete!(
        true_f | false_f | base_value | once | eventually | bracketted_formula | prev | next | forall | exists | empty_fact | fact | eos | num_equals | equals | always | historically
    ))
);

/*fn formula_l2(input: &str) -> IResult<&str, Formula> {
    //println!("input {:?}", input);
    if input == "false" {
        println!("WTF")
    }
    formula_l2_2(input)
}*/

named!(prev<&str, Formula>,
    ws!(do_parse!(
        tag!("PREVIOUS") >>
        time_frame: interval >>
        f: formula_l2 >>
        (build_prev(f, time_frame))
    ))
);

named!(next<&str, Formula>,
    ws!(do_parse!(
        tag!("NEXT") >>
        time_frame: interval >>
        f: formula_l2 >>
        (build_next(f, time_frame))
    ))
);

named!(always<&str, Formula>,
    ws!(do_parse!(
        tag!("ALWAYS") >>
        time_frame: interval >>
        rhs: formula_l1 >>
        (build_always(rhs, time_frame))
        )
    )
);

named!(historically<&str, Formula>,
    ws!(do_parse!(
        tag!("PAST_ALWAYS") >>
        time_frame: interval >>
        rhs: formula_l1 >>
        (build_historically(rhs, time_frame))
        )
    )
);

named!(once<&str, Formula>,
    ws!(do_parse!(
        tag!("ONCE") >>
        time_frame: interval >>
        rhs: formula_l2 >>
        (build_once(rhs, time_frame)))
    )
);

named!(eventually<&str, Formula>,
    ws!(do_parse!(
        tag!("EVENTUALLY") >>
        time_frame: interval >>
        rhs: formula_l2 >>
            (build_eventually(rhs, time_frame))
        )
    )
);

// Parses a FACT of the form:
//   fact_name(arg+)
named!(fact<&str, Formula>,
    ws!(do_parse!(
        name: take_while!(|c| {
            let forbidden_fact_name_chars = ['(', ')', '<', '|', '[', ']', '@','.', '+', '?', '='];
            !forbidden_fact_name_chars.contains(&c)
        }) >>
        args: delimited!(
            char!('('),
            separated_list_complete!(char!(','), arg),
             char!(')')
        ) >>
        (build_fact_args(name, args))
    ))
);

// Parses a FACT of the form:
//   fact_name(arg+)
named!(empty_fact<&str, Formula>,
    ws!(do_parse!(
        name: take_while!(|c| {
            let forbidden_fact_name_chars = ['(', ')', '<', '|', '[', ']', '@','.', '+', '?', '='];
            !forbidden_fact_name_chars.contains(&c)
        }) >> args: tag!("()") >>
        (build_fact_args(name, vec![]))
    ))
);

named!(eos<&str, Formula>,
    ws!(do_parse!(
        tag!("<eos>") >>
        (build_eos())
    ))
);

named!(true_f<&str, Formula>,
    ws!(do_parse!(
        tag!("TRUE") >>
        (build_true())
    ))
);

named!(base_value<&str, Formula>,
    ws!(do_parse!(
        tag!("VALUE") >>
        (build_value())
    ))
);

named!(false_f<&str, Formula>,
    ws!(do_parse!(
        tag!("FALSE") >>
        (build_false())
    ))
);

/// Parses a NOT formula of the form:
///   ~ formula_l1
/// It parses a formula_l1, because of the precedence. The AND operator
/// has higher precedence and hence:
///   ~p && q = ((~p) && q)
named!(not<&str, Formula>,
    ws!(do_parse!(
        tag!("NOT") >>
        f: formula_l1 >>
        (build_not(f))
    ))
);

/// Parses a THERE EXISTS formula of the form:
///   'E' var '.' formula_l1
/// It parses a formula_l1, because it has lower precedence than the AND operator.
/// Hence:
///   Ex.p(4) && p(6) = (Ex.p(4)) && p(6)
///   Ex.~p(4) = Ex.(~p(4))
named!(exists<&str, Formula>,
    ws!(do_parse!(
        tag!("EXISTS") >>
        vars: separated_list_complete!(char!(','), alphanumeric) >>
        tag!(".") >>
        t: formula_l1 >>
        (build_exists(vars, t))
    ))
);

/// Parses a FOR ALL formula of the form:
///   'A' var '.' formula_l1
/// It parses a formula_l1, because it has lower precedence than the AND operator.
///   Ax.p(4) && p(6) = (Ax.p(4)) && p(6)
///   Ax.~p(4) = Ax.(~p(4))
named!(forall<&str, Formula>,
    ws!(do_parse!(
        tag!("FORALL") >>
        vars: separated_list_complete!(char!(','), alphanumeric) >>
        //var: alpha >>
        tag!(".") >>
        f: formula_l1 >>
        (build_forall(vars, f))
    ))
);

/// Bracketed formulas take precedence over all other operators.
named!(bracketted_formula<&str, Formula>,
    ws!(delimited!(
        tag!("("),
        formula,
        tag!(")")
    ))
);

/// Arguments to facts can be of the forms:
///  - digit, e.g. 5
///  - strings, e.g. 'hey'
///  - varibales, e.g. x
named!(arg<&str, Arg>,
    ws!(alt_complete!(
            string | number | variable
    ))
);

/// String argumenrs are of the form:
///   - 'hey'
named!(string<&str, Arg>,
    ws!(do_parse!(
        tag!("'") >>
        s: take_until_and_consume!("'")>>
        (Arg::Cst(Str(s.to_string())))
    ))
);

/// Numeric arguments are of the form:
///  - 5
named!(number<&str, Arg>,
    ws!(do_parse!(
        n: digit >>
        (Arg::Cst(Int(n.parse::<i32>().unwrap())))
    ))
);

/// Variable arguments are of the form:
///   - x
named!(variable<&str, Arg>,
    ws!(do_parse!(
        var: alphanumeric >>
        (Arg::Var(var.to_string()))
    ))
);

named!(interval<&str, TimeInterval>,
     ws!(alt_complete!(
            inf_interval | fin_interval
    ))
);

named!(fin_interval<&str, TimeInterval>,
    ws!(do_parse!(
       open_paran: take!(1) >>
       start: digit >>
       tag!(",") >>
       end: digit >>
       closing_paran: take!(1) >>
       (if open_paran == "[" && closing_paran == "]" {
           TimeInterval::new(TS::new(start.parse::<usize>().unwrap()), TS::new(end.parse::<usize>().unwrap()))
       } else if open_paran == "(" && closing_paran == ")" {
            TimeInterval::new(TS::new(start.parse::<usize>().unwrap()+1), TS::new(end.parse::<usize>().unwrap()-1))
       } else if open_paran == "[" && closing_paran == ")" {
            TimeInterval::new(TS::new(start.parse::<usize>().unwrap()), TS::new(end.parse::<usize>().unwrap()-1))
       } else { //if open_paran == "(" && closing_paran == "]" {
            TimeInterval::new(TS::new(start.parse::<usize>().unwrap()+1), TS::new(end.parse::<usize>().unwrap()))
       })
    ))
);

named!(inf_interval<&str, TimeInterval>,
    ws!(do_parse!(
       open_paran: take!(1) >>
       start: digit >>
       tag!(",") >>
        tag!("*)") >>
       (if open_paran == "[" {
           TimeInterval::new(TS::new(start.parse::<usize>().unwrap()), TS::INFINITY)
        } else {
            TimeInterval::new(TS::new(start.parse::<usize>().unwrap()+1), TS::INFINITY)
        })
    ))
);

#[cfg(test)]
mod tests {
    use constants::{default_start_time, test_formula, TEST_FACT};
    use parse_formula;
    use parser::formula_syntax_tree::Arg::{Cst, Var};

    use parser::formula_parser::{fin_interval, inf_interval, interval};
    use parser::formula_syntax_tree::Constant::{Int, Str};
    use parser::formula_syntax_tree::*;

    use timeunits::{TimeInterval, TS};

    #[test]
    fn inf_interval_test() {
        let x = "[0,*)";
        let (_, output) = inf_interval(x).unwrap();
        println!("[{},{}]", output.start, output.get_raw_end());

        let x1 = "[1000,*)";
        let (_, output) = inf_interval(x1).unwrap();
        println!("[{},{}]", output.start, output.get_raw_end());
    }

    #[test]
    fn fin_interval_test() {
        let x = "[0,1]";
        let (_, output) = fin_interval(x).unwrap();
        println!("[{},{}]", output.start, output.end);

        let x1 = "[1000,2000]";
        let (_, output) = fin_interval(x1).unwrap();
        println!("[{},{}]", output.start, output.end);
    }

    #[test]
    fn interval_test() {
        let x = "[0,*)";
        let (_, output) = interval(x).unwrap();
        println!("[{},{}]", output.start, output.is_infinite());

        let x1 = "[1000,2000]";
        let (_, output) = interval(x1).unwrap();
        println!("[{},{}]", output.start, output.end);

        let x1 = "(1000,2000)";
        let (_, output) = interval(x1).unwrap();
        println!("[{},{}]", output.start, output.end);

        let x1 = "[1000,2000)";
        let (_, output) = interval(x1).unwrap();
        println!("[{},{}]", output.start, output.end);

        let x1 = "(1000,2000]";
        let (_, output) = interval(x1).unwrap();
        println!("[{},{}]", output.start, output.end);
    }

    #[test]
    fn implication_basic() {
        let input = &format!("{s} IMPLIES {s}", s = TEST_FACT);
        let not_input = &format!("{s}", s = TEST_FACT);
        let output = parse_formula(input);
        let not_output = parse_formula(not_input);
        let expected_output = build_implication(test_formula(None), test_formula(None));
        assert_eq!(expected_output, output);
        assert_ne!(expected_output, not_output)
    }

    #[test]
    fn iff_basic() {
        let input = &format!("{s} EQUIV {s}", s = TEST_FACT);
        let output = parse_formula(input);
        let expected_output = build_iff(test_formula(None), test_formula(None));
        assert_eq!(expected_output, output);
    }

    #[test]
    fn fact_basic() {
        let output = parse_formula("p(5)");
        let expected_output = build_fact_int("p", vec![5]);
        assert_eq!(expected_output, output);
    }

    #[test]
    fn since_basic() {
        let input = &format!("{s} SINCE [0,*){s}", s = TEST_FACT);
        let output = parse_formula(input);
        let expected_output = build_since(
            test_formula(None),
            test_formula(None),
            TimeInterval::new(default_start_time(), TS::infinity()),
        );
        assert_eq!(expected_output, output);
    }

    #[test]
    fn neg_since_basic() {
        let input = &format!("(NOT {s}) SINCE [0,*){s}", s = TEST_FACT);
        let output = parse_formula(input);
        let expected_output = build_neg_since(
            test_formula(None),
            test_formula(None),
            TimeInterval::new(default_start_time(), TS::infinity()),
        );
        assert_eq!(expected_output, output);
    }

    #[test]
    fn until_basic() {
        let input = &format!("({s} AND {s}) UNTIL [0,*){s}", s = TEST_FACT);
        let output = parse_formula(input);
        let expected_output = build_until(
            build_conj(test_formula(None), test_formula(None)),
            test_formula(None),
            TimeInterval::new(default_start_time(), TS::infinity()),
        );
        print!("{}\n", output);
        print!("{}", expected_output);
        assert_eq!(expected_output, output);
    }

    #[test]
    fn once_basic() {
        let input = &format!("ONCE [0,1] {s}", s = TEST_FACT);
        let output = parse_formula(input);
        let expected_output = build_once(
            test_formula(None),
            TimeInterval::new(TS::new(0), TS::new(1)),
        );
        assert_eq!(expected_output, output);
    }

    #[test]
    fn variable_patterns() {
        let input = &format!("(ONCE [0,1] A(x,y) AND B(y,z)) AND NOT EVENTUALLY [0,1] C(z,x)");
        let output = parse_formula(input);
        let expected_output = build_anticonj(
            build_conj(
                build_once(
                    build_fact("A", vec!["x", "y"]),
                    TimeInterval::new(TS::new(0), TS::new(1)),
                ),
                build_fact("B", vec!["y", "z"]),
            ),
            build_eventually(
                build_fact("C", vec!["z", "x"]),
                TimeInterval::new(TS::new(0), TS::new(1)),
            ),
        );
        assert_eq!(expected_output, output);
    }

    #[test]
    fn next() {
        let input = &format!("NEXT [0,1] {s}", s = TEST_FACT);
        let output = parse_formula(input);
        let expected_output = build_next(
            test_formula(None),
            TimeInterval::new(TS::new(0), TS::new(1)),
        );
        assert_eq!(expected_output, output);
    }

    #[test]
    fn prev() {
        let input = &format!("PREVIOUS [0,1] {s}", s = TEST_FACT);
        let output = parse_formula(input);
        let expected_output = build_prev(
            test_formula(None),
            TimeInterval::new(TS::new(0), TS::new(1)),
        );
        assert_eq!(expected_output, output);
    }

    #[test]
    fn eventually_basic() {
        let input = &format!("EVENTUALLY [0,1] {s}", s = TEST_FACT);
        let output = parse_formula(input);
        let expected_output = build_eventually(
            test_formula(None),
            TimeInterval::new(TS::new(0), TS::new(1)),
        );
        assert_eq!(expected_output, output);
    }

    #[test]
    fn historically_basic() {
        let input = &format!("PAST_ALWAYS [0,1] {s}", s = TEST_FACT);
        let output = parse_formula(input);
        //let expected_output = build_historically( TEST_FORMULA!(), TimeInterval::new(TS::new(0), TS::new(1)));
        let expected_output = build_not(build_once(
            build_not(test_formula(None)),
            TimeInterval::new(TS::new(0), TS::new(1)),
        ));
        assert_eq!(expected_output, output);
    }

    #[test]
    fn always_basic() {
        let input = &format!("ALWAYS [0,1] {s}", s = TEST_FACT);
        let output = parse_formula(input);
        //let expected_output = build_historically( TEST_FORMULA!(), TimeInterval::new(TS::new(0), TS::new(1)));
        let expected_output = build_not(build_eventually(
            build_not(test_formula(None)),
            TimeInterval::new(TS::new(0), TS::new(1)),
        ));
        assert_eq!(expected_output, output);

        let input = &format!("ALWAYS [0,1) {s}", s = TEST_FACT);
        let output = parse_formula(input);
        //let expected_output = build_historically( TEST_FORMULA!(), TimeInterval::new(TS::new(0), TS::new(1)));
        let expected_output = build_not(build_eventually(
            build_not(test_formula(None)),
            TimeInterval::new(TS::new(0), TS::new(0)),
        ));
        assert_eq!(expected_output, output);
    }

    #[test]
    fn arg_string() {
        let output = parse_formula("p('foo')");
        let expected = build_fact("p", vec!["'foo'"]);

        assert_eq!(expected, output);
    }

    #[test]
    fn fact_multiple_arguments_integers() {
        let output = parse_formula("p(4, 3, 2, 1)");
        let expected_output = build_fact_int("p", vec![4, 3, 2, 1]);
        assert_eq!(expected_output, output);
    }

    #[test]
    fn fact_basic_string() {
        let output = parse_formula(TEST_FACT);
        let expected_output = test_formula(None);
        assert_eq!(expected_output, output);
    }

    #[test]
    fn fact_multiple_arguments_strings() {
        let output = parse_formula("p('foo', 'bar')");
        let expected_output = build_fact("p", vec!["'foo'", "'bar'"]);
        assert_eq!(expected_output, output);
    }

    #[test]
    fn fact_multiple_arguments_mixed_strings_and_integers() {
        let output = parse_formula("p('foo', 1, 'bar', 'foobar', 2, x)");
        let expected_output = build_fact_args(
            "p",
            vec![
                Cst(Str("foo".into())),
                Cst(Int(1)),
                Cst(Str("bar".into())),
                Cst(Str("foobar".into())),
                Cst(Int(2)),
                Var("x".into()),
            ],
        );

        assert_eq!(expected_output, output);
    }

    #[test]
    fn and_basic() {
        let output = parse_formula(&format!("{s} AND {s}", s = TEST_FACT));
        let expected_output = build_conj(test_formula(None), test_formula(None));
        assert_eq!(expected_output, output);
    }

    #[test]
    fn and_multiple() {
        let input = &format!("{s} AND {s} AND {s}", s = TEST_FACT);
        let output = parse_formula(input);
        let expected_output = build_conj(
            test_formula(None),
            build_conj(test_formula(None), test_formula(None)),
        );

        assert_eq!(expected_output, output);
    }

    #[test]
    fn disjunction_simple() {
        let input = &format!("{s} OR {s}", s = TEST_FACT);
        let output = parse_formula(input);
        let expected_output = build_disj(test_formula(None), test_formula(None));

        assert_eq!(expected_output, output);
    }

    #[test]
    fn disjunction_and_precedence() {
        let input = &format!("{s} OR {s} AND {s} OR {s}", s = TEST_FACT);
        let output = parse_formula(input);
        let expected_output = build_conj(
            build_disj(test_formula(None), test_formula(None)),
            build_disj(test_formula(None), test_formula(None)),
        );

        assert_eq!(expected_output, output);
    }

    #[test]
    fn disjunction_exists() {
        let input = &format!("EXISTSx.{s} OR {s}", s = TEST_FACT);
        let output = parse_formula(input);
        let expected_output = build_disj(
            build_exists(vec!["x"], test_formula(None)),
            test_formula(None),
        );

        assert_eq!(expected_output, output);
    }

    #[test]
    fn not_basic() {
        let input = &format!("NOT {}", TEST_FACT);

        let output = parse_formula(input);
        let expected_output = build_not(test_formula(None));
        assert_eq!(expected_output, output);
    }

    #[test]
    fn not_double() {
        let input = &format!("NOT NOT {}", TEST_FACT);

        let output = parse_formula(input);
        let expected_output = build_not(build_not(test_formula(None)));

        assert_eq!(expected_output, output);
    }

    #[test]
    fn not_and_combination_left() {
        let input = &format!("NOT {s} AND {s}", s = TEST_FACT);

        let output = parse_formula(input);
        let expected_output = build_conj(build_not(test_formula(None)), test_formula(None));

        assert_eq!(expected_output, output);
    }

    #[test]
    fn not_and_combination_right() {
        let input = &format!("{s} AND NOT {s}", s = TEST_FACT);

        let output = parse_formula(input);
        let expected_output = build_conj(test_formula(None), build_not(test_formula(None)));
        assert_eq!(expected_output, output);
    }

    #[test]
    fn not_and_combination_left_right() {
        let input = &format!("NOT {s} AND NOT {s}", s = TEST_FACT);

        let output = parse_formula(input);
        let expected_output =
            build_conj(build_not(test_formula(None)), build_not(test_formula(None)));

        assert_eq!(expected_output, output);
    }

    #[test]
    fn not_and_multiple_combination_left_right() {
        let input = &format!("NOT {s} AND {s} AND NOT {s}", s = TEST_FACT);

        let output = parse_formula(input);
        let expected_output = build_conj(
            build_not(test_formula(None)),
            build_conj(test_formula(None), build_not(test_formula(None))),
        );
        assert_eq!(expected_output, output);
    }

    #[test]
    fn exists_basic() {
        let input = &format!("EXISTSx.{}", TEST_FACT);

        let output = parse_formula(input);
        let expected_output = build_exists(vec!["x"], test_formula(None));

        assert_eq!(expected_output, output);
    }

    #[test]
    fn exists_not_basic() {
        let input = &format!("EXISTSx.(NOT {})", TEST_FACT);

        let output = parse_formula(input);
        let expected_output = build_exists(vec!["x"], build_not(test_formula(None)));

        assert_eq!(expected_output, output);
    }

    #[test]
    fn exists_and_basic() {
        let input = &format!("EXISTSx.{s} AND {s}", s = TEST_FACT);

        let output = parse_formula(input);
        let expected_output = build_conj(
            build_exists(vec!["x"], test_formula(None)),
            test_formula(None),
        );
        assert_eq!(expected_output, output);
    }

    #[test]
    fn exists_nested_and() {
        let input = &format!("EXISTSx.({s} AND {s})", s = TEST_FACT);

        let output = parse_formula(input);
        let expected_output = build_exists(
            vec!["x"],
            build_conj(test_formula(None), test_formula(None)),
        );
        assert_eq!(expected_output, output);
    }

    #[test]
    fn exists_and_not() {
        let input = &format!("EXISTSx . NOT {s} AND NOT {s})", s = TEST_FACT);

        let output = parse_formula(input);
        let expected_output = build_conj(
            build_exists(vec!["x"], build_not(test_formula(None))),
            build_not(test_formula(None)),
        );
        assert_eq!(expected_output, output);
    }

    #[test]
    fn and_exists_not() {
        let input = &format!("{s} AND EXISTS x.{s}", s = TEST_FACT);

        let output = parse_formula(input);
        let expected_output = build_conj(
            test_formula(None),
            build_exists(vec!["x"], test_formula(None)),
        );
        assert_eq!(expected_output, output);
    }

    #[test]
    fn and_not_exists() {
        let input = &format!("{s} AND NOT EXISTS x.{s}", s = TEST_FACT);

        let output = parse_formula(input);
        let expected_output = build_conj(
            test_formula(None),
            build_not(build_exists(vec!["x"], test_formula(None))),
        );

        assert_eq!(expected_output, output);
    }

    #[test]
    fn forall_basic() {
        let input = &format!("FORALLx.{}", TEST_FACT);

        let output = parse_formula(input);
        let expected_output = build_forall(vec!["x"], test_formula(None));

        assert_eq!(expected_output, output);
    }

    #[test]
    fn brackets_test() {
        let input = &format!(
            "NOT ({s} AND {s}) AND EXISTSx.({s} AND NOT {s} AND EXISTSy.{s})",
            s = TEST_FACT
        );

        let output = parse_formula(input);
        let expected_output = build_conj(
            build_not(build_conj(test_formula(None), test_formula(None))),
            build_exists(
                vec!["x"],
                build_conj(
                    test_formula(None),
                    build_conj(
                        build_not(test_formula(None)),
                        build_exists(vec!["y"], test_formula(None)),
                    ),
                ),
            ),
        );

        assert_eq!(expected_output, output);
    }

    #[test]
    fn equals() {
        let variable = "x";
        let input = &format!("p({v}) AND {v}=5", v = variable);

        let expected = build_conj(
            build_fact("p", vec![variable.clone()]),
            build_equals(variable, Cst(Int(5))),
        );
        let actual = parse_formula(input);
        assert_eq!(expected, actual);
    }

    #[test]
    fn equals_var_var() {
        let variable1 = "x";
        let variable2 = "y";
        let input = &format!("p({v},{w}) AND {v}={w}", v = variable1, w = variable2);

        let expected = build_conj(
            build_fact("p", vec![variable1.clone(), variable2.clone()]),
            build_equals(variable1, Var(variable2.to_string())),
        );
        let actual = parse_formula(input);
        assert_eq!(expected, actual);
    }

    #[test]
    fn prev_inf() {
        let variable1 = "x";
        let input = &format!("PREVIOUS [0,*) p({variable1})");

        let expected = build_prev(
            build_fact("p", vec![variable1.clone()]),
            TimeInterval::new(TS::FINITE(0), TS::INFINITY),
        );
        let actual = parse_formula(input);
        assert_eq!(expected, actual);
    }

    #[test]
    fn equals_conj() {
        let variable = "x";
        let input = &format!("p({v}) AND {v} = 5", v = variable);

        let expected = build_conj(
            build_fact("p", vec![variable]),
            build_equals(variable, Cst(Int(5))),
        );
        let actual = parse_formula(input);
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_filter() {
        let x1 = "x1";
        let x2 = "x2";
        let input = &format!("(P({}) AND P({})) AND {}={}", x1, x2, x1, x2);

        let expected = build_conj(
            build_conj(build_fact("P", vec![x1]), build_fact("P", vec![x2])),
            build_equals(x1, Var(x2.to_string())),
        );
        let actual = parse_formula(input);
        assert_eq!(expected, actual);
    }

    #[test]
    fn empty_fact() {
        let input = &format!("p()");

        let expected = build_fact("p", vec![]);
        let actual = parse_formula(input);
        assert_eq!(expected, actual);
    }

    #[test]
    fn eos() {
        let input = &format!("<eos>");

        let expected = build_eos();
        let actual = parse_formula(input);
        assert_eq!(expected, actual);
    }

    #[test]
    fn randy1() {
        let input =
            &format!("(ONCE(0,177) ((NOT (ONCE[0,312] (P0() AND P1()))) SINCE[0,301) P2(x2,x1)))");

        let expected = build_once(
            build_neg_since(
                build_once(
                    build_conj(build_fact("P0", vec![]), build_fact("P1", vec![])),
                    TimeInterval::new(TS::new(0), TS::new(312)),
                ),
                build_fact("P2", vec!["x2", "x1"]),
                TimeInterval::new(TS::new(0), TS::new(300)),
            ),
            TimeInterval::new(TS::new(1), TS::new(176)),
        );
        let actual = parse_formula(input);
        assert_eq!(expected, actual);
    }

    #[test]
    fn rand_4() {
        for f in vec![
            &format!("(ONCE(0,177) ((NOT (ONCE[0,312] (P0() AND P1()))) SINCE[0,301) P2(x2,x1)))"),
            &format!("((P0(x2,x3) AND (x3 = 21)) SINCE(0,211) (P1(x3) SINCE(0,*) (P2(x1,x3,x2) SINCE[0,*) P3(x3,x1,x2,x4))))"),
            &format!("((P0(x6,x3,x1) SINCE(0,381] P1(x3,x1,x5,x6,x2,x4)) OR (((7 = 35) AND P2(x5,x3)) AND P1(x4,x1,x5,x6,x3,x2)))"),
            &format!("(((NOT (ONCE(0,*) (ONCE(0,*) P0(x8,x4,x5,x6)))) UNTIL(0,279) P1(x4,x7,x6,x3,x2,x8,x5,x1)) AND (x2 = 40))"),
            &format!("((P0(x6,x10,x7,x4,x3,x2) AND P1(x8,x2,x6,x1,x3,x5,x9,x10,x4,x7)) AND (NOT ((P2(x5,x2,x10) AND (NOT P3(x2,x10,x5))) AND (NOT P4(x5,x10)))))")
        ] {
            let actual = parse_formula(f);
            println!("{}", actual);
        }
    }

    #[test]
    fn rand_10() {
        for f in vec![
            &format!("((((27 = 36) AND (x1 = 16)) AND (NEXT(0,26) ((NOT P0()) SINCE(0,*) ((NOT (5 = 42)) SINCE(0,446] P1())))) AND ((NOT ((NOT P2()) SINCE(0,385) ((38 = 30) AND (NOT P2())))) SINCE(0,*) (EVENTUALLY[0,344) P3(x2,x1))))"),
            &format!("(ONCE[0,*) (NEXT[0,78] ((ONCE[0,*) ((((NOT (29 = 29)) SINCE[0,*) (22 = 37)) SINCE(0,33) P0(x3)) AND P1(x3))) SINCE(0,*) (EVENTUALLY(0,288) ((NOT (PREVIOUS(0,*) P2(x3,x1,x4,x2))) UNTIL(0,139) P2(x1,x2,x3,x4))))))"),
            &format!("((NEXT[0,211] ((NOT P0(x5,x1)) SINCE(0,399] ((NOT (PREVIOUS(0,266) ((NOT (x2 = 19)) SINCE(0,295) P1(x5,x2)))) SINCE(0,*) P2(x3,x1,x6,x5,x4,x2)))) AND (EVENTUALLY[0,65] (NEXT[0,275] (NEXT[0,46) ((NOT P3()) SINCE[0,*) P4(x2,x5,x4))))))"),

            &format!("(((NOT (P0() SINCE[0,*) P1(x3,x2,x7,x6))) UNTIL(0,84] (EVENTUALLY[0,305) (P2(x1,x6,x5,x8,x7,x4,x3) SINCE(0,*) (P3(x1,x3,x8,x7,x6,x4,x2,x5) AND (x8 = 40))))) AND (NOT (((EXISTS y1. P1(x4,y1,x7,x2)) AND (NEXT(0,363] P1(x5,x2,x4,x7))) AND P4(x4,x3,x7))))"),
            &format!("(EVENTUALLY(0,25] ((P0(x10,x3,x6,x4) AND (P1(x6,x3,x10) AND P2())) SINCE(0,*) (ONCE(0,*) ((ONCE(0,*) (EVENTUALLY[0,103) (NEXT(0,140) P3(x3,x5,x9,x6,x1)))) AND (EXISTS y1. P4(x7,x8,y1,x2,x4,x10,x9,x6,x5))))))"),
            //&format!("EXISTS y1. P4(x7,x8,y1,x2,x4,x10,x9,x6,x5))))))")
        ] {
            let actual = parse_formula(f);
            println!("{}", actual);
        }
    }

    #[test]
    fn rand_20() {
        for f in vec![
            &format!("(((ONCE(0,*) ((PREVIOUS(0,*) (ONCE[0,*) P0())) UNTIL[0,63) P1(x1,x2))) AND (x2 = 22)) AND (ONCE(0,*) (ONCE[0,*) (((ONCE[0,*) (ONCE[0,*) (EXISTS y1. (P2(y1,x2) AND (NOT (EVENTUALLY(0,213) P3(y1))))))) AND ((ONCE[0,346) ((NOT (NEXT[0,274] (P4() SINCE(0,334] (5 = 4)))) SINCE[0,459] P5(x2))) AND (x2 = 8))) AND (x2 = 18)))))"),
            &format!("(EVENTUALLY[0,343) (ONCE[0,240) (ONCE[0,255] ((NOT (EXISTS y1. (EVENTUALLY[0,372) P0(x3,y1,x4,x1)))) SINCE[0,*) (((ONCE(0,*) (((((NOT P1()) UNTIL(0,153) (42 = 31)) SINCE(0,*) P2(x2,x1)) AND (NOT P3(x2,x1))) AND (NOT (x1 = 7)))) AND (NOT P4(x1,x2))) AND ((NOT (x3 = 14)) SINCE[0,*) (((NOT (((P4(x1,x4) AND (NOT P2(x1,x4))) AND P5(x4,x3,x1)) AND (NOT (P5(x4,x1,x3) AND (NOT P5(x3,x1,x4)))))) SINCE(0,194) P6(x3,x4,x2,x1)) OR P0(x1,x4,x2,x3))))))))"),
            &format!("(((EVENTUALLY(0,339) P0(x1,x2,x6,x3)) OR ((P1(x2,x1,x3) UNTIL[0,7] (P2(x1,x6,x2,x3) AND (NOT P2(x2,x1,x6,x3)))) AND (NOT (x1 = 35)))) UNTIL(0,214] (EXISTS y1. (((((P3(y1,x3) AND P3(y1,x3)) AND P4(y1,x3)) AND (NOT (EVENTUALLY(0,446] (P4(y1,x3) AND P5(y1))))) SINCE[0,*) ((NOT (EXISTS y2. (ONCE(0,142) (P6(y1,x4,y2,x2) AND (NOT P2(y2,x2,x4,y1)))))) SINCE[0,195] P0(x4,x2,x3,y1))) SINCE(0,*) (P7(x4,x5,y1,x2,x1,x6,x3) AND (NOT ((NOT P8(x6,y1,x5,x2,x3,x1)) SINCE(0,*) P7(y1,x3,x4,x6,x5,x2,x1)))))))"),
            &format!("((PREVIOUS(0,354) (P0(x5,x7,x1,x2) AND ((EXISTS y1. (P1(x3,y1,x7,x4,x5,x8) OR P1(x8,x7,x5,y1,x3,x4))) SINCE(0,*) P2(x7,x4,x3,x5,x6,x8,x1)))) AND (NOT (((ONCE(0,*) (NEXT(0,150] (ONCE[0,*) (EVENTUALLY(0,237) ((P3(x7) AND ((NOT (20 = 40)) UNTIL[0,375] P4())) AND (x7 = 0)))))) AND (P3(x7) AND (EVENTUALLY[0,149) (P4() SINCE[0,*) P5(x7))))) SINCE[0,*) ((NOT (P6(x4,x6) OR P7(x6,x4))) SINCE(0,*) P8(x4,x6,x7,x5)))))"),
            &format!("((NOT ((P0(x10,x4,x8) OR P1(x10,x8,x4)) OR (ONCE(0,*) (P2(x8,x10,x4) AND (NOT (P3(x10,x4) AND P3(x10,x8))))))) SINCE[0,*) ((ONCE(0,*) (EXISTS y1. (ONCE[0,*) (EVENTUALLY(0,10) ((ONCE[0,*) P4(x4,x3,y1,x2,x8)) AND (NOT (ONCE[0,*) ((P4(y1,x3,x4,x8,x2) AND (x3 = 37)) AND (NOT (NEXT[0,358) (EVENTUALLY(0,27) (P5(y1) AND P5(y1))))))))))))) SINCE[0,*) (P6(x8,x3,x2,x1,x5,x6,x9,x7,x10,x4) AND P6(x5,x7,x2,x3,x9,x8,x6,x1,x4,x10))))"),
        ] {
            let actual = parse_formula(f);
            println!("{}", actual);
        }
    }

    #[test]
    fn rand_30() {
        for f in vec![
            &format!("(NEXT[0,433) ((EVENTUALLY[0,66] (NEXT[0,417] (EVENTUALLY(0,47) ((EVENTUALLY[0,256) (P0() AND ((EVENTUALLY[0,403] P0()) AND (NOT ((NOT (20 = 10)) UNTIL(0,286) (19 = 25)))))) SINCE[0,*) ((NOT ((NOT ((NOT (P0() SINCE(0,*) (20 = 6))) UNTIL[0,255) P1())) UNTIL[0,36] (29 = 31))) SINCE[0,*) (((NEXT(0,148] (P2(x2,x1) OR ((NOT P3(x1,x2)) UNTIL[0,387] P4(x2,x1)))) OR P5(x2,x1)) OR ((EVENTUALLY[0,46) (ONCE[0,*) (PREVIOUS(0,192] P3(x2,x1)))) AND (NEXT[0,263] (ONCE(0,*) (PREVIOUS[0,475) (P2(x1,x2) OR (NEXT[0,466] P5(x1,x2))))))))))))) AND ((8 = 18) AND P4(x2,x1))))"),
            &format!("((NOT (NEXT(0,434] (((NOT (42 = 23)) UNTIL[0,312] (ONCE(0,*) (ONCE[0,*) (EVENTUALLY[0,23) ((NOT (ONCE(0,*) ((EXISTS y1. (((NOT ((EVENTUALLY[0,25) P0(y1)) AND P1(y1))) SINCE(0,*) P0(y1)) AND ((NOT P2(y1)) UNTIL[0,178) (y1 = 6)))) AND ((21 = 3) SINCE[0,*) (PREVIOUS[0,*) ((PREVIOUS(0,40) P3()) AND (NOT (15 = 30)))))))) SINCE[0,408] ((EVENTUALLY(0,478] P4()) AND (P3() AND (NOT (15 = 32))))))))) AND (ONCE[0,133] (ONCE[0,425] (NEXT(0,73] P4())))))) SINCE[0,94] (NEXT[0,292) (P5(x1,x3,x4,x2) OR (P6(x4,x3,x1) SINCE[0,*) (P7(x1,x3) SINCE[0,*) P8(x4,x3,x1,x2))))))"),
            &format!("((EVENTUALLY[0,192] ((EVENTUALLY(0,469] (((NEXT[0,164) ((x1 = 23) AND (PREVIOUS(0,*) ((EVENTUALLY(0,497] (x2 = 39)) OR (x2 = 18))))) UNTIL(0,430) P0(x6,x2,x1,x4)) OR ((ONCE[0,*) (P1(x4,x2,x6,x1) AND (NOT ((NOT (x1 = 7)) UNTIL(0,295] P2(x1,x6))))) AND (NOT ((((NEXT[0,298] P3()) AND (ONCE(0,*) ((17 = 13) UNTIL[0,469] ((P4(x4) UNTIL[0,221] (x4 = 21)) AND (x4 = 29))))) AND (x4 = 7)) AND (x4 = 11)))))) AND (ONCE(0,*) (((x1 = 25) AND (NOT P3())) AND P5(x1,x6))))) AND ((P2(x6,x5) AND (EXISTS y1. P6(x1,x2,y1,x3,x4))) AND (NOT (EXISTS y1. P7(x2,y1,x1,x4,x3)))))"),
            &format!("(EXISTS y1. (((ONCE[0,*) (ONCE[0,*) (((ONCE(0,47) P0(x8,x2,y1)) UNTIL[0,398] (ONCE[0,*) (ONCE[0,*) ((PREVIOUS[0,281] ((NOT P1(x1,y1,x2)) UNTIL[0,4] ((P1(x2,y1,x1) AND (NOT P1(x1,y1,x2))) AND P2(x2,x1,x8,y1)))) AND ((EVENTUALLY(0,255) (P3(y1,x1) AND P4(x1,y1))) SINCE[0,*) (NEXT[0,280] ((ONCE(0,*) (P5(y1,x8) AND (y1 = 1))) UNTIL[0,353) (NEXT(0,361) P6(y1,x8,x2,x1))))))))) AND ((NOT P6(x1,y1,x5,x2)) UNTIL[0,167] ((ONCE[0,325) P7(x4,x2,x1,x5,y1)) AND (NOT P7(x5,x1,y1,x4,x2))))))) AND (NOT (P1(x1,x5,y1) AND (NOT (P8(y1,x5,x1) SINCE[0,377] ((NOT P0(y1,x1,x5)) UNTIL(0,413] P8(x1,y1,x5))))))) SINCE(0,*) (ONCE(0,375] P9(y1,x3,x5,x7,x6,x2,x4,x8,x1))))"),
            &format!("((NOT (EVENTUALLY[0,400) ((P0(x6) AND (PREVIOUS[0,360) (((NOT P1(x7,x6,x9)) UNTIL(0,164] (PREVIOUS[0,294] ((P2(x7,x9) SINCE[0,*) P3(x9,x8,x6,x7)) SINCE[0,*) ((NOT P1(x6,x4,x3)) SINCE(0,369) P4(x6,x9,x8,x3,x7,x4))))) OR ((ONCE(0,396) (P4(x4,x9,x6,x3,x8,x7) AND (ONCE(0,*) P4(x8,x7,x9,x6,x3,x4)))) OR ((EVENTUALLY(0,405] (P5(x7,x9,x3) AND (NOT ((NOT P6(x7,x3)) SINCE(0,197) P1(x7,x9,x3))))) SINCE(0,*) (NEXT[0,330] (ONCE[0,*) ((NOT ((NOT (1 = 30)) UNTIL[0,53) P7(x4,x6,x7))) SINCE(0,*) P4(x8,x3,x7,x4,x6,x9))))))))) AND ((P2(x2,x5) UNTIL(0,322) P8(x8,x5,x10,x6,x2)) SINCE[0,*) (ONCE[0,*) P9(x3,x8,x10,x5,x2,x6)))))) UNTIL[0,73) (P10(x5,x2,x10,x4,x6,x3,x9,x8) UNTIL[0,209) ((P11(x4,x6,x1,x9,x3,x10,x8,x7,x2,x5) AND (NOT (ONCE(0,68] P12(x9)))) AND (x3 = 0))))"),
        ] {
            let actual = parse_formula(f);
            println!("{}", actual);
        }
    }

    #[test]
    fn rand_40() {
        for f in vec![
            &format!("(PREVIOUS[0,*) (ONCE[0,64] ((NOT (((ONCE(0,*) (((NOT (19 = 19)) SINCE[0,*) (EVENTUALLY[0,178] ((P0() AND (NOT P1())) AND ((ONCE[0,*) (4 = 23)) SINCE[0,303) P2())))) UNTIL(0,140) (NEXT(0,304) (PREVIOUS[0,24] P1())))) AND (NOT (((NOT (EVENTUALLY(0,267] P0())) SINCE(0,*) ((34 = 33) UNTIL[0,375) (EVENTUALLY[0,383) (ONCE(0,*) P0())))) AND (NOT ((3 = 18) SINCE(0,*) (EXISTS y1. (P3(y1) SINCE[0,*) P4(y1)))))))) SINCE[0,*) (ONCE[0,*) (((NOT P3(x1)) SINCE[0,*) P5(x1)) AND (NOT (x1 = 24)))))) SINCE(0,*) (NEXT[0,184] ((EVENTUALLY[0,322] ((EVENTUALLY[0,282) (EXISTS y1. P6(x1,x2,y1))) AND P5(x1))) OR ((NOT ((NOT ((NOT P0()) SINCE(0,69) P2())) UNTIL(0,470) (ONCE[0,11] (EVENTUALLY(0,128] (26 = 41))))) UNTIL(0,60] (P7(x2,x1) AND (NOT ((x2 = 6) SINCE(0,*) (x2 = 24))))))))))"),
            &format!("((EXISTS y1. ((NOT P0(x2,y1)) UNTIL[0,430] (((NOT (P0(x1,y1) SINCE[0,*) P1(x1,y1,x3))) SINCE(0,231) P2(x3,x4,x1,y1,x2)) AND (NOT P2(x3,y1,x1,x2,x4))))) OR ((P3(x1,x3,x2,x4) AND (NEXT[0,156) (EVENTUALLY(0,347) (ONCE(0,*) ((ONCE[0,244) (EVENTUALLY(0,451) (ONCE(0,247] P4(x4,x3)))) SINCE(0,*) (((NOT P4(x4,x3)) UNTIL(0,215] (P3(x4,x2,x1,x3) AND (P1(x2,x1,x4) AND (P4(x3,x4) SINCE[0,*) P5(x4,x3,x1))))) AND (x4 = 39))))))) OR ((NOT (ONCE(0,*) (EVENTUALLY(0,189) (EVENTUALLY[0,488] ((NOT ((x1 = 30) AND ((P6() AND P7(x2)) OR ((EXISTS y1. (EVENTUALLY(0,72] (P7(y1) AND (y1 = 10)))) UNTIL[0,63) (EVENTUALLY[0,388) (x2 = 4)))))) UNTIL(0,98] ((x4 = 24) SINCE(0,*) P8(x2,x1,x4))))))) SINCE[0,*) ((P0(x3,x1) OR (P9(x1) AND P4(x3,x1))) SINCE(0,*) (ONCE[0,*) ((P9(x2) AND (x2 = 1)) SINCE[0,*) P3(x3,x4,x2,x1)))))))"),
            &format!("((((NOT (15 = 25)) UNTIL(0,444] P0(x3,x2,x6,x1)) SINCE[0,*) ((P0(x5,x4,x3,x1) SINCE(0,*) (EXISTS y1. P1(x4,x6,x1,y1,x3,x5,x2))) AND (NOT P2(x3,x4,x2,x6,x5,x1)))) OR (((NOT ((((P3(x4,x5,x6,x2) AND (P4(x2,x5) AND (P5(x5) SINCE[0,*) (x5 = 34)))) AND ((P3(x2,x4,x6,x5) AND P6(x4,x6)) OR (P6(x5,x4) SINCE(0,145) P0(x2,x5,x4,x6)))) AND (x4 = 12)) AND (x4 = 13))) SINCE[0,*) (ONCE[0,218) P7(x1,x4,x3,x5,x2,x6))) OR ((((NOT ((NOT P8(x6)) SINCE[0,*) P5(x6))) SINCE(0,249) P9(x4,x6,x2)) AND (ONCE(0,*) (P10(x6,x2,x1) SINCE(0,*) (ONCE(0,*) (EXISTS y1. (ONCE[0,*) ((P11(x4,x6,x2,x3,y1,x1) AND P12(y1,x3,x1)) AND (NOT ((P6(y1,x3) AND P13(x3,y1)) SINCE[0,*) ((EVENTUALLY[0,289] (P14(y1) SINCE(0,*) (ONCE[0,*) P4(x3,y1)))) AND (x3 = 13))))))))))) UNTIL(0,276) (EXISTS y1. (((NOT (P10(x6,x4,y1) SINCE[0,*) P9(x6,y1,x4))) UNTIL(0,190) (ONCE[0,*) P15(x4,y1,x3,x5,x6))) SINCE[0,*) P1(x5,x1,x6,y1,x2,x3,x4))))))"),
            &format!("(EXISTS y1. ((P0(y1,x4,x8,x1,x2,x5,x7,x6,x3) AND (((P1(y1,x5,x4,x2) AND P2(x4,x5,x2,y1)) AND (x2 = 22)) AND (NOT (NEXT(0,156] ((y1 = 14) SINCE[0,106) ((NOT ((ONCE(0,*) (P3(x5,y1,x2) AND (NOT P4(y1)))) AND (NOT (PREVIOUS(0,237) (EXISTS y2. P5(y2,x5,y1,x2)))))) SINCE(0,*) (ONCE(0,288] (P6(y1,x2) SINCE[0,*) ((NOT (((y1 = 29) AND (P4(y1) SINCE(0,*) (y1 = 34))) UNTIL[0,232] P6(y1,x5))) UNTIL(0,473) ((NOT P7(x4,y1)) SINCE[0,*) (P5(y1,x4,x5,x2) AND (NOT P2(x4,x2,y1,x5))))))))))))) AND (NEXT(0,38) ((P8(y1,x8,x1,x5,x3) SINCE[0,*) P9(x2,x6,x1,y1,x3,x7,x5,x4,x8)) AND (NOT (ONCE(0,*) ((((NOT (((P3(x1,x5,y1) OR (ONCE[0,*) ((NOT P10(y1)) UNTIL(0,442] P3(x5,y1,x1)))) OR (EVENTUALLY(0,331] ((ONCE(0,227] ((NOT P11(y1)) UNTIL[0,494) (y1 = 15))) SINCE[0,*) P12(y1,x5,x1)))) AND (NOT ((y1 = 27) SINCE(0,*) P4(y1))))) SINCE[0,*) P2(y1,x1,x5,x7)) AND (x7 = 2)) AND (EVENTUALLY(0,423) P8(y1,x7,x1,x8,x5)))))))))"),
            &format!("(ONCE(0,488] ((ONCE(0,*) ((NOT (EXISTS y1. (EXISTS y2. ((NOT P0(y2,y1)) SINCE(0,61] P1(y1,x7,y2))))) SINCE[0,489) (NEXT(0,360] ((((x7 = 40) AND (P0(x7,x1) AND (EXISTS y1. (P1(y1,x1,x7) AND P1(x1,x7,y1))))) AND (NOT ((NOT ((((NOT (P2() AND (NOT (ONCE(0,361] P3())))) UNTIL(0,433] (PREVIOUS[0,*) P3())) SINCE(0,*) (EVENTUALLY(0,429] (x1 = 38))) AND (x1 = 42))) UNTIL(0,65] (x1 = 16)))) AND (x1 = 7))))) SINCE(0,*) (EVENTUALLY[0,459] ((PREVIOUS(0,*) (ONCE[0,*) ((NOT P1(x2,x6,x5)) UNTIL(0,214] ((EVENTUALLY[0,418) ((P4(x4,x7,x9,x3,x10,x2) AND P5(x9,x5,x6,x8,x1,x3,x10,x7,x4)) AND (EVENTUALLY[0,424] P6(x3,x10,x2,x1,x6,x5,x9,x8,x4,x7)))) OR ((NOT P7(x2,x5,x8,x7,x1,x9)) SINCE[0,*) P6(x3,x7,x9,x8,x4,x5,x10,x6,x1,x2)))))) AND (NOT (((x8 = 10) AND (NEXT[0,423] ((NOT (P8(x8) OR P9(x8))) SINCE[0,*) (EVENTUALLY[0,333] P4(x9,x3,x6,x10,x8,x5))))) UNTIL[0,396) (ONCE[0,*) P10(x1,x10,x2,x3,x8,x6,x4,x5,x9,x7))))))))"),
        ] {
            let actual = parse_formula(f);
            println!("{}", actual);
        }
    }

    #[test]
    fn rand_50() {
        for f in vec![
            &format!("((NEXT[0,7] ((NOT ((NOT (((EVENTUALLY(0,4) (16 = 24)) OR (30 = 1)) AND (NOT (NEXT[0,4] (((EXISTS y1. (y1 = 8)) AND P0()) AND (NOT (EVENTUALLY[0,6) (7 = 34)))))))) UNTIL(0,4) (((ONCE(0,1] (23 = 10)) SINCE(0,*) P0()) SINCE(0,*) P0()))) UNTIL(0,3) ((ONCE(0,*) (((NOT P1(x1,x2)) UNTIL(0,4] (EVENTUALLY[0,8) (EXISTS y1. (EVENTUALLY[0,1) (NEXT(0,7) ((y1 = 21) SINCE[0,*) (ONCE(0,*) P2(x2,x1,y1)))))))) AND (NOT (EXISTS y1. ((NOT P3(x2,y1)) SINCE[0,*) P3(x2,y1)))))) AND ((EVENTUALLY(0,9) (EVENTUALLY[0,5) ((25 = 41) AND (((NOT (EVENTUALLY[0,0] (5 = 36))) UNTIL[0,5) (6 = 7)) UNTIL[0,2) (1 = 5))))) SINCE[0,*) (((NOT (35 = 19)) SINCE[0,*) (13 = 32)) AND (8 = 41)))))) AND (((NOT ((P0() AND (NOT ((NOT P0()) SINCE[0,*) (7 = 38)))) AND (NOT (NEXT[0,4) (41 = 17))))) SINCE[0,*) (P4(x1) AND (NOT (PREVIOUS(0,*) (x1 = 24))))) AND ((P1(x2,x1) OR P5(x1,x2)) OR (ONCE(0,*) (EVENTUALLY[0,0] (ONCE(0,*) (P5(x1,x2) AND (x2 = 4))))))))"),
            &format!("((NEXT[0,9] ((EXISTS y1. ((EVENTUALLY(0,3) ((NOT P0(y1)) SINCE(0,*) ((NOT (((EVENTUALLY[0,4] P1(y1)) AND (y1 = 6)) AND (NOT P1(y1)))) SINCE[0,*) ((NOT (y1 = 0)) UNTIL[0,7] (ONCE(0,2] P1(y1)))))) AND (EXISTS y2. ((NOT P2(y1,y2)) UNTIL(0,9] P3(y2,y1))))) AND ((EVENTUALLY[0,5] ((P4() OR P5()) AND (EVENTUALLY[0,0] (EXISTS y1. (PREVIOUS[0,*) ((NOT ((NOT P6(y1)) SINCE(0,*) (y1 = 37))) UNTIL[0,8] (y1 = 11))))))) SINCE(0,*) (EVENTUALLY[0,1] (ONCE(0,6) (EVENTUALLY(0,7) ((NOT (((30 = 14) AND (ONCE(0,*) (39 = 28))) SINCE[0,*) (((EVENTUALLY[0,0] (29 = 9)) AND P5()) AND (18 = 27)))) SINCE[0,*) ((PREVIOUS[0,*) (6 = 24)) UNTIL(0,3] (1 = 39))))))))) AND (((NOT ((P3(x1,x3) OR ((NOT P7(x1,x3)) SINCE(0,*) P3(x1,x3))) AND ((x3 = 11) AND (NOT P1(x3))))) UNTIL(0,6) ((P0(x1) SINCE(0,*) P7(x3,x1)) AND (NOT P3(x3,x1)))) AND ((EVENTUALLY(0,9) P7(x3,x1)) AND ((P5() AND P5()) AND ((NOT (PREVIOUS(0,*) P2(x2,x1))) SINCE(0,*) P8(x2,x4,x1))))))"),
            &format!("(((NOT (P0(x4,x6) AND P1())) SINCE(0,*) (NEXT(0,9] (((P2(x4,x2,x1,x3,x5,x6) OR ((EVENTUALLY(0,6) P3(x1,x6,x5,x3)) SINCE(0,*) P4(x6,x3,x2,x1,x5,x4))) AND (NOT (EXISTS y1. P5(x5,x4,x1,x3,x6,y1,x2)))) AND (EVENTUALLY(0,1] (EVENTUALLY(0,9) ((((P6(x1,x6) OR P6(x1,x6)) UNTIL[0,1) (ONCE(0,*) (P7(x6,x4,x3,x1) AND (NOT P7(x6,x1,x4,x3))))) AND (NOT ((EVENTUALLY(0,5] ((P8(x4) AND P8(x4)) SINCE[0,*) (EXISTS y1. (((NOT (y1 = 9)) SINCE[0,*) (P9(y1) SINCE[0,0] P0(y1,x3))) SINCE[0,*) ((((NOT P10(y1,x4,x3)) SINCE[0,*) P10(x3,x4,y1)) AND P10(x4,x3,y1)) AND (x3 = 22)))))) OR (((P1() SINCE(0,*) ((13 = 21) AND (NOT (40 = 40)))) AND P11()) SINCE(0,*) P6(x4,x3))))) AND (((P4(x5,x2,x4,x3,x1,x6) AND P4(x2,x6,x1,x3,x5,x4)) AND (NOT P7(x3,x5,x2,x4))) OR P12(x4,x3,x5,x1,x6,x2)))))))) AND ((((NOT P0(x2,x1)) SINCE(0,*) P4(x2,x4,x1,x5,x6,x3)) AND (x6 = 27)) OR ((NOT (NEXT[0,0] (NEXT[0,7] (NEXT[0,5] ((P13(x6) SINCE[0,*) P6(x6,x1)) SINCE(0,*) (((NOT P10(x1,x4,x2)) UNTIL(0,9] P3(x4,x2,x6,x1)) AND (((42 = 28) SINCE(0,10) P13(x2)) SINCE(0,*) P14(x6,x4,x2)))))))) UNTIL(0,2] ((NOT P15(x6,x1)) UNTIL(0,1] P4(x2,x6,x3,x4,x5,x1)))))"),
            &format!("(ONCE(0,*) ((P0(x3,x7,x6,x4) AND P1(x4,x3,x7,x6)) AND (ONCE(0,3) (EVENTUALLY(0,9) (((PREVIOUS(0,*) ((NEXT[0,0] (((NOT (((30 = 11) AND (NOT P2())) UNTIL(0,3] P2())) SINCE[0,*) (ONCE(0,*) (((NOT (x5 = 32)) UNTIL(0,1] P3(x5)) SINCE(0,*) P4(x5)))) AND ((NOT (P3(x8) AND P2())) UNTIL(0,1] (ONCE[0,*) ((NEXT(0,2) P5(x3,x8)) OR (P6(x3,x8) AND (NOT P5(x8,x3)))))))) AND (NOT (PREVIOUS[0,1] (ONCE(0,*) (P4(x5) AND (((NOT (37 = 17)) SINCE(0,*) (0 = 2)) AND (EVENTUALLY(0,4] (x5 = 3))))))))) UNTIL(0,8) (EVENTUALLY[0,3) ((P7() AND P8(x3,x8,x5)) SINCE[0,*) ((NOT ((NOT (x8 = 25)) UNTIL[0,9) (EVENTUALLY[0,9) P9(x8,x1)))) SINCE[0,*) P10(x1,x3,x2,x8,x5))))) AND ((NEXT[0,1] ((((NOT ((30 = 9) UNTIL(0,2) P11())) SINCE[0,*) (P7() AND (33 = 35))) UNTIL[0,2] P12(x5,x2,x3)) AND (NEXT[0,10] P3(x5)))) AND (NOT (ONCE[0,10] (((ONCE[0,*) (NEXT[0,9) P13(x2,x5,x3))) AND ((P14(x3,x2,x5) AND P15(x5,x3,x2)) OR P13(x3,x2,x5))) AND P16(x5,x2,x3))))))))))"),
            &format!("((((NOT P0(x5,x6,x7,x1,x10,x2,x4)) SINCE[0,*) P1(x6,x5,x10,x2,x1,x8,x7,x4)) SINCE[0,10) (EXISTS y1. (((P2(x7,x1,x9,y1,x5) AND (NOT ((P3(y1) SINCE[0,*) (P2(x1,x7,x9,x5,y1) AND (NOT (EXISTS y2. P4(x9,y2,x5,y1,x1,x7))))) AND (x7 = 14)))) AND ((NOT ((NOT ((NOT P5(x9,y1)) UNTIL[0,2] (P6(x3,y1) AND P5(y1,x9)))) UNTIL(0,7) (P2(x2,x9,x3,x6,y1) AND (NOT P6(x2,y1))))) UNTIL[0,5] P7(x10,x8,x6,y1,x1,x9,x3,x4,x5,x7,x2))) AND (ONCE[0,*) ((ONCE[0,*) (EXISTS y2. (EVENTUALLY[0,3] (((NOT (ONCE[0,7] (ONCE(0,*) (ONCE(0,*) P8(x3,y2,y1))))) SINCE(0,*) (((NOT P9(y2,y1)) UNTIL[0,5) (P5(y2,y1) AND P9(y2,y1))) SINCE(0,*) P10(x3,y2,x5,y1))) SINCE(0,*) (ONCE[0,4] (ONCE[0,*) (ONCE[0,*) (P11(y1,x6,x1,x5,x4,x3,x9,y2) AND (NOT ((NOT P12(x6,y2,x1,y1,x3,x4)) SINCE(0,*) (P8(x3,y1,y2) SINCE[0,9] (PREVIOUS(0,*) P13(x6,x1,y2,y1,x4,x3))))))))))))) AND P0(x6,x3,x9,y1,x1,x5,x4)))))) AND (ONCE[0,5) (EVENTUALLY(0,7) (EVENTUALLY[0,0] (((ONCE(0,*) (P8(x7,x8,x3) SINCE(0,*) (P14(x8,x3,x9,x7) AND (NOT ((NOT (NEXT[0,6] (P15(x7) AND (x7 = 17)))) UNTIL(0,4) P6(x8,x7)))))) SINCE(0,*) ((NOT (EXISTS y1. P16(x1,x7,x8,x2,x9,x6,y1))) SINCE[0,2) (P17(x9,x1,x6,x8,x10) AND P16(x5,x3,x2,x6,x4,x8,x7)))) AND P18(x8,x5,x3,x6,x10,x1,x4,x7,x2,x9))))))"),
        ] {
            let actual = parse_formula(f);
            println!("{}", actual);
        }
    }

    #[test]
    fn f() {
        let x = &format!("((P0(x2) UNTIL(0,22) P1(x1,x2)) AND (NOT (EXISTS y1. ((NOT (y1 = 28)) SINCE[0,*) P1(x2,y1)))))");
        let actual = parse_formula(x);
        println!("{:?}", actual);
    }

    #[test]
    fn nokia() {
        let input = "((ONCE [0,108000] EXISTSu.EXISTSv.insert(u,\'db2\',v,data))) OR EVENTUALLY[0,108000] (EXISTSu.EXISTSv.insert(u,\'db2\',v,data))".to_string();
        let lhs = "ONCE [0,108000] (EXISTSu.EXISTSv.insert(u,\'db2\',v,data))".to_string();
        let rhs = "EVENTUALLY[0,108000] (EXISTSu.EXISTSv.insert(u,\'db2\',v,data))".to_string();

        let lhs_expected = build_once(
            build_exists(
                vec!["u"],
                build_exists(
                    vec!["v"],
                    build_fact("insert", vec!["u", "'db2'", "v", "data"]),
                ),
            ),
            TimeInterval::new(TS::new(0), TS::new(108000)),
        );
        let rhs_expected = build_eventually(
            build_exists(
                vec!["u"],
                build_exists(
                    vec!["v"],
                    build_fact("insert", vec!["u", "'db2'", "v", "data"]),
                ),
            ),
            TimeInterval::new(TS::new(0), TS::new(108000)),
        );

        let lhs_actual = parse_formula(&lhs);
        assert_eq!(lhs_actual, lhs_expected);

        let rhs_actual = parse_formula(&rhs);
        assert_eq!(rhs_actual, rhs_expected);

        let formula_expected = build_disj(lhs_expected, rhs_expected);
        let formula_actual = parse_formula(&input);
        assert_eq!(formula_expected, formula_actual);
    }
}
