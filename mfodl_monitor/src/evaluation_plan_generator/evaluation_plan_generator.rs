use std::fmt;

use parser::formula_syntax_tree::Formula::*;
use parser::formula_syntax_tree::*;
use timeunits::*;

use evaluation_plan_generator::evaluation_plan_generator::Expr::*;

#[derive(Debug, PartialEq, Eq, Clone, Ord, PartialOrd, Hash)]
pub enum Expr {
    FULL,
    EMPTY,
    Fact(String, Vec<Arg>),

    Not(Box<Expr>),
    // This is a table in the monitor corresponding to a single subformula
    Join(Box<Expr>, Box<Expr>),
    UnionJoin(Box<Expr>, Box<Expr>),
    Equals(String, Box<Arg>),
    // Since and Until
    Since(Box<Expr>, Box<Expr>, TimeInterval),
    Until(Box<Expr>, Box<Expr>, TimeInterval),
    NegSince(Box<Expr>, Box<Expr>, TimeInterval),
    NegUntil(Box<Expr>, Box<Expr>, TimeInterval),
    Once(Box<Expr>, TimeInterval),
    Eventually(Box<Expr>, TimeInterval),

    Next(Box<Expr>, TimeInterval),
    Prev(Box<Expr>, TimeInterval),
    // This is a table with all columns from the subexpression, but the column
    // specified by the string is omitted.
    VarEquals(String, Arg),
    Project(Vec<String>, Box<Expr>),
    Extend(String, String, Box<Expr>),
    Filter(String, Arg, Box<Expr>),
    NegFilter(String, Arg, Box<Expr>),
    Antijoin(Box<Expr>, Box<Expr>),
    Error(String),
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut s = String::new();

        match self.clone() {
            FULL => s.push_str(&"FULL"),
            EMPTY => s.push_str(&"EMPTY"),
            UnionJoin(lhs, rhs) => {
                s.push_str(&format!(
                    "UnionJoin({}, {})",
                    lhs.to_string(),
                    rhs.to_string()
                ));
            }
            VarEquals(var, x) => {
                s.push_str(&format!(
                    "VarEquals({var} = {})",
                    x.to_string()
                ));
            }
            Join(lhs, rhs) => {
                s.push_str(&format!("Join({}, {})", lhs, rhs));
            }
            Expr::Next(lhs, _) => {
                s.push_str(&format!("Next({})", lhs));
            }
            Expr::Prev(lhs, _) => {
                s.push_str(&format!("Prev({})", lhs));
            }
            Expr::Once(lhs, _) => {
                s.push_str(&format!("Once({})", lhs));
            }
            Expr::Eventually(lhs, _) => {
                s.push_str(&format!("Until({})", lhs));
            }
            Expr::Since(lhs, rhs, _) => {
                s.push_str(&format!("Since({}, {})", lhs, rhs));
            }
            Expr::Until(lhs, rhs, _) => {
                s.push_str(&format!("Until({}, {})", lhs, rhs));
            }
            Expr::NegSince(lhs, rhs, _) => {
                s.push_str(&format!("NegSince({}, {})", lhs, rhs));
            }
            Expr::NegUntil(lhs, rhs, _) => {
                s.push_str(&format!("NegUntil({}, {})", lhs, rhs));
            }
            Extend(var1, var2, expr) => {
                s.push_str(&format!("Extend column {} with {} in {}", var1, var2, expr));
            }
            Project(var, expr) => {
                s.push_str(&format!("Cut column {:?} from {}", var, expr));
            }
            Antijoin(lhs, rhs) => {
                s.push_str(&format!(
                    "Antijoin({}, {})",
                    lhs.to_string(),
                    rhs.to_string()
                ));
            }
            Filter(var, val, expr) => {
                s.push_str(&format!(
                    "Filter all {} != {} from {}",
                    var,
                    val,
                    expr.to_string()
                ));
            }
            NegFilter(var, val, expr) => {
                s.push_str(&format!(
                    "Filter all {} == {} from {}",
                    var,
                    val,
                    expr.to_string()
                ));
            }
            Error(message) => s.push_str(&message),
            _ => {}
        }
        write!(f, "{}", s)
    }
}

pub fn generate_evaluation_plan(f: &Formula) -> Expr {
    generate_boolean_evaluation_plan(f.clone())
}

pub fn optimize_evaluation_plan(plan : Expr) -> Expr {
    optimize_cases(plan)
}

fn optimize_cases(plan : Expr) -> Expr {
    match plan {
        FULL | EMPTY | Expr::Fact(_,_) | VarEquals(_, _) | Extend(_, _, _)
        | Filter(_, _, _) | NegFilter(_, _, _) | Expr::Equals(_,_) => plan,
        Expr::Next(lhs, interval) => {
            let new_lhs = optimize_cases(*lhs);
            match new_lhs {
                EMPTY => EMPTY,
                lhs => Expr::Next(Box::new(lhs), interval)
            }
        }
        Expr::Prev(lhs, interval) => {
            let new_lhs = optimize_cases(*lhs);
            match new_lhs {
                EMPTY => EMPTY,
                lhs => Expr::Prev(Box::new(lhs), interval)
            }
        }
        Project(vals, rhs) => {
            // if vals not in rhs filter non existent vals, when empty remove
            let new_rhs = optimize_cases(*rhs);
            match new_rhs {
                FULL => FULL,
                EMPTY => EMPTY,
                rhs => Project(vals, Box::new(rhs))
            }
        }
        Expr::Once(lhs, interval) => {
            let new_lhs = optimize_cases(*lhs);
            match new_lhs {
                EMPTY => EMPTY,
                lhs => Expr::Once(Box::new(lhs), interval)
            }
        }
        Expr::Eventually(lhs, interval) => {
            let new_lhs = optimize_cases(*lhs);
            match new_lhs {
                EMPTY => EMPTY,
                lhs => Expr::Eventually(Box::new(lhs), interval)
            }
        }
        Join(lhs, rhs) => {
            let new_lhs = optimize_cases(*lhs);
            let new_rhs = optimize_cases(*rhs);
            match (new_lhs, new_rhs) {
                (EMPTY, _) | (_, EMPTY) => EMPTY,
                (FULL, FULL) => FULL,
                (FULL, expr) | (expr, FULL) => expr,
                (lhs, rhs) => { if lhs == rhs { return lhs; }
                    Join(Box::new(lhs), Box::new(rhs))
                }
            }
        }
        Antijoin(lhs, rhs) => {
            let new_lhs = optimize_cases(*lhs);
            let new_rhs = optimize_cases(*rhs);
            match (new_lhs, new_rhs) {
                (FULL, FULL) | (EMPTY, FULL) | (EMPTY, EMPTY) => EMPTY,
                (FULL, EMPTY) => FULL,
                (lhs, EMPTY) => lhs,
                (EMPTY, _) => EMPTY,
                (_, FULL) => EMPTY,
                (lhs, rhs) => Antijoin(Box::new(lhs), Box::new(rhs))
            }
        }
        UnionJoin(lhs, rhs) => {
            let new_lhs = optimize_cases(*lhs);
            let new_rhs = optimize_cases(*rhs);
            match (new_lhs, new_rhs) {
                (FULL, _) | (_, FULL) => FULL,
                (EMPTY, EMPTY) => EMPTY,
                (expr, EMPTY) | (EMPTY, expr) => expr,
                (lhs, rhs) => {
                    if lhs == rhs { lhs } else {
                        UnionJoin(Box::new(lhs), Box::new(rhs))
                    }
                }
            }
        }
        Expr::Since(lhs, rhs, interval) => {
            let new_lhs = optimize_cases(*lhs);
            let new_rhs = optimize_cases(*rhs);
            match (new_lhs, new_rhs) {
                (_, EMPTY) => EMPTY,
                (EMPTY, rhs) => if interval.get_raw_start() == 0 { rhs } else { EMPTY },
                (FULL, rhs) => Expr::Once(Box::new(rhs), interval),
                (lhs, rhs) => Expr::Since(Box::new(lhs), Box::new(rhs), interval)
            }
        }
        Expr::Until(lhs, rhs, interval) => {
            let new_lhs = optimize_cases(*lhs);
            let new_rhs = optimize_cases(*rhs);
            match (new_lhs, new_rhs) {
                (_, EMPTY) => EMPTY,
                (EMPTY, rhs) => if interval.get_raw_start() == 0 { rhs } else { EMPTY },
                (FULL, rhs) => Expr::Eventually(Box::new(rhs), interval),
                (lhs, rhs) => Expr::Until(Box::new(lhs), Box::new(rhs), interval)
            }
        }
        Expr::NegSince(lhs, rhs, interval) => {
            let new_lhs = optimize_cases(*lhs);
            let new_rhs = optimize_cases(*rhs);
            match (new_lhs, new_rhs) {
                (EMPTY, EMPTY) | (FULL, EMPTY) => EMPTY,
                (FULL, rhs) => if interval.get_raw_start() == 0 {rhs} else { EMPTY },
                (lhs, rhs) => Expr::NegSince(Box::new(lhs), Box::new(rhs), interval)
            }
        }
        Expr::NegUntil(lhs, rhs, interval) => {
            let new_lhs = optimize_cases(*lhs);
            let new_rhs = optimize_cases(*rhs);
            match (new_lhs, new_rhs) {
                (EMPTY, EMPTY) | (FULL, EMPTY) => EMPTY,
                (FULL, rhs) => if interval.get_raw_start() == 0 {rhs} else { EMPTY },
                (lhs, rhs) => Expr::NegUntil(Box::new(lhs), Box::new(rhs), interval)
            }
        }
        _ => {Error("Unhandled Case in optimize cases".to_string())}
    }
}


fn generate_boolean_evaluation_plan(f: Formula) -> Expr {
    match f.clone() {
        True => FULL,
        False => EMPTY,
        Formula::Fact(_, _) => build_assignment(f),
        Formula::Not(subf) => {
            match *subf.clone() {
                FormulaError(_) => Error(format!("Error while generating an evaluation plan for {}", f.to_string())),
                _ => {
                    build_assignment(*subf)
                }
            }
        }
        Conj(lhs, rhs) => {
            let fv = free_variables(*lhs.clone());
            //let org_fv = free_variables_original_order(*lhs.clone());
            let lhs_expr = build_assignment(*lhs);
            match *rhs.clone() {
                Formula::Equals(var, val) => {
                    //println!("Build equals in conj {:?}", var);
                    let is_data = match *val.clone() {
                        Arg::Cst(_) => true,
                        Arg::Var(_) => false
                    };

                    if fv.contains(&Arg::Var(var.clone())) && is_data {
                        build_filter(var, *val, lhs_expr)
                    } else {
                        if fv.contains(&val) {
                            build_filter(var, *val, lhs_expr)
                        } else {
                            if let Arg::Var(var2) = *val {
                                build_extend(var, var2, lhs_expr)
                            } else {
                                panic!(" EQUALS at RHS of conj is neither filter nor extend")
                            }
                        }
                    }
                },
                _ => {
                    let rhs_expr = build_assignment(*rhs);
                    build_join(lhs_expr, rhs_expr)
                }
            }
        }
        Formula::Equals(var, val) => {
            build_var_equals(var, *val)
        }
        Disj(lhs, rhs) => {
            let lhs_expr = build_assignment(*lhs);
            let rhs_expr = build_assignment(*rhs);

            build_union(lhs_expr, rhs_expr)
        }
        AntiConj(lhs, rhs) => {
            let lhs_expr = build_assignment(*lhs);
            let rhs_expr = build_assignment(*rhs);
            build_antijoin(lhs_expr, rhs_expr)
        }
        Formula::Prev(lhs, interval) => {
            let lhs_expr = build_assignment(*lhs);
            build_prev(lhs_expr, interval)
        }
        Formula::Next(lhs, interval) => {
            let lhs_expr = build_assignment(*lhs);
            build_next(lhs_expr, interval)
        }
        Formula::Once(lhs, interval) => {
            let lhs_expr = build_assignment(*lhs);
            build_once(lhs_expr, interval)
        }
        Formula::Eventually(lhs, interval) => {
            let lhs_expr = build_assignment(*lhs);
            build_eventually(lhs_expr, interval)
        }
        Formula::Since(lhs, rhs, interval) => {
            let lhs_expr = build_assignment(*lhs);
            let rhs_expr = build_assignment(*rhs);
            build_since(lhs_expr, rhs_expr, interval)
        }
        Formula::Until(lhs, rhs, interval) => {
            let lhs_expr = build_assignment(*lhs);
            let rhs_expr = build_assignment(*rhs);

            build_until(lhs_expr, rhs_expr, interval)
        }
        Formula::NegSince(lhs, rhs, interval) => {
            let lhs_expr = build_assignment(*lhs);
            let rhs_expr = build_assignment(*rhs);
            build_neg_since(lhs_expr, rhs_expr, interval)
        }
        Formula::NegUntil(lhs, rhs, interval) => {
            let lhs_expr = build_assignment(*lhs);
            let rhs_expr = build_assignment(*rhs);

            build_neg_until(lhs_expr, rhs_expr, interval)
        }
        Exists(var, subf) => {
            let subf_expr = build_assignment(*subf);
            build_neg_projection(var, subf_expr)
        }
        _ => Error(format!("Unrecognised formula for boolean evaluation plan generation: {:?}", f)),
    }
}

fn build_var_equals(var: String, lhs: Arg) -> Expr {
    VarEquals(var, lhs)
}

fn build_union(lhs: Expr, rhs: Expr) -> Expr {
    UnionJoin(Box::new(lhs), Box::new(rhs))
}

fn build_join(lhs: Expr, rhs: Expr) -> Expr {
    Join(Box::new(lhs), Box::new(rhs))
}
fn build_neg_projection(var: Vec<String>, expr: Expr) -> Expr {
    Project(var, Box::new(expr))
}

fn build_antijoin(lhs: Expr, rhs: Expr) -> Expr {
    Antijoin(Box::new(lhs), Box::new(rhs))
}

fn build_since(lhs: Expr, rhs: Expr, interval: TimeInterval) -> Expr {
    Expr::Since(Box::new(lhs), Box::new(rhs), interval)
}

fn build_neg_since(lhs: Expr, rhs: Expr, interval: TimeInterval) -> Expr {
    Expr::NegSince(Box::new(lhs), Box::new(rhs), interval)
}

fn build_next(lhs: Expr, interval: TimeInterval) -> Expr {
    Expr::Next(Box::new(lhs), interval)
}

fn build_prev(lhs: Expr, interval: TimeInterval) -> Expr {
    Expr::Prev(Box::new(lhs), interval)
}

fn build_until(lhs: Expr, rhs: Expr, interval: TimeInterval) -> Expr {
    Expr::Until(Box::new(lhs), Box::new(rhs), interval)
}

fn build_neg_until(lhs: Expr, rhs: Expr, interval: TimeInterval) -> Expr {
    Expr::NegUntil(Box::new(lhs), Box::new(rhs), interval)
}

fn build_once(lhs: Expr, interval : TimeInterval) -> Expr {
    match lhs.clone() {
        EMPTY => EMPTY,
        _ => Expr::Once(Box::new(lhs), interval)
    }
}

fn build_eventually(lhs: Expr, interval : TimeInterval) -> Expr {
    match lhs.clone() {
        EMPTY => EMPTY,
        _ => Expr::Eventually(Box::new(lhs), interval)
    }
}

fn build_filter(var: String, val: Arg, expr: Expr) -> Expr {
    Filter(var, val, Box::new(expr))
}

fn build_extend(var1: String, var2: String, expr: Expr) -> Expr {
    Extend(var1, var2, Box::new(expr))
}

/*fn build_neg_filter(var: String, val: Arg, expr: Expr) -> Expr {
    NegFilter(var, val, Box::new(expr))
}*/

fn build_expr_equals(var: String, val: Arg) -> Expr {
    Expr::Equals(var, Box::new(val))
}

// todo build expr
pub fn build_assignment(f: Formula) -> Expr {
    match f.clone() {
        True => FULL,
        False => EMPTY,
        Formula::Fact(x, y) => Expr::Fact(x, y),
        Formula::Not(lhs) => {
            let expr_lhs = build_assignment(*lhs);
            Expr::Not(Box::new(expr_lhs))
        },
        Formula::Equals(x, y) => build_expr_equals(x, *y),
        Conj(lhs, rhs) => {
            let expr_lhs = build_assignment(*lhs);
            let expr_rhs = build_assignment(*rhs);
            build_join(expr_lhs, expr_rhs)
        },
        Disj(lhs, rhs) => {
            let expr_lhs = build_assignment(*lhs);
            let expr_rhs = build_assignment(*rhs);
            build_union(expr_lhs, expr_rhs)
        }
        AntiConj(lhs, rhs) => {
            let expr_lhs = build_assignment(*lhs);
            let expr_rhs = build_assignment(*rhs);
            build_antijoin(expr_lhs, expr_rhs)
        }
        Exists(vals, rhs) => {
            let expr_rhs = build_assignment(*rhs);
            build_neg_projection(vals, expr_rhs)
        },
        Formula::Since(lhs, rhs, interval) => {
            let expr_lhs = build_assignment(*lhs);
            let expr_rhs = build_assignment(*rhs);
            build_since(expr_lhs, expr_rhs, interval)
        }
        Formula::Until(lhs, rhs, interval) => {
            let expr_lhs = build_assignment(*lhs);
            let expr_rhs = build_assignment(*rhs);
            build_until(expr_lhs, expr_rhs, interval)
        }
        Formula::NegSince(lhs, rhs, interval) => {
            let expr_lhs = build_assignment(*lhs);
            let expr_rhs = build_assignment(*rhs);
            build_neg_since(expr_lhs, expr_rhs, interval)
        }
        Formula::NegUntil(lhs, rhs, interval) => {
            let expr_lhs = build_assignment(*lhs);
            let expr_rhs = build_assignment(*rhs);
            build_neg_until(expr_lhs, expr_rhs, interval)
        }
        Formula::Once(lhs, interval) => {
            let expr_lhs = build_assignment(*lhs);
            build_once(expr_lhs, interval)
        }
        Formula::Eventually(lhs, interval) => {
            let expr_lhs = build_assignment(*lhs);
            build_eventually(expr_lhs, interval)
        }
        Formula::Next(lhs, interval) => {
            let expr_lhs = build_assignment(*lhs);
            build_next(expr_lhs, interval)
        }
        Formula::Prev(lhs, interval) => {
            let expr_lhs = build_assignment(*lhs);
            build_prev(expr_lhs, interval)
        }
        _ => {Error("build assignment missing case".to_string())}
    }
}

#[cfg(test)]
mod tests {
    use constants::test_formula;
    use parse_formula;
    use super::*;
    use parser::formula_syntax_tree;
    use timeunits::TS;
    use parser::formula_syntax_tree::Arg::Cst;
    use parser::formula_syntax_tree::Constant::{Int};

    fn test_bool_formula(f: Formula, expected: Expr) {
        let actual = generate_evaluation_plan(&f);

        assert_eq!(expected, actual);
    }

    #[test]
    fn equals_var_equals() {
        let f = parse_formula("(x2 = 0)");
        let expected = build_var_equals("x2".to_string(), Cst(Int(0)));

        test_bool_formula(f, expected);
    }

    #[test]
    fn equals_var_equals_1() {
        let f = parse_formula("P0() SINCE[0,10] (x2 = 0)");
        let expected = build_since(
            build_assignment(build_fact("P0", vec![])),
            build_assignment(build_equals("x2", Cst(Int(0)))),
            TimeInterval::new(TS::new(0), TS::new(10))
        );


        test_bool_formula(f, expected);
    }

    #[test]
    fn fact_plan() {
        let f = test_formula(None);

        let expected = build_assignment(f.clone());

        test_bool_formula(f, expected);
    }

    #[test]
    fn conj_plan() {
        let lhs = test_formula(Some(vec![1]));
        let rhs = test_formula(Some(vec![2]));

        let f = build_conj(lhs.clone(), rhs.clone());

        let expected = build_join(
            build_assignment(lhs),
            build_assignment(rhs),
        );

        test_bool_formula(f, expected)
    }

    #[test]
    fn conj_plan_var_equals_int() {
        let variable = "x";
        let lhs = build_fact("p", vec![variable.clone()]);
        let rhs = build_equals(variable, Cst(Int(5)));

        let f = build_conj(lhs.clone(), rhs.clone());

        let expected = build_filter(
            "x".to_string(),
            Cst(Int(5)),
            build_assignment(lhs),
        );

        test_bool_formula(f, expected)
    }

    #[test]
    fn conj_plan_var_equals_var() {
        let variable1 = "x";
        let variable2 = "y";
        let lhs = build_fact("p", vec![variable1.clone(), variable2.clone()]);
        let rhs = build_equals(variable1, Arg::Var(variable2.to_string()));

        let f = build_conj(lhs.clone(), rhs.clone());

        let expected = build_filter(
            variable1.to_string(),
            Arg::Var(variable2.to_string()),
            build_assignment(lhs),
        );

        test_bool_formula(f, expected)
    }

    #[test]
    fn disj_plan() {
        let lhs = test_formula(Some(vec![1]));
        let rhs = test_formula(Some(vec![2]));

        let f = build_disj(lhs.clone(), rhs.clone());

        let expected = build_union(
            build_assignment(lhs),
            build_assignment(rhs),
        );

        test_bool_formula(f, expected)
    }

    #[test]
    fn exists_plan() {
        let subf = test_formula(None);
        let f = build_exists(vec!["x"], subf.clone());

        let expected = build_neg_projection(vec!["x".into()], build_assignment(subf));

        test_bool_formula(f, expected)
    }

    #[test]
    fn conj_neg_plan() {
        let lhs = test_formula(Some(vec![1]));
        let rhs = test_formula(Some(vec![2]));

        let f = build_conj(lhs.clone(), build_not(rhs.clone()));

        let expected = build_antijoin(
            build_assignment(lhs),
            build_assignment(rhs),
        );

        test_bool_formula(f, expected);
    }

    #[test]
    fn since_plan() {
        let lhs = test_formula(Some(vec![1]));
        let rhs = test_formula(Some(vec![2]));

        let f = formula_syntax_tree::build_since(
            lhs.clone(),
            rhs.clone(),
            TimeInterval::new(TS::new(1), TS::new(5)),
        );

        let expected = build_since(
            build_assignment(lhs),
            build_assignment(rhs),
            TimeInterval::new(TS::new(1), TS::new(5)),
        );

        test_bool_formula(f, expected);
    }


    #[test]
    fn neg_since_plan() {
        let lhs = build_not(test_formula(Some(vec![1])));
        let rhs = test_formula(Some(vec![2]));

        let f = formula_syntax_tree::build_neg_since(
            lhs.clone(),
            rhs.clone(),
            TimeInterval::new(TS::new(1), TS::new(5)),
        );

        let expected = build_neg_since(
            build_assignment(lhs),
            build_assignment(rhs),
            TimeInterval::new(TS::new(1), TS::new(5)),
        );

        test_bool_formula(f, expected);
    }

    #[test]
    fn until_plan() {
        let lhs = test_formula(Some(vec![1]));
        let rhs = test_formula(Some(vec![2]));

        let f = formula_syntax_tree::build_until(
            lhs.clone(),
            rhs.clone(),
            TimeInterval::new(TS::new(1), TS::new(5)),
        );

        let expected = build_until(
            build_assignment(lhs),
            build_assignment(rhs),
            TimeInterval::new(TS::new(1), TS::new(5)),
        );

        test_bool_formula(f, expected);
    }

    #[test]
    fn prev_zero_inf() {
        let rhs = test_formula(Some(vec![2]));

        let f = formula_syntax_tree::build_prev(
            rhs.clone(),
            TimeInterval::new(TS::new(1), TS::INFINITY),
        );

        let expected = build_prev(
            build_assignment(rhs),
            TimeInterval::new(TS::new(1), TS::INFINITY),
        );

        test_bool_formula(f, expected);
    }

    #[test]
    fn prev_one_seven() {
        let rhs = test_formula(Some(vec![2]));

        let f = formula_syntax_tree::build_prev(
            rhs.clone(),
            TimeInterval::new(TS::new(1), TS::new(7)),
        );

        let expected = build_prev(
            build_assignment(rhs),
            TimeInterval::new(TS::new(1), TS::new(7)),
        );

        test_bool_formula(f, expected);
    }


    #[test]
    fn next_zero_inf() {
        let rhs = test_formula(Some(vec![2]));
        let f = formula_syntax_tree::build_next(
            rhs.clone(),
            TimeInterval::new(TS::new(1), TS::INFINITY),
        );
        let expected = build_next(
            build_assignment(rhs),
            TimeInterval::new(TS::new(1), TS::INFINITY),
        );
        test_bool_formula(f, expected);
    }

    #[test]
    fn next_one_seven() {
        let rhs = test_formula(Some(vec![2]));
        let f = formula_syntax_tree::build_next(
            rhs.clone(),
            TimeInterval::new(TS::new(1), TS::new(7)),
        );
        let expected = build_next(
            build_assignment(rhs),
            TimeInterval::new(TS::new(1), TS::new(7)),
        );
        test_bool_formula(f, expected);
    }


    #[test]
    fn once_plan() {
        let rhs = test_formula(Some(vec![2]));

        let f = formula_syntax_tree::build_once(
            rhs.clone(),
            TimeInterval::new(TS::new(1), TS::new(5)),
        );

        let expected = build_once(
            build_assignment(rhs),
            TimeInterval::new(TS::new(1), TS::new(5)),
        );

        test_bool_formula(f, expected);
    }

    #[test]
    fn eventually_plan() {
        let rhs = test_formula(Some(vec![2]));

        let f = formula_syntax_tree::build_eventually(
            rhs.clone(),
            TimeInterval::new(TS::new(1), TS::new(5)),
        );

        let expected = build_eventually(
            build_assignment(rhs),
            TimeInterval::new(TS::new(1), TS::new(5)),
        );

        test_bool_formula(f, expected);
    }

    #[test]
    fn once_formula_test() {
        let lhs = build_fact("A", vec!["a", "b"]);
        let rhs = build_fact("B", vec!["b", "c"]);
        let con = build_conj(lhs.clone(), rhs.clone());
        let f = formula_syntax_tree::build_once(
                build_conj(
                    lhs.clone(), rhs.clone()
                ), TimeInterval::new(TS::new(0), TS::new(7))
        );

        let expected = build_once(
            build_assignment(con),
            TimeInterval::new(TS::new(0), TS::new(7)),
        );

        test_bool_formula(f, expected);
    }
}
