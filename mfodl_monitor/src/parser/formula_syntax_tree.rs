use std::fmt;
use core::hash;

use constants::CONJ_NEG_ERROR;
use parser::formula_syntax_tree::Constant::{Int, Str};
use parser::formula_syntax_tree::Formula::*;
use std::collections::{BTreeSet, HashSet};

use timeunits::*;

// impl hash::Hash for JqProgram {
//     fn hash<H: hash::Hasher>(&self, state: &mut H) {
//         self.0.hash(state);
//     }
// }

// impl fmt::Debug for JqProgram {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         write!(f, "JqProgram {{ jq: [JqProgram Instance] }}")
//     }
// }


#[derive(Hash, Eq, Clone, Debug, PartialEq, Ord, PartialOrd, Abomonation)]
pub enum Formula {
    True,
    False,
    VALUE,
    Eos,

    CstFact(String, Vec<Constant>),
    Fact(String, Vec<Arg>),
    JSONQuery(String, Vec<String>),
    Not(Box<Formula>),
    Equals(String, Box<Arg>),

    Conj(Box<Formula>, Box<Formula>),
    Disj(Box<Formula>, Box<Formula>),
    AntiConj(Box<Formula>, Box<Formula>),
    Exists(Vec<String>, Box<Formula>),

    Since(Box<Formula>, Box<Formula>, TimeInterval),
    Until(Box<Formula>, Box<Formula>, TimeInterval),
    NegSince(Box<Formula>, Box<Formula>, TimeInterval),
    NegUntil(Box<Formula>, Box<Formula>, TimeInterval),
    Once(Box<Formula>, TimeInterval),
    Eventually(Box<Formula>, TimeInterval),

    Next(Box<Formula>, TimeInterval),
    Prev(Box<Formula>, TimeInterval),

    FormulaError(String),
}

#[derive(Hash, Eq, Clone, Debug, PartialEq, Ord, PartialOrd, Abomonation)]
pub enum Constant {
    Int(i32),
    Str(String),
}

#[derive(Hash, Eq, Clone, Debug, PartialEq, Ord, PartialOrd, Abomonation)]
pub enum Arg {
    Cst(Constant),
    Var(String),
}

pub fn is_var(f: &Arg) -> bool {
    match f.clone() {
        Arg::Var(_) => true,
        _ => false,
    }
}

impl fmt::Display for Constant {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Int(i) => i.to_string(),
                Str(s) => format!("'{}'", s.to_string()).to_string(),
            }
        )
    }
}

impl fmt::Display for Arg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Arg::Cst(i) => match i {
                    Int(i) => i.to_string(),
                    Str(s) => format!("'{}'", s.to_string()).to_string(),
                },
                Arg::Var(v) => format!("{}", v).to_string(),
            }
        )
    }
}

impl fmt::Display for Formula {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut str = String::new();

        match self.clone() {
            Not(f) => {
                str.push('~');
                str.push_str(&(*f).to_string())
            }
            CstFact(name, args) => {
                str.push_str(&name);

                if !args.is_empty() {
                    let mut tmp = String::new();
                    for i in 0..(args.len() - 1) {
                        tmp.push_str(&args[i].to_string());
                        tmp.push(',');
                    }
                    tmp.push_str(&args[args.len() - 1].to_string());

                    str.push_str(&bracket_string(tmp));
                } else {
                    str.push_str("()");
                }
            }
            Fact(name, args) => {
                str.push_str(&name);

                if !args.is_empty() {
                    let mut tmp = String::new();
                    for i in 0..(args.len() - 1) {
                        tmp.push_str(&args[i].to_string());
                        tmp.push(',');
                    }
                    tmp.push_str(&args[args.len() - 1].to_string());

                    str.push_str(&bracket_string(tmp));
                } else {
                    str.push_str("()");
                }
            }
            JSONQuery(name, aliases) => {
                str.push_str(&name);
                //TODO add aliases
            }
            Once(lhs, time) => {
                let mut tmp = String::new();
                tmp.push_str(&"<O>");
                tmp.push_str(&"[");
                tmp.push_str(&*time.start.to_string());
                tmp.push_str(&", ");
                tmp.push_str(&*time.end.to_string());
                tmp.push_str(&"]");
                tmp.push_str(&(*lhs).to_string());
                str.push_str(&bracket_string(tmp))
            }
            Eventually(lhs, time) => {
                let mut tmp = String::new();
                tmp.push_str(&"<E>");
                tmp.push_str(&"[");
                tmp.push_str(&*time.start.to_string());
                tmp.push_str(&", ");
                tmp.push_str(&*time.end.to_string());
                tmp.push_str(&"]");
                tmp.push_str(&(*lhs).to_string());
                str.push_str(&bracket_string(tmp))
            }
            Since(lhs, rhs, time) => {
                let mut tmp = String::new();
                tmp.push_str(&(*lhs).to_string());
                tmp.push_str(&"<S>");
                tmp.push_str(&(rhs).to_string());
                tmp.push_str(&"[");
                tmp.push_str(&*time.start.to_string());
                tmp.push_str(&", ");
                tmp.push_str(&*time.end.to_string());
                tmp.push_str(&"]");
                str.push_str(&bracket_string(tmp))
            }
            Until(lhs, rhs, time) => {
                let mut tmp = String::new();
                tmp.push_str(&(*lhs).to_string());
                tmp.push_str(&"<U>");
                tmp.push_str(&(rhs).to_string());
                tmp.push_str(&"[");
                tmp.push_str(&*time.start.to_string());
                tmp.push_str(&", ");
                tmp.push_str(&*time.end.to_string());
                tmp.push_str(&"]");
                str.push_str(&bracket_string(tmp))
            }
            NegSince(lhs, rhs, time) => {
                let mut tmp = String::new();
                tmp.push_str(&(*lhs).to_string());
                tmp.push_str(&"<Neg_S>");
                tmp.push_str(&(rhs).to_string());
                tmp.push_str(&"[");
                tmp.push_str(&*time.start.to_string());
                tmp.push_str(&", ");
                tmp.push_str(&*time.end.to_string());
                tmp.push_str(&"]");
                str.push_str(&bracket_string(tmp))
            }
            NegUntil(lhs, rhs, time) => {
                let mut tmp = String::new();
                tmp.push_str(&(*lhs).to_string());
                tmp.push_str(&"<Neg_U>");
                tmp.push_str(&(rhs).to_string());
                tmp.push_str(&"[");
                tmp.push_str(&*time.start.to_string());
                tmp.push_str(&", ");
                tmp.push_str(&*time.end.to_string());
                tmp.push_str(&"]");
                str.push_str(&bracket_string(tmp))
            }
            Equals(var, val) => {
                str.push_str(&format!("{} == {}", var.to_string(), val.to_string()));
            }
            Conj(f1, f2) => {
                let mut tmp = String::new();
                tmp.push_str(&(*f1).to_string());
                tmp.push_str(&"&&");
                tmp.push_str(&(f2).to_string());
                str.push_str(&bracket_string(tmp));
            }
            Disj(f1, f2) => {
                let mut tmp = String::new();
                tmp.push_str(&(*f1).to_string());
                tmp.push_str(&"||");
                tmp.push_str(&(f2).to_string());
                str.push_str(&bracket_string(tmp));
            }
            AntiConj(f1, f2) => {
                str.push_str(&Conj(Box::new(*f1), Box::new(Not(Box::new(*f2)))).to_string());
            }
            Exists(v, f) => {
                str.push('E');

                for i in 0..v.len() {
                    str.push_str(&v[i]);
                    str.push(".".parse().unwrap());
                }

                str.push('.');
                str.push_str(&(*f).to_string());
            }
            True => {
                str.push_str("true");
            }
            False => {
                str.push_str("false");
            }
            Eos => {
                str.push_str("<eos>");
            }
            VALUE => {
                str.push_str("value");
            }
            Next(lhs, time) => {
                let mut tmp = String::new();
                tmp.push_str(&"next");
                tmp.push_str(&"[");
                tmp.push_str(&*time.start.to_string());
                tmp.push_str(&", ");
                tmp.push_str(&*time.end.to_string());
                tmp.push_str(&"]");
                tmp.push_str(&(*lhs).to_string());
                str.push_str(&bracket_string(tmp))
            }
            Prev(lhs, time) => {
                let mut tmp = String::new();
                tmp.push_str(&"prev");
                tmp.push_str(&"[");
                tmp.push_str(&*time.start.to_string());
                tmp.push_str(&", ");
                tmp.push_str(&*time.end.to_string());
                tmp.push_str(&"]");
                tmp.push_str(&(*lhs).to_string());
                str.push_str(&bracket_string(tmp))
            }
            FormulaError(message) => println!("{}", message),
        }

        write!(f, "{}", str)
    }
}

fn bracket_string(s: String) -> String {
    let mut str = String::new();

    str.push('(');
    str.push_str(&s);
    str.push(')');

    str
}

pub fn free_variables(f: Formula) -> BTreeSet<Arg> {
    match f.clone() {
        Not(f) => free_variables(*f),
        Fact(_name, args) => args.into_iter().filter(|x| is_var(x)).collect(),
        Next(lhs, _time) | Prev(lhs, _time) => {
            let l = free_variables(*lhs);
            return l;
        }
        Once(lhs, _time) | Eventually(lhs, _time) => {
            let l = free_variables(*lhs);
            return l;
        }
        Since(lhs, rhs, _time)
        | Until(lhs, rhs, _time)
        | NegSince(lhs, rhs, _time)
        | NegUntil(lhs, rhs, _time) => {
            let mut l = free_variables(*lhs);
            let mut r = free_variables(*rhs);
            l.append(&mut r);
            return l;
        }
        Conj(lhs, rhs) | Disj(lhs, rhs) | AntiConj(lhs, rhs) => {
            let mut l = free_variables(*lhs);
            let mut r = free_variables(*rhs);
            l.append(&mut r);
            return l;
        }
        CstFact(var, _) => {
            let mut l = BTreeSet::new();
            l.insert(Arg::Var(var));
            return l;
        }
        Equals(_var, val) => {
            let mut l = BTreeSet::new();
            l.insert(*val);
            return l;
        }
        Exists(v, f) => {
            let mut l = free_variables(*f);
            for v_ in v {
                l.remove(&Arg::Var(v_));
            }
            return l;
        }
        _ => BTreeSet::new(),
    }
}

pub fn merge_variables(lhs: Vec<Arg>, rhs: Vec<Arg>) -> Vec<Arg> {
    let mut result = lhs.clone();
    let mut lhs_string = HashSet::with_capacity(lhs.len());
    for l in lhs {
        if let Arg::Var(s) = l {
            lhs_string.insert(s.clone());
        }
    }

    for r in rhs {
        if let Arg::Var(s) = r.clone() {
            if !lhs_string.contains(&s.clone()) {
                result.push(r.clone());
            }
        }
    }

    result
}

pub fn merge_variables_string(lhs: Vec<String>, rhs: Vec<String>) -> Vec<String> {
    let mut result = lhs.clone();
    for r in rhs {
        if !lhs.contains(&r.clone()) {
            result.push(r.clone());
        }
    }

    result
}

pub fn free_variables_original_order(f: Formula) -> Vec<Arg> {
    match f.clone() {
        Not(f) => free_variables_original_order(*f),
        Fact(_name, args) => args.clone(),
        Next(lhs, _time) | Prev(lhs, _time) => free_variables_original_order(*lhs),
        Once(lhs, _time) | Eventually(lhs, _time) => free_variables_original_order(*lhs),
        Since(_lhs, rhs, _time)
        | Until(_lhs, rhs, _time)
        | NegSince(_lhs, rhs, _time)
        | NegUntil(_lhs, rhs, _time) => free_variables_original_order(*rhs),
        Conj(lhs, rhs) | Disj(lhs, rhs) | AntiConj(lhs, rhs) => merge_variables(
            free_variables_original_order(*lhs),
            free_variables_original_order(*rhs),
        ),
        CstFact(var, _) | Equals(var, _) => vec![Arg::Var(var)],
        Exists(v, f) => {
            let mut l = free_variables_original_order(*f);

            let mut v_arg = Vec::new();
            for v_ in v {
                v_arg.push(Arg::Var(v_))
            }

            let _ = l.iter_mut().filter(|elem| {
                let b = v_arg.contains(elem);
                b
            });
            return l;
        }
        _ => Vec::new(),
    }
}

// ====================== FORMULA BUILDERS ========================

pub fn build_true() -> Formula {
    True
}

pub fn build_eos() -> Formula {
    Eos
}

pub fn build_false() -> Formula {
    False
}

pub fn build_value() -> Formula {
    VALUE
}

pub fn build_fact_args(name: &str, args: Vec<Arg>) -> Formula {
    Fact(name.to_string(), args)
}

pub fn build_fact_json(name: &str, args: Vec<String>) -> Formula {
    let mut vec = Vec::new();
    for v in args {
        vec.push(Arg::Var(v.to_string()));
    }
    Fact(name.into(), vec)
}

pub fn build_fact_const(name: &str, args: Vec<Constant>) -> Formula {
    CstFact(name.to_string(), args)
}

pub fn build_fact(name: &str, vs: Vec<&str>) -> Formula {
    let mut vec = Vec::new();
    for v in vs {
        vec.push(if let Ok(i) = v.clone().parse::<i32>() {
            Arg::Cst(Int(i))
        } else if v.contains("'") {
            Arg::Cst(Str(v[1..v.len() - 1].to_string()))
        } else {
            Arg::Var(v.to_string())
        });
    }
    Fact(name.into(), vec)
}

pub fn build_fact_int(name: &str, vs: Vec<i32>) -> Formula {
    let mut vec = vec![];
    for v in vs {
        vec.push(Arg::Cst(Int(v)));
    }
    Fact(name.into(), vec)
}

pub fn build_not(f: Formula) -> Formula {
    match f.clone() {
        Not(subf) => *subf,
        _ => Not(Box::new(f)),
    }
}

pub fn build_equals(var: &str, val: Arg) -> Formula {
    Equals(var.to_string(), Box::new(val))
}

/*pub fn build_not_equals(var: &str, val: Arg) -> Formula {
    NotEquals(var.to_string(), Box::new(val))
}*/

pub fn build_conj(lhs: Formula, rhs: Formula) -> Formula {
    match (lhs.clone(), rhs.clone()) {
        (Not(_), Not(_)) => FormulaError(format!("{}: {}", CONJ_NEG_ERROR, lhs.to_string())),
        (Not(lhs), rhs) | (rhs, Not(lhs)) => build_anticonj(rhs, *lhs),
        _ => Conj(Box::new(lhs), Box::new(rhs)),
    }
}

pub fn build_anticonj(lhs: Formula, rhs: Formula) -> Formula {
    AntiConj(Box::new(lhs), Box::new(rhs))
}

pub fn build_disj(lhs: Formula, rhs: Formula) -> Formula {
    Disj(Box::new(lhs), Box::new(rhs))
}

pub fn build_implication(lhs: Formula, rhs: Formula) -> Formula {
    build_not(build_conj(build_not(lhs), rhs))
}

pub fn build_iff(lhs: Formula, rhs: Formula) -> Formula {
    build_conj(
        build_implication(lhs.clone(), rhs.clone()),
        build_implication(rhs, lhs),
    )
}

pub fn build_exists(var: Vec<&str>, expr: Formula) -> Formula {
    let mut vars = Vec::with_capacity(var.len());
    for x in var {
        vars.push(x.to_string())
    }
    Exists(vars, Box::new(expr))
}

pub fn build_forall(var: Vec<&str>, f: Formula) -> Formula {
    build_not(build_exists(var, build_not(f)))
}

pub fn build_since(lhs: Formula, rhs: Formula, interval: TimeInterval) -> Formula {
    Since(Box::new(lhs), Box::new(rhs), interval)
}

pub fn build_neg_since(lhs: Formula, rhs: Formula, interval: TimeInterval) -> Formula {
    NegSince(Box::new(lhs), Box::new(rhs), interval)
}

pub fn build_until(lhs: Formula, rhs: Formula, interval: TimeInterval) -> Formula {
    Until(Box::new(lhs), Box::new(rhs), interval)
}

pub fn build_neg_until(lhs: Formula, rhs: Formula, interval: TimeInterval) -> Formula {
    NegUntil(Box::new(lhs), Box::new(rhs), interval)
}

pub fn build_once(rhs: Formula, interval: TimeInterval) -> Formula {
    Once(Box::new(rhs), interval)
}

pub fn build_eventually(rhs: Formula, interval: TimeInterval) -> Formula {
    Eventually(Box::new(rhs), interval)
}

pub fn build_historically(rhs: Formula, interval: TimeInterval) -> Formula {
    build_not(build_once(build_not(rhs), interval))
}

pub fn build_always(rhs: Formula, interval: TimeInterval) -> Formula {
    build_not(build_eventually(build_not(rhs), interval))
}

pub fn build_next(rhs: Formula, interval: TimeInterval) -> Formula {
    Next(Box::new(rhs), interval)
}

pub fn build_prev(rhs: Formula, interval: TimeInterval) -> Formula {
    Prev(Box::new(rhs), interval)
}

#[cfg(test)]
mod tests {
    use parse_formula;
    use parser::formula_syntax_tree::{free_variables_original_order, merge_variables, Arg};

    #[test]
    fn test_merge_variables() {
        let lhs = vec![Arg::Var("y".to_string()), Arg::Var("x".to_string())];
        let rhs = vec![Arg::Var("x".to_string()), Arg::Var("y".to_string())];
        assert_eq!(
            vec![Arg::Var("y".to_string()), Arg::Var("x".to_string())],
            merge_variables(lhs, rhs)
        );
    }

    #[test]
    fn test_merge_variables_1() {
        let lhs = vec![Arg::Var("y".to_string()), Arg::Var("x".to_string())];
        let rhs = vec![
            Arg::Var("z".to_string()),
            Arg::Var("y".to_string()),
            Arg::Var("x".to_string()),
        ];
        assert_eq!(
            vec![
                Arg::Var("y".to_string()),
                Arg::Var("x".to_string()),
                Arg::Var("z".to_string())
            ],
            merge_variables(lhs, rhs)
        );
    }

    #[test]
    fn test_original_order() {
        let formula = parse_formula(&"P() SINCE[0,1] (x2 = 0)");
        let res = free_variables_original_order(formula);
        assert_eq!(vec![Arg::Var("x2".to_string())], res)
    }

    #[test]
    fn test_original_order1() {
        let formula = parse_formula(&"P() SINCE[0,1] (P2(x1) AND (x2 = 0))");
        let res = free_variables_original_order(formula);
        assert_eq!(
            vec![Arg::Var("x1".to_string()), Arg::Var("x2".to_string())],
            res
        )
    }

    #[test]
    fn test_original_order2() {
        let formula = parse_formula(&"P() SINCE[0,1] (A(x,u,i) AND B(x,i,u,n,m))");
        let res = free_variables_original_order(formula);
        assert_eq!(
            vec![
                Arg::Var("x".to_string()),
                Arg::Var("u".to_string()),
                Arg::Var("i".to_string()),
                Arg::Var("n".to_string()),
                Arg::Var("m".to_string())
            ],
            res
        )
    }
}
