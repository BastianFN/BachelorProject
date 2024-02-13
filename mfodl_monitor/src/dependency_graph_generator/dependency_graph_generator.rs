#![allow(dead_code)]

use std::collections::HashSet;
use std::hash::Hash;

use parser::formula_syntax_tree::Formula::*;
use parser::formula_syntax_tree::*;

use dependency_graph_generator::dependency_graph::DependencyGraph;

pub fn build_dependency_graph(f: &Formula) -> DependencyGraph {
    let mut graph = DependencyGraph::new(f.clone());
    populate_dependency_graph(f, &mut graph);

    graph
}

fn populate_dependency_graph(f: &Formula, graph: &mut DependencyGraph) {
    let mut subformulas = vec![];

    match f.clone() {
        Exists(_, subf) => {
            populate_dependency_graph(&*subf, graph);
            subformulas = vec![*subf];
        }
        Conj(lhs, rhs) => {
            populate_dependency_graph(&*lhs, graph);
            subformulas.push(*lhs);
            match *rhs {
                Equals(_, _) | NotEquals(_, _) => (),
                _ => {
                    populate_dependency_graph(&*rhs, graph);
                    subformulas.push(*rhs);
                }
            }
        }
        Disj(lhs, rhs) => {
            populate_dependency_graph(&*lhs, graph);
            populate_dependency_graph(&*rhs, graph);
            subformulas = vec![*lhs, *rhs];
        }
        Once(lhs, _) | Eventually(lhs, _) => {
            populate_dependency_graph(&*lhs, graph);
            subformulas = vec![*lhs];
        }
        Next(lhs, _) | Prev(lhs, _) => {
            populate_dependency_graph(&*lhs, graph);
            subformulas = vec![*lhs];
        }
        Since(lhs, rhs, _) | Until(lhs, rhs, _) |
        NegSince(lhs, rhs, _) | NegUntil(lhs, rhs, _) => {
            populate_dependency_graph(&*lhs, graph);
            populate_dependency_graph(&*rhs, graph);
            subformulas = vec![*lhs, *rhs];
        }
        AntiConj(lhs, rhs) => {
            populate_dependency_graph(&*lhs, graph);
            populate_dependency_graph(&*rhs, graph);
            subformulas = vec![*lhs, *rhs];
        }
        Not(lhs) => {
            populate_dependency_graph(&*lhs, graph);
            subformulas = vec![*lhs];
            // We do not want to put any dependencies for these operators.
            //return;
        }
        _ => (),
    }

    graph.insert(f.clone(), subformulas);
}

/// Local implementation of the union operator. This is because the built-in union operator seems
/// unnecessarily complicated for our needs and its use makes the code a bit messy with lots of
/// duplication.
fn union<T: Eq + Hash>(lhs: &mut HashSet<T>, rhs: HashSet<T>) {
    for elem in rhs {
        lhs.insert(elem);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use constants::{get_diff, test_formula};
    use std::string::ToString;
    use parser::formula_syntax_tree::Arg::Cst;
    use parser::formula_syntax_tree::Constant::{Int, Str};
    use timeunits::TimeInterval;
    use TS;

    fn print_test_results<F: ToString>(f: &F, expected: &Vec<F>, actual: &Vec<F>) {
        println!("Testing formula: {}\n", f.to_string());

        println!("Expected:");
        expected
            .clone()
            .iter()
            .enumerate()
            .for_each(|(i, elem)| println!("  {}. {}", i, elem.to_string()));

        println!("Actual:");
        actual
            .clone()
            .iter()
            .enumerate()
            .for_each(|(i, elem)| println!("  {}. {}", i, elem.to_string()));
    }

    fn test(f: Formula, expected: Vec<Formula>) {
        let graph = build_dependency_graph(&f);

        let actual = graph.get_all_subformulas();

        print_test_results(&f, &expected, &actual);

        assert_eq!(get_diff(expected, actual), 0);
    }

    fn test_dependency(f: Formula, expected: Vec<Formula>) {
        let graph = build_dependency_graph(&f);
        let actual = graph.get_formula_dependencies(&f);

        print_test_results(&f, &expected, &actual);

        assert_eq!(get_diff(expected, actual), 0);
    }

    #[test]
    fn fact_basic() {
        let formula = test_formula(None);

        let expected = vec![formula.clone()];

        test(formula, expected);
    }

    #[test]
    fn and_basic() {
        let lhs = test_formula(Some(vec![1]));
        let rhs = test_formula(Some(vec![2]));
        let formula = build_conj(lhs.clone(), rhs.clone());

        let expected = vec![rhs, lhs, formula.clone()];

        test(formula, expected);
    }

    #[test]
    fn exists_basic() {
        let formula = build_exists(vec!["x"], test_formula(None));

        let expected = vec![formula.clone(), test_formula(None)];

        test(formula, expected);
    }

    #[test]
    fn fact_dependencies() {
        let f = test_formula(None);

        let expected = vec![];

        test_dependency(f, expected);
    }

    #[test]
    fn conj_dependencies() {
        let lhs = test_formula(Some(vec![1]));
        let rhs = test_formula(Some(vec![2]));
        let f = build_conj(lhs.clone(), rhs.clone());

        let expected = vec![lhs, rhs];

        test_dependency(f, expected);
    }

    #[test]
    fn exists_dependencies() {
        let f = build_exists(vec!["x"], test_formula(None));

        let expected = vec![test_formula(None)];

        test_dependency(f, expected);
    }

    #[test]
    fn since_dependencies() {
        let lhs = test_formula(Some(vec![1]));
        let rhs = test_formula(Some(vec![2]));
        let f = build_since(
            lhs.clone(),
            rhs.clone(),
            TimeInterval::new(TS::new(0), TS::new(10)),
        );

        let expected = vec![lhs, rhs];

        test_dependency(f, expected);
    }

    #[test]
    fn until_dependencies() {
        let lhs = test_formula(Some(vec![1]));
        let rhs = test_formula(Some(vec![2]));
        let f = build_until(
            lhs.clone(),
            rhs.clone(),
            TimeInterval::new(TS::new(0), TS::new(10)),
        );

        let expected = vec![lhs, rhs];

        test_dependency(f, expected);
    }

    #[test]
    fn more_complex() {
        let lhs_j = test_formula(Some(vec![1]));
        let rhs_j = test_formula(Some(vec![2]));

        let rhs_s = test_formula(Some(vec![1]));

        let tmp = build_conj(lhs_j.clone(), rhs_j.clone());

        let f = build_until(
            tmp.clone(),
            rhs_s.clone(),
            TimeInterval::new(TS::new(0), TS::new(10)),
        );

        let expected = vec![tmp, rhs_s];

        test_dependency(f, expected);
    }
}
