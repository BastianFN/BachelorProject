use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;

use constants::calculate_hash;
use parser::formula_syntax_tree::Formula;
use parser::formula_syntax_tree::Formula::FormulaError;

type Id = u64;

#[derive(Clone)]
pub struct DependencyGraph {
    policy: Formula,
    base_formulas: HashSet<Id>,
    h: HashMap<Id, Node>,
}

#[derive(Clone)]
struct Node {
    pub f: Formula,
    dependencies: HashSet<Id>,
    dependants: HashSet<Id>,
}

impl Node {
    fn new(f: Formula) -> Node {
        Node {
            f,
            dependencies: HashSet::new(),
            dependants: HashSet::new(),
        }
    }
}

impl DependencyGraph {
    /// Create an empty graph
    pub fn new(policy: Formula) -> DependencyGraph {
        DependencyGraph {
            policy,
            base_formulas: HashSet::new(),
            h: HashMap::new(),
        }
    }

    /// Insert a formula in the dependency graph.
    /// Preconditions:
    ///   - 'Benchmark_10K' is not in the graph already
    ///   - all 'subformulas' have already been inserted in the graph
    pub fn insert(&mut self, f: Formula, subformulas: Vec<Formula>) {
        if let FormulaError(message) = f {
            panic!("Error when building dependency graph: {}", message);
        }

        // Invalidate the insertion if the formula is already in the graph.
        if self.contains(&f) {
            return;
        }

        let hash = calculate_hash(&f);

        let dep_node = Node::new(f.clone());
        self.h.insert(hash, dep_node);

        self.set_up_bidirectional_connection(&f, &subformulas);

        match f {
            Formula::Fact(_, _) | Formula::True | Formula::False => {
                self.base_formulas.insert(hash.clone());
            }
            _ => (),
        };
    }

    /// Update an existing entry in the graph.
    /// Preconditions:
    ///   - 'Benchmark_10K' should already be in the graph
    ///   -  all 'subformulas' should already be in the graph
    pub fn update(&mut self, f: &Formula, subformulas: Vec<Formula>) {
        // Invalidate the update if the formula is not in the graph already.
        if !self.contains(&f) {
            return;
        }

        let hash = calculate_hash(&f);

        // Make the bidirectional link onedirectional. We remove the node for
        // the formula and want to ensure that its dependencies don't hold
        // the information that this formula needs them.
        self.get_formula_dependencies(f).iter().for_each(|subf| {
            if let Some(subdep) = self.h.get_mut(&calculate_hash(subf)) {
                subdep.dependants.remove(&hash);
            }
        });

        self.set_up_bidirectional_connection(f, &subformulas);
    }

    pub fn contains(&self, f: &Formula) -> bool {
        self.h.contains_key(&calculate_hash(f))
    }

    pub fn get_formula_dependencies(&self, f: &Formula) -> Vec<Formula> {
        let mut dependencies = Vec::new();

        if let Some(dep_node) = self.h.get(&calculate_hash(&f)) {
            dep_node.dependencies.iter().for_each(|subdep_id| {
                if let Some(subdep) = self.h.get(&subdep_id) {
                    dependencies.push(subdep.f.clone());
                }
            });
        }

        dependencies.sort();
        dependencies
    }

    pub fn get_formula_dependants(&self, f: &Formula) -> Vec<Formula> {
        let mut dependants = Vec::new();

        if let Some(ref dep_node) = self.h.get(&calculate_hash(&f)) {
            dep_node.dependants.iter().for_each(|subdep_id| {
                if let Some(subdep) = self.h.get(&subdep_id) {
                    dependants.push(subdep.f.clone());
                }
            });
        }

        dependants.sort();
        dependants
    }

    pub fn get_all_subformulas(&self) -> Vec<Formula> {
        let mut h = Vec::new();

        for dep in self.h.values() {
            h.push(dep.f.clone());
        }

        h.sort();
        h
    }

    pub fn get_base_formulas(&self) -> Vec<Formula> {
        let mut h = Vec::new();

        self.base_formulas.iter().for_each(|dep_id| {
            if let Some(dep) = self.h.get(&dep_id) {
                h.push(dep.f.clone());
            }
        });

        h.sort();
        h
    }

    pub fn get_policy_formula(&self) -> Formula {
        self.policy.clone()
    }

    /// Set up bidirectional connection between the formula node and its
    /// subformula nodes. The formula node must be in the graph already.
    fn set_up_bidirectional_connection(&mut self, f: &Formula, subformulas: &Vec<Formula>) {
        if !self.contains(&f) {
            return;
        }

        let hash = calculate_hash(&f);

        let mut dependencies = HashSet::new();

        subformulas.into_iter().for_each(|subformula| {
            let subf_hash = calculate_hash(&subformula);

            if let Some(subdep) = self.h.get_mut(&subf_hash) {
                subdep.dependants.insert(hash.clone());
                dependencies.insert(subf_hash);
            } else {
                panic!(
                    "Trying to insert formula {} in the dependency graph,
                           but its subformula {} has not been added yet.",
                    f.to_string(),
                    subformula.to_string()
                );
            }
        });

        self.h.get_mut(&hash).unwrap().dependencies = dependencies;
    }
}

impl fmt::Display for DependencyGraph {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let mut res = String::new();

        self.h.iter().for_each(|(_id, dep)| {
            let f = dep.f.clone();

            res.push_str(&format!("Formula: {}\n", f.to_string()));

            let dependencies = self.get_formula_dependencies(&f);
            if dependencies.len() > 0 {
                res.push_str("    Dependencies:\n");
                dependencies.iter().for_each(|d| {
                    res.push_str(&format!("        {}\n", d.to_string()));
                });
            }

            let dependants = self.get_formula_dependants(&f);
            if dependants.len() > 0 {
                res.push_str("    Dependants:\n");
                dependants.iter().for_each(|d| {
                    res.push_str(&format!("        {}\n", d.to_string()));
                });
            }
        });

        write!(fmt, "{}", res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use constants::{get_diff, test_formula};
    use parser::formula_syntax_tree::*;

    #[test]
    fn insertion_updates_dependants() {
        let lhs = test_formula(Some(vec![4]));
        let rhs = test_formula(Some(vec![5]));

        let f = build_conj(lhs.clone(), rhs.clone());

        let mut graph = DependencyGraph::new(f.clone());

        graph.insert(lhs.clone(), vec![]);
        graph.insert(rhs.clone(), vec![]);
        graph.insert(f.clone(), vec![lhs.clone(), rhs.clone()]);

        let expected = vec![f];

        let lhs_dependants_actual = graph.get_formula_dependants(&lhs);
        let rhs_dependants_actual = graph.get_formula_dependants(&rhs);

        assert_eq!(expected, lhs_dependants_actual);
        assert_eq!(expected, rhs_dependants_actual);
    }

    #[test]
    fn base_formulas() {
        let fst = test_formula(Some(vec![1]));
        let snd = test_formula(Some(vec![2]));
        let trd = build_conj(fst.clone(), snd.clone());

        let mut graph = DependencyGraph::new(trd.clone());
        graph.insert(fst.clone(), vec![]);
        graph.insert(snd.clone(), vec![]);
        graph.insert(trd, vec![fst.clone(), snd.clone()]);

        let expected = vec![fst, snd];
        let actual = graph.get_base_formulas();

        assert_eq!(get_diff(expected, actual), 0);
    }

    #[test]
    fn get_formula_dependencies_test() {
        let fst = test_formula(Some(vec![1]));
        let snd = test_formula(Some(vec![2]));
        let trd = build_conj(fst.clone(), snd.clone());

        let mut graph = DependencyGraph::new(trd.clone());
        graph.insert(fst.clone(), vec![]);
        graph.insert(snd.clone(), vec![]);
        graph.insert(trd.clone(), vec![fst.clone(), snd.clone()]);

        let actual = graph.get_formula_dependencies(&trd);
        let expected = vec![fst, snd];

        assert_eq!(get_diff(expected, actual), 0);
    }
}
