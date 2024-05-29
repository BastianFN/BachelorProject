#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::iter::zip;

use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::Map;

use timely::dataflow::operators::FrontierNotificator;
use timely::dataflow::operators::{Broadcast, Operator};

use timely::dataflow::{Scope, Stream};

use parse_formula;

use constants::calculate_hash;
use dataflow_constructor::types::{
    FlowValues::Data, FlowValues::MetaData, OperatorOptions, Record, TimeFlowValues,
};

use evaluation_plan_generator::evaluation_plan_generator::Expr::*;
use evaluation_plan_generator::evaluation_plan_generator::*;

use parser::formula_syntax_tree::Arg::*;
use parser::formula_syntax_tree::Formula::*;
use parser::formula_syntax_tree::*;

use dataflow_constructor::operators::{
    find_common_bound_variables, find_common_bound_variables1, get_wanted_indices, Operators,
    SupportOperators,
};
use parser::formula_syntax_tree::Constant::Str;

use jq_rs::compile as jq_compile;

type MonitorStream<G> = Stream<G, Record>;
type DataStream<G> = Stream<G, String>;
type TimeStream<G> = Stream<G, TimeFlowValues>;

//TODO change data_stream: Stream<G, String> to Stream<G, Constant>.
struct DataflowConstructor<G: Scope<Timestamp = usize>> {
    data_stream: Stream<G, String>,
    stream_map: HashMap<Expr, (Vec<String>, MonitorStream<G>)>,
    time_stream: Stream<G, TimeFlowValues>,
}

pub fn create_dataflow<G: Scope<Timestamp = usize>>(
    policy: Formula,
    data_stream: DataStream<G>,
    time_stream: TimeStream<G>,
    options: OperatorOptions,
) -> (Vec<String>, MonitorStream<G>) {
    let output_time_steam = time_stream.clone();
    let mut dataflow_constructor = DataflowConstructor {
        data_stream,
        stream_map: HashMap::new(),
        time_stream,
    };

    let mut visitor = 0;
    let plan = generate_evaluation_plan(&policy.clone());
    let optimized_plan = optimize_evaluation_plan(plan.clone());
    dataflow_constructor.create_stream(
        &mut visitor,
        optimized_plan.clone(),
        options.get_deduplication(),
    );

    if dataflow_constructor.stream_exists(plan.clone()) {
        let (tmp_str, tmp_stream) = dataflow_constructor.get_stream(optimized_plan.clone());
        // add exhaust operator to filter all metadata before pushing data to output
        //println!("OUTPUT {:?}", tmp_str);
        return (
            tmp_str.clone(),
            tmp_stream
                .exhaust(&mut 0, output_time_steam.broadcast(), tmp_str, options)
                .1,
        );
    }

    panic!("Error - output stream missing");
}

impl<'a, G: Scope<Timestamp = usize>> DataflowConstructor<G> {
    fn create_base_stream(
        &mut self,
        visitor: &mut usize,
        f: &Formula,
    ) -> (Vec<String>, MonitorStream<G>) {
        *visitor = visitor.clone() + 1;
        let (f_name, args) = new_split_fact(f);
        let exchange = Exchange::new(move |event| calculate_hash(event));

        let simple_mode = is_simple_mode(args.clone());
        let f_vars = fv(args.clone());
        let f_vars_args: Vec<Arg> = args.clone();

        let formula_clone = f.clone();

        let output = if !simple_mode {
            self.data_stream
                .unary_frontier(exchange, "Base Stream", move |_cap, _info| {
                    let mut notifier = FrontierNotificator::new();
                    let mut stash: HashMap<usize, HashSet<String>> = HashMap::new();
                    move |input, output| {
                        while let Some((time, data)) = input.next() {
                            if data.len() == 0 {
                                return;
                            }

                            let tp = time.time().clone();

                            let name_copy = f_name.clone();
                            let f_vars_args_copy = f_vars_args.clone();
                            data.iter().for_each(|d| {
                                match parse_formula(&d) {
                                    Formula::Fact(name, vec) => {
                                        if vec.is_empty() && name == name_copy {
                                            output.session(&time).give(Data(true, vec![]));
                                        } else {
                                            if name == name_copy {
                                                if let Some(mapping) = match_vars(
                                                    f_vars_args_copy.clone(),
                                                    vec.clone(),
                                                ) {
                                                    let mut duplicates: HashSet<Arg> =
                                                        HashSet::new();
                                                    let mut new_vars: Vec<Constant> =
                                                        Vec::with_capacity(vec.len());
                                                    for arg in &f_vars_args_copy {
                                                        match arg {
                                                            Cst(_) => {
                                                                //ignore
                                                            }
                                                            Var(_v) => {
                                                                if let Some(res) = mapping.get(arg)
                                                                {
                                                                    match res {
                                                                        Cst(x) => {
                                                                            if !duplicates
                                                                                .contains(arg)
                                                                            {
                                                                                new_vars.push(
                                                                                    x.clone(),
                                                                                );
                                                                                duplicates.insert(
                                                                                    arg.clone(),
                                                                                );
                                                                            }
                                                                        }
                                                                        _ => {}
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }

                                                    if stash.contains_key(&tp) {
                                                        // println!("stash {:?}, time: {:?}", d, tp);
                                                        if !stash.entry(tp).or_default().contains(d)
                                                        {
                                                            // count_outputs += 1;
                                                            // println!("count");
                                                            stash
                                                                .entry(tp)
                                                                .or_default()
                                                                .insert(d.clone());
                                                            output
                                                                .session(&time)
                                                                .give(Data(true, new_vars));
                                                        }
                                                    } else {
                                                        // count_outputs += 1;
                                                        let mut tmp = HashSet::with_capacity(8);
                                                        tmp.insert(d.clone());
                                                        stash.insert(tp, tmp);
                                                        output
                                                            .session(&time)
                                                            .give(Data(true, new_vars));
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    Eos => {
                                        output.session(&time).give(MetaData(false, false));
                                    }
                                    _ => {}
                                };
                            });
                        }

                        notifier.for_each(&[input.frontier()], |time, _inf| {
                            stash.remove(&time);
                        });
                    }
                })
        } else {
            self.data_stream
                .unary_frontier(exchange, "Base Stream", move |_cap, _info| {
                    let mut notifier = FrontierNotificator::new();
                    let mut stash: HashMap<usize, HashSet<String>> = HashMap::new();
                    move |input, output| {
                        while let Some((time, data)) = input.next() {
                            if data.len() == 0 {
                                return;
                            }

                            let tp = time.time().clone();
                            let name_copy = f_name.clone();

                            data.iter().for_each(|d| {
                                // println!("In operator 2 {:?}", &d);
                                if d == "<eos>" {
                                    output.session(&time).give(MetaData(false, false));
                                } else {
                                    match &formula_clone {
                                        Formula::Fact(name, vec) => {
                                            if vec.is_empty() && name.clone() == name_copy {
                                                output.session(&time).give(Data(true, vec![]));
                                            } else {
                                                if name.clone() == name_copy {
                                                    if stash.contains_key(&tp) {
                                                        if !stash.entry(tp).or_default().contains(d)
                                                        {
                                                            stash
                                                                .entry(tp)
                                                                .or_default()
                                                                .insert(d.clone());
                                                            let mut tmp =
                                                                Vec::with_capacity(vec.len());
                                                            for v in vec {
                                                                match v {
                                                                    Cst(x) => tmp.push(x.clone()),
                                                                    Var(_) => {}
                                                                }
                                                            }
                                                            output
                                                                .session(&time)
                                                                .give(Data(true, tmp));
                                                        }
                                                    } else {
                                                        let mut tmp = HashSet::with_capacity(8);
                                                        tmp.insert(d.clone());
                                                        stash.insert(tp, tmp);
                                                        let mut tmp = Vec::with_capacity(vec.len());
                                                        for v in vec {
                                                            match v {
                                                                Cst(x) => tmp.push(x.clone()),
                                                                Var(_) => {}
                                                            }
                                                        }
                                                        output.session(&time).give(Data(true, tmp));
                                                    }
                                                }
                                            }
                                        }
                                        Eos => {
                                            output.session(&time).give(MetaData(false, false));
                                        }
                                        _ => {}
                                    }
                                }
                            });
                        }

                        notifier.for_each(&[input.frontier()], |time, _inf| {
                            stash.remove(&time);
                        });
                    }
                })
        };

        let plan = generate_evaluation_plan(f);
        self.stream_map
            .insert(plan, (f_vars.clone(), output.clone()));

        (f_vars, output)
    }

    fn create_json_base_stream(
        &mut self,
        visitor: &mut usize,
        query: String,
        aliases: Vec<String>,
    ) -> (Vec<String>, MonitorStream<G>) {
        *visitor = visitor.clone() + 1;
        let exchange = Exchange::new(move |event| calculate_hash(event));

        let plan = Expr::JSONQuery(query.clone(), aliases.clone());

        let output =
            self.data_stream
                .unary_frontier(exchange, "Base Stream", move |_cap, _info| {
                    let mut compiled_query = jq_compile(&query).unwrap();
                    // let new_query = "select(.timestamp == 1714134706 and .length.old == 2927 + 1) | {user: .user, bot: .bot}";
                    // let mut compiled_query = jq_compile(&new_query).unwrap();

                    let mut notifier = FrontierNotificator::new();
                    let mut stash: HashMap<usize, HashSet<Constant>> = HashMap::new();
                    move |input, output| {
                        while let Some((time, data)) = input.next() {
                            if data.len() == 0 {
                                return;
                            }
                            let tp = time.time().clone();
                            data.iter().for_each(|d| {
                                if d == "<eos>" {
                                    output.session(&time).give(MetaData(false, false));
                                } else {
                                    let result = &compiled_query.run(&d).unwrap();
                                    if result.is_empty() {
                                        return;
                                    }
                                    let parsed_result = serde_json::from_str(result);
                                    match parsed_result {
                                        Ok(result) => {
                                            let constant = Constant::JSONValue(result);
                                            if stash.contains_key(&tp) {
                                                if !stash.entry(tp).or_default().contains(&constant)
                                                {
                                                    let mut tmp = Vec::with_capacity(8);
                                                    stash
                                                        .entry(tp)
                                                        .or_default()
                                                        .insert(constant.clone());

                                                    tmp.push(constant);
                                                    output.session(&time).give(Data(true, tmp));
                                                }
                                            } else {
                                                let mut tmp = HashSet::with_capacity(8);
                                                tmp.insert(constant.clone());
                                                stash.insert(tp, tmp);
                                                let mut tmp = Vec::with_capacity(8);
                                                tmp.push(constant);
                                                output.session(&time).give(Data(true, tmp));
                                            }
                                        }
                                        Err(e) => {
                                            println!(
                                                "Error processing JSON query: {}, {}",
                                                e, result
                                            )
                                        }
                                    }
                                }
                            });
                        }
                        notifier.for_each(&[input.frontier()], |time, _inf| {
                            stash.remove(&time);
                        });
                    }
                });

        self.stream_map
            .insert(plan, (aliases.clone(), output.clone()));

        (aliases, output)
    }

    fn create_let_stream(
        &mut self,
        visitor: &mut usize,
        f: &Formula,
        pred_args: &mut Vec<Arg>,
        alpha_attrs: &Vec<String>,
        stream: MonitorStream<G>,
    ) -> MonitorStream<G> {
        *visitor = visitor.clone() + 1;
        let (_, mut f_args) = split_f(f.clone());

        let indices = get_reordered_indices(pred_args, alpha_attrs);
        f_args = indices.into_iter().map(|i| f_args[i].clone()).collect();

        let flat_map_output = stream.flat_map(move |rec| match rec.clone() {
            Data(t, tuple) => {
                let d_vars_assignment = assign_d_vars(f_args.clone(), tuple);
                return match d_vars_assignment {
                    Some(d_vars) => Some(Data(t, d_vars)),
                    None => None,
                };
            }
            _ => Some(rec),
        });
        flat_map_output
    }

    fn create_true_stream(&mut self, visitor: &mut usize) -> (Vec<String>, MonitorStream<G>) {
        *visitor = visitor.clone() + 1;

        let output = self
            .time_stream
            .unary(Pipeline, "True Stream", move |_cap, _info| {
                move |input, output| {
                    while let Some((time, data)) = input.next() {
                        data.iter().for_each(|tfv| match tfv {
                            TimeFlowValues::Timestamp(_) => {
                                output.session(&time).give(Data(true, vec![]));
                            }
                            TimeFlowValues::EOS => {
                                output.session(&time).give(MetaData(false, false))
                            }
                        });
                    }
                }
            });
        self.stream_map
            .insert(FULL, (vec![].clone(), output.clone()));
        (vec![], output)
    }

    fn create_value_stream(
        &mut self,
        visitor: &mut usize,
        f: String,
        value: Arg,
    ) -> (Vec<String>, MonitorStream<G>) {
        *visitor = visitor.clone() + 1;
        let f_vars = vec![f.clone()];

        let val = match value {
            Cst(x) => x,
            Var(x) => Str(x),
        };

        let output = self
            .time_stream
            .unary(Pipeline, "Fixed Value Stream", move |_cap, _info| {
                move |input, output| {
                    while let Some((time, data)) = input.next() {
                        data.iter().for_each(|tfv| match tfv {
                            TimeFlowValues::Timestamp(_) => {
                                output.session(&time).give(Data(true, vec![val.clone()]));
                            }
                            TimeFlowValues::EOS => {
                                output.session(&time).give(MetaData(false, false))
                            }
                        });
                    }
                }
            });

        let plan = generate_evaluation_plan(&parse_formula("VALUE"));
        self.stream_map
            .insert(plan, (f_vars.clone(), output.clone()));

        (f_vars, output)
    }

    fn create_false_stream(&mut self, visitor: &mut usize) -> (Vec<String>, MonitorStream<G>) {
        *visitor = visitor.clone() + 1;

        let output = self
            .time_stream
            .unary(Pipeline, "False Stream", move |_cap, _info| {
                move |input, output| {
                    while let Some((time, data)) = input.next() {
                        data.iter().for_each(|tfv| match tfv {
                            TimeFlowValues::EOS => {
                                output.session(&time).give(MetaData(false, false))
                            }
                            _ => {}
                        });
                    }
                }
            });
        self.stream_map
            .insert(EMPTY, (vec![].clone(), output.clone()));
        (vec![], output)
    }

    fn create_stream(&mut self, visitor: &mut usize, plan: Expr, dedup: bool) {
        self.create_stream_from_evaluation_plan(
            visitor,
            plan,
            dedup,
            &mut HashMap::new(),
            &mut HashMap::new(),
        );
    }

    //fn create_stream_from_evaluation_plan(&mut self, visitor: &mut usize, plan: Expr, dedup: bool, ) -> (Vec<String>, MonitorStream<G>) {
    fn create_stream_from_evaluation_plan(
        &mut self,
        visitor: &mut usize,
        plan: Expr,
        dedup: bool,
        let_stream_map: &mut HashMap<(String, Vec<String>), MonitorStream<G>>,
        let_attrs_map: &mut HashMap<(String, Vec<String>), Vec<Arg>>,
    ) -> (Vec<String>, MonitorStream<G>) {
        if self.stream_map.contains_key(&plan) {
            return self.stream_map.get(&plan).unwrap().clone();
        }
        let out = match plan.clone() {
            FULL => self.create_true_stream(visitor),
            EMPTY => self.create_false_stream(visitor),
            VarEquals(var, arg) => self.create_value_stream(visitor, var.clone(), arg),
            Expr::Equals(var, arg) => self.create_value_stream(visitor, var.clone(), *arg),
            Expr::Fact(f_name, f_args) => {
                let f = Formula::Fact(f_name.clone(), f_args.clone());
                let new_attrs = get_attributes(plan.clone(), let_attrs_map);
                for (pred_name, alpha_attrs) in let_stream_map.keys() {
                    if &f_name == pred_name && f_args.len() == alpha_attrs.len() {
                        let let_stream = let_stream_map
                            .get(&(pred_name.clone(), alpha_attrs.clone()))
                            .unwrap()
                            .clone();

                        let pred_args = let_attrs_map
                            .get_mut(&(pred_name.to_string(), alpha_attrs.to_vec()))
                            .unwrap();
                        let stream = self.create_let_stream(
                            visitor,
                            &f,
                            pred_args,
                            &alpha_attrs,
                            let_stream,
                        );

                        return (new_attrs, stream);
                    }
                }
                let (_, stream) = self.create_base_stream(visitor, &f);
                (new_attrs, stream)
            }
            Expr::JSONQuery(query, aliases) => {
                let (_, stream) = self.create_json_base_stream(visitor, query, aliases.clone());
                (aliases, stream)
            }
            Join(e1, e2) => {
                let (at, lhs) = self.create_stream_from_evaluation_plan(
                    visitor,
                    *e1.clone(),
                    dedup,
                    let_stream_map,
                    let_attrs_map,
                );
                let (at1, rhs) = self.create_stream_from_evaluation_plan(
                    visitor,
                    *e2.clone(),
                    dedup,
                    let_stream_map,
                    let_attrs_map,
                );
                (
                    merge_variables_string(at.clone(), at1.clone()),
                    lhs.join(visitor, &rhs, &at, &at1).1,
                )
            }
            Expr::Next(e1, interval) => {
                let (at, stream) = self.create_stream_from_evaluation_plan(
                    visitor,
                    *e1.clone(),
                    dedup,
                    let_stream_map,
                    let_attrs_map,
                );
                //let attrs = get_attributes(*e1, let_attrs_map);
                if interval.get_start() == Some(0) && interval.is_infinite() {
                    stream
                        .distribute(&mut 0, true, &at, &vec![])
                        .1
                        .next_zero_inf(visitor, &at)
                } else {
                    (
                        at.clone(),
                        stream
                            .distribute(&mut 0, false, &vec![], &at)
                            .1
                            .next(visitor, &self.time_stream.broadcast(), &at, interval)
                            .1,
                    )
                }
            }
            Expr::Prev(e1, interval) => {
                let (at, stream) = self.create_stream_from_evaluation_plan(
                    visitor,
                    *e1.clone(),
                    dedup,
                    let_stream_map,
                    let_attrs_map,
                );
                if interval.get_start() == Some(0) && interval.is_infinite() {
                    (
                        at.clone(),
                        stream
                            .distribute(&mut 0, true, &at, &vec![])
                            .1
                            .prev_zero_inf(visitor, &self.time_stream.broadcast(), &at)
                            .1,
                    )
                } else {
                    (
                        at.clone(),
                        stream
                            .distribute(&mut 0, false, &vec![], &at)
                            .1
                            .prev(visitor, &self.time_stream.broadcast(), &at, interval)
                            .1,
                    )
                }
            }
            Expr::Once(e1, interval) => {
                let (at, stream) = self.create_stream_from_evaluation_plan(
                    visitor,
                    *e1.clone(),
                    dedup,
                    let_stream_map,
                    let_attrs_map,
                );
                if interval.get_start() == Some(0) && interval.is_infinite() {
                    (
                        at.clone(),
                        stream
                            .distribute(&mut 0, false, &vec![], &at)
                            .1
                            .once_zero_inf(visitor, &self.time_stream.broadcast(), &at)
                            .1,
                    )
                } else {
                    (
                        at.clone(),
                        stream
                            .distribute(&mut 0, false, &vec![], &at)
                            .1
                            .once(visitor, &self.time_stream.broadcast(), &at, interval, dedup)
                            .1,
                    )
                }
            }
            Expr::Eventually(e1, interval) => {
                let (at, stream) = self.create_stream_from_evaluation_plan(
                    visitor,
                    *e1.clone(),
                    dedup,
                    let_stream_map,
                    let_attrs_map,
                );
                if interval.get_start() == Some(0) && interval.is_infinite() {
                    (
                        at.clone(),
                        stream
                            .distribute(&mut 0, false, &vec![], &at)
                            .1
                            .eventually_zero_inf(visitor, &at)
                            .1,
                    )
                } else {
                    (
                        at.clone(),
                        stream
                            .distribute(&mut 0, false, &vec![], &at)
                            .1
                            .eventually(
                                visitor,
                                &self.time_stream.broadcast(),
                                &at,
                                interval,
                                dedup,
                            )
                            .1,
                    )
                }
            }
            Expr::Since(e1, e2, interval) => {
                let (at, lhs) = self.create_stream_from_evaluation_plan(
                    visitor,
                    *e1.clone(),
                    dedup,
                    let_stream_map,
                    let_attrs_map,
                );
                let (at1, rhs) = self.create_stream_from_evaluation_plan(
                    visitor,
                    *e2.clone(),
                    dedup,
                    let_stream_map,
                    let_attrs_map,
                );

                lhs.distribute(&mut 0, true, &at, &at1).1.since(
                    visitor,
                    rhs.distribute(&mut 0, false, &at, &at1).1,
                    self.time_stream.broadcast().clone(),
                    &at,
                    &at1,
                    interval,
                    dedup,
                )
            }
            Expr::Until(e1, e2, interval) => {
                let (at, lhs) = self.create_stream_from_evaluation_plan(
                    visitor,
                    *e1.clone(),
                    dedup,
                    let_stream_map,
                    let_attrs_map,
                );
                let (at1, rhs) = self.create_stream_from_evaluation_plan(
                    visitor,
                    *e2.clone(),
                    dedup,
                    let_stream_map,
                    let_attrs_map,
                );
                lhs.distribute(&mut 0, true, &at, &at1).1.until(
                    visitor,
                    rhs.distribute(&mut 0, false, &at, &at1).1,
                    self.time_stream.broadcast().clone(),
                    &at,
                    &at1,
                    interval,
                    dedup,
                )
            }
            Expr::NegSince(e1, e2, interval) => {
                let (at, lhs) = self.create_stream_from_evaluation_plan(
                    visitor,
                    *e1.clone(),
                    dedup,
                    let_stream_map,
                    let_attrs_map,
                );
                let (at1, rhs) = self.create_stream_from_evaluation_plan(
                    visitor,
                    *e2.clone(),
                    dedup,
                    let_stream_map,
                    let_attrs_map,
                );
                lhs.distribute(&mut 0, true, &at, &at1).1.neg_since(
                    visitor,
                    rhs.distribute(&mut 0, false, &at, &at1).1,
                    self.time_stream.broadcast().clone(),
                    &at,
                    &at1,
                    interval,
                    dedup,
                )
            }
            Expr::NegUntil(e1, e2, interval) => {
                let (at, lhs) = self.create_stream_from_evaluation_plan(
                    visitor,
                    *e1.clone(),
                    dedup,
                    let_stream_map,
                    let_attrs_map,
                );
                let (at1, rhs) = self.create_stream_from_evaluation_plan(
                    visitor,
                    *e2.clone(),
                    dedup,
                    let_stream_map,
                    let_attrs_map,
                );
                lhs.distribute(&mut 0, true, &at, &at1).1.neg_until(
                    visitor,
                    rhs.distribute(&mut 0, false, &at, &at1).1,
                    self.time_stream.broadcast().clone(),
                    &at,
                    &at1,
                    interval,
                    dedup,
                )
            }
            UnionJoin(e1, e2) => {
                let (at, lhs) = self.create_stream_from_evaluation_plan(
                    visitor,
                    *e1.clone(),
                    dedup,
                    let_stream_map,
                    let_attrs_map,
                );
                let (at1, rhs) = self.create_stream_from_evaluation_plan(
                    visitor,
                    *e2.clone(),
                    dedup,
                    let_stream_map,
                    let_attrs_map,
                );
                (at.clone(), lhs.union(visitor, &rhs, &at, &at1).1)
            }
            Antijoin(e1, e2) => {
                let (at, lhs) = self.create_stream_from_evaluation_plan(
                    visitor,
                    *e1.clone(),
                    dedup,
                    let_stream_map,
                    let_attrs_map,
                );
                let (at1, rhs) = self.create_stream_from_evaluation_plan(
                    visitor,
                    *e2.clone(),
                    dedup,
                    let_stream_map,
                    let_attrs_map,
                );
                lhs.anti_join(visitor, &rhs, &at, &at1)
            }
            Project(vars, expr) => {
                let (at, stream) = self.create_stream_from_evaluation_plan(
                    visitor,
                    *expr.clone(),
                    dedup,
                    let_stream_map,
                    let_attrs_map,
                );
                stream.projection(visitor, &at, &vars)
            }
            Extend(var1, var2, expr) => {
                let (at, stream) = self.create_stream_from_evaluation_plan(
                    visitor,
                    *expr.clone(),
                    dedup,
                    let_stream_map,
                    let_attrs_map,
                );
                stream.equality(visitor, &at, &var1, &var2)
            }
            Filter(var, val, expr) => {
                let (at, stream) = self.create_stream_from_evaluation_plan(
                    visitor,
                    *expr.clone(),
                    dedup,
                    let_stream_map,
                    let_attrs_map,
                );
                stream.filter(visitor, at.clone(), var, val)
            }
            NegFilter(var, val, expr) => {
                let (at, stream) = self.create_stream_from_evaluation_plan(
                    visitor,
                    *expr.clone(),
                    dedup,
                    let_stream_map,
                    let_attrs_map,
                );
                stream.neg_filter(visitor, at.clone(), var, val)
            }
            Error(m) => {
                eprintln!("{:?}", m);
                panic!();
            }
            _ => panic!("Evaluation operator not recognised: {:?}", plan),
        };

        self.stream_map.insert(plan, out.clone());
        out
    }

    fn stream_exists(&self, plan: Expr) -> bool {
        self.stream_map.contains_key(&plan)
    }

    fn get_stream(&self, plan: Expr) -> (Vec<String>, MonitorStream<G>) {
        if !self.stream_exists(plan.clone()) {
            panic!(
                "Attempting to use an uninitialised stream for {:?}",
                plan.clone()
            )
        }
        self.stream_map.get(&plan.clone()).unwrap().clone()
    }
}

fn fv(args: Vec<Arg>) -> Vec<String> {
    let mut free_vars = Vec::with_capacity(args.len());
    for arg in args {
        match arg {
            Cst(_) => {}
            Var(x) => free_vars.push(x),
        }
    }

    return free_vars.clone();
}

//True for Predicate without constants and without duplicates
fn is_simple_mode(args: Vec<Arg>) -> bool {
    let mut duplicates = HashSet::with_capacity(args.len());
    let mut no_duplicate = true;
    let mut no_constant = true;

    for arg in args {
        match arg {
            Cst(_) => no_constant = false,
            Var(x) => {
                if duplicates.contains(&x) {
                    no_duplicate = false;
                } else {
                    duplicates.insert(x);
                }
            }
        }
    }

    return no_duplicate && no_constant;
}

fn new_split_fact(f: &Formula) -> (String, Vec<Arg>) {
    let mut name = String::new();
    let mut args = Vec::new();

    match f.clone() {
        Formula::Fact(n, v) => {
            name = n;
            args = v;
        }
        _ => (),
    }

    (name, args)
}

// match [] [] = Some (|x| none)
// match (Cst c # ts) (d # ds) = if c=d then match ts ds else None
// match (Var x # ts) (d # ds) = case match ts ds of None =>
//          None
//          | Some G => case G x of
//                      None => x := Some d
//                      | Some v => if v=d then Some G else None

fn match_vars(ts: Vec<Arg>, ds: Vec<Arg>) -> Option<HashMap<Arg, Arg>> {
    let mut tmp: HashMap<Arg, Arg> = HashMap::with_capacity(ds.len());
    return match match_vars_internal_iter(ts, ds, &mut tmp) {
        None => None,
        Some(res) => Some(res.clone()),
    };
}

fn match_vars_internal_iter(
    ts: Vec<Arg>,
    ds: Vec<Arg>,
    mapping: &mut HashMap<Arg, Arg>,
) -> Option<&mut HashMap<Arg, Arg>> {
    for i in 0..ds.len() {
        let t = ts[i].clone();
        let d = ds[i].clone();

        match t {
            Var(xt) => match mapping.get(&Var(xt.clone())) {
                None => {
                    mapping.insert(Var(xt), d);
                }
                Some(v) => {
                    if v.clone() != d {
                        return None;
                    }
                }
            },
            Cst(_) => {
                if t.clone() != d {
                    return None;
                }
            }
        }
    }

    return Some(mapping);
}

fn assign_d_vars(f_args: Vec<Arg>, d_args: Vec<Constant>) -> Option<Vec<Constant>> {
    let mut assignment: HashMap<String, Constant> = HashMap::new();
    let mut new_d_vars = vec![];
    for (f_arg, d_arg) in zip(f_args.iter(), d_args.iter()) {
        match f_arg {
            Var(var_name) => match assignment.get(var_name) {
                None => {
                    assignment.insert(var_name.to_string(), d_arg.clone());
                    new_d_vars.push(d_arg.clone())
                }
                Some(v) => {
                    if v != d_arg {
                        return None;
                    }
                }
            },
            Cst(v) => {
                if v != d_arg {
                    return None;
                }
            }
        }
    }

    Some(new_d_vars)
}

fn get_f_vars(f_args: Vec<Arg>) -> Vec<String> {
    let mut f_vars = vec![];
    for arg in f_args {
        if let Var(a) = arg {
            f_vars.push(a)
        }
    }
    f_vars
}

fn get_reordered_indices(pred_args: &mut Vec<Arg>, alpha_attrs: &Vec<String>) -> Vec<usize> {
    let mut indices = vec![];
    let alpha_args: Vec<Arg> = alpha_attrs
        .iter()
        .map(|attr| Var(attr.to_string()))
        .collect();
    for arg in alpha_args {
        let index = pred_args.iter().position(|a| arg == *a).unwrap();
        indices.push(index)
    }
    return indices;
}

fn get_attributes(
    e: Expr,
    let_attrs_map: &mut HashMap<(String, Vec<String>), Vec<Arg>>,
) -> Vec<String> {
    let mut attributes = vec![];
    match e.clone() {
        EMPTY | FULL => return attributes,
        VarEquals(var, y) => {
            let_attrs_map.insert((var.clone(), vec![var.clone()]), vec![y]);
            attributes.push(var);
            return attributes;
        }
        Expr::Fact(f_name, f_args) => {
            let f_vars = get_f_vars(f_args.clone());

            for ((pred_name, alpha_attrs), pred_args) in let_attrs_map {
                if &f_name == pred_name && &f_args.len() == &alpha_attrs.len() {
                    let indices = get_reordered_indices(pred_args, alpha_attrs);

                    let mut res_vars = vec![];
                    // if no f_vars, simply return empty vec
                    // need to ensure the correct order of returned f_vars/attributes
                    // eg if in beta we have pred S(x,y,z) but in the definition of the pred the attributes appear in different order, eg S(x,y,z) = B(y,z)&&C(z,x), we have that the incoming f_vars are ["x","y", "z"] which should be changed to ["y", "z", "x"]
                    if f_vars.len() > 0 {
                        for i in indices {
                            let arg = f_args[i].clone();
                            if let Var(a) = arg {
                                res_vars.push(a);
                            }
                        }
                    }

                    let mut seen: HashMap<String, bool> = HashMap::new();
                    res_vars.retain(|x| seen.insert(x.to_string(), true).is_none());

                    return res_vars;
                }
            }

            let f_vars = fv(f_args.clone());
            for var in f_vars {
                if !attributes.contains(&var) {
                    attributes.push(var)
                }
            }
            attributes
        }
        Join(e1, e2) => {
            let lhs_attrs = get_attributes(*e1, let_attrs_map);
            let rhs_attrs = get_attributes(*e2, let_attrs_map);
            let (attrs, _, _) = find_common_bound_variables1(lhs_attrs, rhs_attrs);
            return attrs;
        }
        UnionJoin(e1, e2) | Antijoin(e1, e2) => {
            let lhs_attrs = get_attributes(*e1, let_attrs_map);
            let rhs_attrs = get_attributes(*e2, let_attrs_map);
            let (attrs, _, _) = find_common_bound_variables(lhs_attrs, rhs_attrs);
            return attrs;
        }
        Project(var, expr) => {
            let attrs = get_attributes(*expr, let_attrs_map);
            let (_, new_attrs) = get_wanted_indices(&attrs, &var);
            new_attrs
        }
        Extend(_, var, e) => {
            let attrs = get_attributes(*e, let_attrs_map);
            let mut new_attrs = attrs.clone();
            new_attrs.push(var.clone());
            new_attrs
        }
        Filter(_, _, expr)
        | NegFilter(_, _, expr)
        | Expr::Next(expr, _)
        | Expr::Prev(expr, _)
        | Expr::Once(expr, _)
        | Expr::Eventually(expr, _) => return get_attributes(*expr, let_attrs_map),
        Expr::Since(_, expr, _) | Expr::Until(_, expr, _) => {
            let rhs_attrs = get_attributes(*expr, let_attrs_map);
            rhs_attrs
        }
        /*Expr::Aggregation(var, agg_f , term, group_by, expr) => {
            let attrs = get_attributes(*expr, let_attrs_map);
            let group_by_indices = get_group_by_indices(&attrs, &group_by);
            let mut new_attrs = split_keys(attrs, &group_by_indices).0;
            new_attrs.insert(0, var);
            new_attrs
        }
        Expr::LetSimple(pred, pred_args, alpha, beta)
        | Expr::LetPast(pred, pred_args, alpha, beta) => {
            let alpha_attrs = get_attributes(*alpha, let_attrs_map);
            let_attrs_map.insert((pred, alpha_attrs), pred_args);
            let beta_attrs = get_attributes(*beta, let_attrs_map);
            beta_attrs
        }*/
        _ => return attributes,
    }
}

fn split_f(f: Formula) -> (String, Vec<Arg>) {
    let mut f_name = String::new();
    let mut f_args = vec![];
    match f {
        Formula::Fact(n, args) => {
            f_name = n;
            f_args = args;
        }
        _ => (),
    }
    (f_name, f_args)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use timely;
    use timely::dataflow::operators::capture::extract::Extract;
    use timely::dataflow::operators::Capture;
    use timely::dataflow::operators::Input;
    use timely::dataflow::operators::{Probe, UnorderedInput};

    use dataflow_constructor::types::default_options;
    use dataflow_constructor::types::TimeFlowValues::{Timestamp, EOS};
    use TS;

    use parser::formula_syntax_tree::Constant::{Int, Str};
    use timeunits::TimeInterval;

    const NUM_WORKERS: usize = 2;

    #[test]
    fn inspect_expression() {
        let policy = "(P0(x2,x1) AND (NOT ((NOT P1()) UNTIL[0,21) ((P2() AND (NOT ((33 = 13) AND (NOT P1())))) AND (NOT (25 = 0))))))".to_string();
        let times = vec![(0, 0), (1, 1), (2, 2)];

        let data = vec![vec![]];

        println!("{:?}", parse_formula(&policy));
        let expected = vec![];
        test_dataflow_unordered(policy, data.clone(), times.clone(), expected);
    }

    #[test]
    fn rand_formula() {
        let times = vec![(0, 0), (1, 1), (2, 2)];
        let policy = "(NOT (x2 = 24)) UNTIL(0,2) P1(x1,x2)".to_string();

        let data = vec![
            vec!["P3()"],
            vec!["P1(1,1)"],
            vec!["P1(1,2)", "P1(1,24)", "P1(1,23)"],
        ];

        println!("{:?}", parse_formula(&policy));
        let expected = vec![
            (0, vec![Data(true, vec![Int(1), Int(1)])]),
            (
                1,
                vec![
                    Data(true, vec![Int(1), Int(2)]),
                    Data(true, vec![Int(1), Int(23)]),
                ],
            ),
        ];
        test_dataflow_unordered(policy, data.clone(), times.clone(), expected);
    }

    #[test]
    fn negated_equals() {
        //let times = vec![(0, 0), (1, 1), (2,2)];
        let policy = "((NOT (NEXT(0,434] (((NOT (42 = 23)) UNTIL[0,312] (ONCE(0,*) (ONCE[0,*) (EVENTUALLY[0,23) ((NOT (ONCE(0,*) ((EXISTS y1. (((NOT ((EVENTUALLY[0,25) P0(y1)) AND P1(y1))) SINCE(0,*) P0(y1)) AND ((NOT P2(y1)) UNTIL[0,178) (y1 = 6)))) AND ((21 = 3) SINCE[0,*) (PREVIOUS[0,*) ((PREVIOUS(0,40) P3()) AND (NOT (15 = 30)))))))) SINCE[0,408] ((EVENTUALLY(0,478] P4()) AND (P3() AND (NOT (15 = 32))))))))) AND (ONCE[0,133] (ONCE[0,425] (NEXT(0,73] P4())))))) SINCE[0,94] (NEXT[0,292) (P5(x1,x3,x4,x2) OR (P6(x4,x3,x1) SINCE[0,*) (P7(x1,x3) SINCE[0,*) P8(x4,x3,x1,x2))))))".to_string();

        println!("{:?}", parse_formula(&policy));

        let times = vec![(0, 0), (1, 1), (2, 2), (3, 3)];
        let policy = "(NOT (15 = 30)) SINCE(0,*) q(x,y)".to_string();

        let data = vec![vec!["p()"], vec!["q(1,1)"], vec!["p()"]];

        println!("{:?}", parse_formula(&policy));
        let expected = vec![(2, vec![Data(true, vec![Int(1), Int(1)])])];
        test_dataflow(policy, data.clone(), times.clone(), expected);

        let times = vec![(0, 0), (1, 1), (2, 2), (3, 3)];
        let policy = "(P3() AND (NOT (15 = 32)))".to_string();

        let data = vec![vec!["P3()"], vec!["P3()"], vec!["P3()"]];

        println!("{:?}", parse_formula(&policy));
        let expected = vec![
            (0, vec![Data(true, vec![])]),
            (1, vec![Data(true, vec![])]),
            (2, vec![Data(true, vec![])]),
        ];
        test_dataflow(policy, data.clone(), times.clone(), expected);
    }

    #[test]
    fn base_stream_test() {
        let data = vec![
            vec!["p(4, 3)"],
            vec!["p(6, 5)"],
            vec!["p(8, 7)"],
            vec!["p(5, 3)"],
        ];

        let times = vec![(0, 0), (1, 1), (2, 2), (3, 3), (4, 4)];
        let policy = "p(x, x)".to_string();

        println!("{}", parse_formula(&policy));

        let expected = vec![];

        test_dataflow(policy, data.clone(), times.clone(), expected);
    }

    #[test]
    fn base_stream_test1() {
        let data = vec![
            vec!["p(4, 4)"],
            vec!["p(6, 5)"],
            vec!["p(8, 7)"],
            vec!["p(5, 5)"],
        ];

        let times = vec![(0, 0), (1, 1), (2, 2), (3, 3), (4, 4)];

        let policy = "p(x, x)".to_string();

        println!("{}", parse_formula(&policy));

        let expected = vec![
            (0, vec![Data(true, vec![Int(4)])]),
            (3, vec![Data(true, vec![Int(5)])]),
        ];

        test_dataflow(policy, data.clone(), times.clone(), expected);
    }

    #[test]
    fn base_stream_test2() {
        let data = vec![
            vec!["p(4, 5, 4)"],
            vec!["p(6, 5, 6)"],
            vec!["p(7, 5, 8)"],
            vec!["p(5, 5, 4)"],
        ];

        let times = vec![(0, 0), (1, 1), (2, 2), (3, 3)];

        let policy = "p(x, 5, x)".to_string();

        println!("{:?}", parse_formula(&policy));

        let expected = vec![
            (0, vec![Data(true, vec![Int(4)])]),
            (1, vec![Data(true, vec![Int(6)])]),
        ];

        test_dataflow(policy, data.clone(), times.clone(), expected);
    }

    #[test]
    fn filter_test_value() {
        let data = vec![
            vec!["p(4, 3)"],
            vec!["p(6, 5)"],
            vec!["p(8, 7)"],
            vec!["p(5, 3)"],
        ];

        let times = vec![(0, 0), (1, 1), (2, 2), (3, 3), (4, 4)];

        let policy = "p(y, x) AND (x = 3)".to_string();

        println!("{}", parse_formula(&policy));

        let expected = vec![
            (0, vec![Data(true, vec![(Int(4)), (Int(3))])]),
            (3, vec![Data(true, vec![(Int(5)), (Int(3))])]),
        ];

        test_dataflow(policy, data.clone(), times.clone(), expected);

        /*let policy = "p(y, x) AND NOT (x=3)".to_string();

        //println!("{}", parse_formula(&policy));

        let expected = vec![
            (1, vec![Data(true, vec![(Int(6)), (Int(5))])]),
            (2, vec![Data(true, vec![(Int(8)), (Int(7))])]),
        ];

        //test_dataflow(policy, data, times, expected);*/
    }

    #[test]
    fn exists_test() {
        let data = vec![
            vec!["p(4, 3)"],
            vec!["p(6, 5)"],
            vec!["p(8, 7)"],
            vec!["p(5, 3)"],
        ];

        let times = vec![(0, 0), (1, 1), (2, 2), (3, 3), (4, 4)];

        let policy = "EXISTSy.p(y, x)".to_string();

        println!("{}", parse_formula(&policy));
        let expected = vec![
            (0, vec![Data(true, vec![(Int(3))])]),
            (1, vec![Data(true, vec![(Int(5))])]),
            (2, vec![Data(true, vec![(Int(7))])]),
            (3, vec![Data(true, vec![(Int(3))])]),
        ];

        test_dataflow(policy, data.clone(), times.clone(), expected);

        let data = vec![
            vec!["p(1, 4, 3)"],
            vec!["p(1, 6, 5)"],
            vec!["p(1, 8, 7)"],
            vec!["p(1, 5, 3)"],
        ];

        let times = vec![(0, 0), (1, 1), (2, 2), (3, 3), (4, 4)];

        let policy = "EXISTSy,x.p(z,y, x)".to_string();

        println!("{}", parse_formula(&policy));
        let expected = vec![
            (0, vec![Data(true, vec![(Int(1))])]),
            (1, vec![Data(true, vec![(Int(1))])]),
            (2, vec![Data(true, vec![(Int(1))])]),
            (3, vec![Data(true, vec![(Int(1))])]),
        ];

        test_dataflow(policy, data.clone(), times.clone(), expected);
    }

    #[test]
    fn equals_test() {
        let data = vec![
            vec!["p(4, 3)"],
            vec!["p(6, 5)"],
            vec!["p(8, 7)"],
            vec!["p(5, 3)"],
        ];

        let times = vec![(0, 0), (1, 1), (2, 2), (3, 3), (4, 4)];

        let policy = "p(x,y) AND (x=5)".to_string();

        println!("{}", parse_formula(&policy));
        let expected = vec![
            //(2, vec![Data(true, vec![Int(8), Int(7)])]),
            (3, vec![Data(true, vec![Int(5), Int(3)])]),
        ];

        test_dataflow(policy, data.clone(), times.clone(), expected);

        /*let data = vec![
            vec!["p(4, 3)"],
            vec!["p(6, 5)"],
            vec!["p(8, 7)"],
            vec!["p(5, 3)"],
        ];

        let times = vec![(0, 0), (1, 1), (2, 2), (3, 3), (4,4)];

        let policy = "p(x,y) AND NOT (x=5)".to_string();

        //println!("{}", parse_formula(&policy));
        let expected = vec![
            (0, vec![Data(true, vec![Int(4), Int(3)])]),
            (1, vec![Data(true, vec![Int(6), Int(5)])]),
            (2, vec![Data(true, vec![Int(8), Int(7)])]),
        ];*/

        //test_dataflow(policy, data.clone(), times.clone(), expected);
    }

    #[test]
    fn neg_temp_test() {
        let data = vec![
            vec!["p(4)", "q(5)"],
            vec!["p(6)", "q(8)"],
            vec!["p(8)"],
            vec!["p(5)"],
        ];

        let times = vec![(0, 0), (1, 1), (2, 2), (3, 3), (4, 4)];
        let policy = "NOT p(x) SINCE [0,*) q(x)".to_string();
        println!("{:?}", parse_formula(&policy.clone()));
        let expected = vec![
            (0, vec![Data(true, vec![Int(5)])]),
            (1, vec![Data(true, vec![Int(5)]), Data(true, vec![Int(8)])]),
            (2, vec![Data(true, vec![Int(5)])]),
        ];
        test_dataflow_unordered(policy, data, times, expected);
    }

    #[test]
    fn random_formula_3() {
        let data = vec![vec!["P3()"], vec!["P3()"], vec!["P3()"]];

        let times = vec![(0, 0), (1, 1), (2, 2)];
        let policy = "(PREVIOUS(0,40) P3()) AND (NOT (15 = 30))".to_string();
        let expected = vec![(1, vec![Data(true, vec![])]), (2, vec![Data(true, vec![])])];
        test_dataflow_unordered(policy, data.clone(), times, expected);

        let times = vec![(0, 0), (1, 1), (2, 2), (3, 3)];
        let policy = "(PREVIOUS[0,*) ((PREVIOUS(0,40) P3()) AND (NOT (15 = 30))))".to_string();
        let expected = vec![(2, vec![Data(true, vec![])]), (3, vec![Data(true, vec![])])];
        test_dataflow_unordered(policy, data, times, expected);
    }

    #[test]
    fn random_formula_3_1() {
        let data = vec![vec!["P3()"], vec!["P3()"], vec!["P3()"]];

        let times = vec![(0, 0), (1, 1), (2, 2)];
        let policy = "P3() AND (NOT (15 = 30))".to_string();
        let expected = vec![
            (0, vec![Data(true, vec![])]),
            (1, vec![Data(true, vec![])]),
            (2, vec![Data(true, vec![])]),
        ];
        test_dataflow_unordered(policy, data.clone(), times, expected);
    }

    #[test]
    fn random_formula_3_2() {
        let data = vec![
            vec!["P7(1,1)", "P8(1,1,1,1)", "P5(1,1,1,1)", "P6(1,1,1)"],
            vec![
                "P7(1,1)",
                "P8(1,1,1,1)",
                "P5(1,1,1,1)",
                "P6(1,1,1)",
                "P7(2,2)",
                "P8(2,2,2,2)",
                "P5(2,2,1,2)",
                "P6(2,1,1)",
            ],
            vec!["P5(1,1,1,1)", "P5(2,2,2,2)", "P5(3,3,3,3)"],
        ];

        let times = vec![(0, 0), (1, 1), (2, 2), (3, 3)];
        let policy = "(P5(x1,x3,x4,x2) OR ( P6(x4,x3,x1) SINCE[0,*) (P7(x1,x3) SINCE[0,*) P8(x4,x3,x1,x2))))".to_string();
        let expected = vec![
            (0, vec![Data(true, vec![Int(1), Int(1), Int(1), Int(1)])]),
            (
                1,
                vec![
                    Data(true, vec![Int(1), Int(1), Int(1), Int(1)]),
                    Data(true, vec![Int(2), Int(2), Int(1), Int(2)]),
                    Data(true, vec![Int(2), Int(2), Int(2), Int(2)]),
                ],
            ),
            (
                2,
                vec![
                    Data(true, vec![Int(1), Int(1), Int(1), Int(1)]),
                    Data(true, vec![Int(2), Int(2), Int(2), Int(2)]),
                    Data(true, vec![Int(3), Int(3), Int(3), Int(3)]),
                ],
            ),
        ];
        test_dataflow_unordered(policy, data.clone(), times.clone(), expected);

        let policy = "(NEXT[0,292) P5(x1,x3,x4,x2))".to_string();
        let expected = vec![
            (
                0,
                vec![
                    Data(true, vec![Int(1), Int(1), Int(1), Int(1)]),
                    Data(true, vec![Int(2), Int(2), Int(1), Int(2)]),
                ],
            ),
            (
                1,
                vec![
                    Data(true, vec![Int(1), Int(1), Int(1), Int(1)]),
                    Data(true, vec![Int(2), Int(2), Int(2), Int(2)]),
                    Data(true, vec![Int(3), Int(3), Int(3), Int(3)]),
                ],
            ),
        ];
        test_dataflow_unordered(policy, data.clone(), times, expected);

        let times = vec![(0, 0), (1, 1), (2, 2), (3, 3)];
        let policy = "(NEXT[0,292) (P5(x1,x3,x4,x2) OR ( P6(x4,x3,x1) SINCE[0,*) (P7(x1,x3) SINCE[0,*) P8(x4,x3,x1,x2) ))))".to_string();
        let expected = vec![
            (
                0,
                vec![
                    Data(true, vec![Int(1), Int(1), Int(1), Int(1)]),
                    Data(true, vec![Int(2), Int(2), Int(1), Int(2)]),
                    Data(true, vec![Int(2), Int(2), Int(2), Int(2)]),
                ],
            ),
            (
                1,
                vec![
                    Data(true, vec![Int(1), Int(1), Int(1), Int(1)]),
                    Data(true, vec![Int(2), Int(2), Int(2), Int(2)]),
                    Data(true, vec![Int(3), Int(3), Int(3), Int(3)]),
                ],
            ),
        ];
        test_dataflow_unordered(policy, data.clone(), times, expected);
    }

    #[test]
    fn prev_test_our() {
        let data = vec![vec!["a(2)", "b(2)"], vec!["a(4)"], vec!["a(3)", "b(4)"]];

        let times = vec![(0, 9), (1, 10), (2, 11)];
        let policy = "PREVIOUS [1,1] (a(x) AND b(x))".to_string();
        let expected = vec![(1, vec![Data(true, vec![Int(2)])])];
        test_dataflow_unordered(policy, data, times, expected);
    }
    #[test]
    fn prev_test_our2() {
        let data = vec![vec!["a(2)", "b(2)"], vec!["a(4)"], vec!["a(3)", "b(4)"]];

        let times = vec![(0, 0), (1, 1), (2, 2), (3, 3)];
        let policy = "PREVIOUS [0,*) a(x) AND b(x)".to_string();
        let expected = vec![(2, vec![Data(true, vec![Int(4)])])];
        test_dataflow_unordered(policy, data, times, expected);
    }

    #[test]
    fn empty_join() {
        let data = vec![vec!["a()", "b()"], vec!["a()"], vec!["a()", "b()"]];

        let times = vec![(0, 0), (1, 1), (2, 2), (3, 3)];
        let policy = "a() AND b()".to_string();
        let expected = vec![(0, vec![Data(true, vec![])]), (2, vec![Data(true, vec![])])];
        test_dataflow_unordered(policy, data, times, expected);
    }

    #[test]
    fn empty_join_1() {
        let data = vec![
            vec!["a()", "b()"],
            vec!["a()", "b()"],
            vec!["a()", "b()"],
            vec!["a()", "b()"],
            vec!["a()", "b()"],
            vec!["a()", "b()"],
            vec!["a()", "b()"],
            vec!["a()", "b()"],
            vec!["a()", "b()"],
            vec!["a()", "b()"],
        ];

        let times = vec![
            (0, 0),
            (1, 1),
            (2, 2),
            (3, 3),
            (4, 4),
            (5, 5),
            (6, 6),
            (7, 7),
            (8, 8),
            (9, 9),
        ];
        let policy = "a() AND b()".to_string();
        let expected = vec![
            (0, vec![Data(true, vec![])]),
            (1, vec![Data(true, vec![])]),
            (2, vec![Data(true, vec![])]),
            (3, vec![Data(true, vec![])]),
            (4, vec![Data(true, vec![])]),
            (5, vec![Data(true, vec![])]),
            (6, vec![Data(true, vec![])]),
            (7, vec![Data(true, vec![])]),
            (8, vec![Data(true, vec![])]),
            (9, vec![Data(true, vec![])]),
        ];
        test_dataflow_unordered(policy, data.clone(), times.clone(), expected.clone());

        let policy = "(ONCE[0,312] ((a() AND b())))".to_string();
        let expected = vec![
            (0, vec![Data(true, vec![])]),
            (1, vec![Data(true, vec![])]),
            (2, vec![Data(true, vec![])]),
            (3, vec![Data(true, vec![])]),
            (4, vec![Data(true, vec![])]),
            (5, vec![Data(true, vec![])]),
            (6, vec![Data(true, vec![])]),
            (7, vec![Data(true, vec![])]),
            (8, vec![Data(true, vec![])]),
            (9, vec![Data(true, vec![])]),
        ];

        println!("{:?}", parse_formula(&policy));

        test_dataflow_unordered(policy, data.clone(), times.clone(), expected.clone());

        let data = vec![
            vec!["a()", "b()", "c(1,1)"],
            vec!["a()", "b()", "c(2,2)"],
            vec![""],
            vec!["a()", "b()"],
            vec!["c(3,3)"],
        ];

        let policy = "((NOT (ONCE[0,312] (a() AND b()))) SINCE[0,301) c(x2,x1))".to_string();
        let times = vec![(0, 0), (1, 1), (2, 2), (3, 3), (4, 4)];
        let expected = vec![
            (0, vec![Data(true, vec![Int(1), Int(1)])]),
            (1, vec![Data(true, vec![Int(2), Int(2)])]),
            (4, vec![Data(true, vec![Int(3), Int(3)])]),
        ];

        test_dataflow_unordered(policy, data.clone(), times.clone(), expected.clone());

        let policy =
            "(ONCE(0,177) ((NOT (ONCE[0,312] (a() AND b()))) SINCE[0,301) c(x2,x1)))".to_string();
        let times = vec![(0, 0), (1, 1), (2, 2), (3, 3), (4, 4)];
        let expected = vec![
            (1, vec![Data(true, vec![Int(1), Int(1)])]),
            (
                2,
                vec![
                    Data(true, vec![Int(1), Int(1)]),
                    Data(true, vec![Int(2), Int(2)]),
                ],
            ),
            (
                3,
                vec![
                    Data(true, vec![Int(1), Int(1)]),
                    Data(true, vec![Int(2), Int(2)]),
                ],
            ),
            (
                4,
                vec![
                    Data(true, vec![Int(1), Int(1)]),
                    Data(true, vec![Int(2), Int(2)]),
                ],
            ),
        ];

        test_dataflow_unordered(policy, data.clone(), times.clone(), expected.clone());
    }

    #[test]
    fn prev_test_our_3() {
        let data = vec![
            vec!["a(2)", "b(2)"],
            vec!["a(4)"],
            vec!["a(3)", "b(4)"],
            vec!["a(5)", "b(5)"],
            vec!["a(1)", "b(7)"],
        ];

        let times = vec![(0, 9), (1, 10), (2, 11), (3, 12), (4, 13)];
        let policy = "PREVIOUS [1,1] (a(x) AND b(x))".to_string();
        let expected = vec![
            (1, vec![Data(true, vec![Int(2)])]),
            (4, vec![Data(true, vec![Int(5)])]),
        ];
        test_dataflow_unordered(policy, data, times, expected);
    }

    #[test]
    fn prev_test_our4() {
        let data = vec![vec!["a(2)", "b(2)"], vec!["a(4)"], vec!["a(3)", "b(4)"]];

        let times = vec![(0, 0), (1, 1), (2, 2)];
        let policy = "PREVIOUS [0,*) a(x)".to_string();
        let expected = vec![
            (1, vec![Data(true, vec![Int(2)])]),
            (2, vec![Data(true, vec![Int(4)])]),
        ];
        test_dataflow_unordered(policy, data, times, expected);
    }

    #[test]
    fn prev_no_interval_multiple() {
        let data = vec![vec!["b(2)"], vec!["b(5)"], vec!["a(2)"]];
        let times = vec![(0, 0), (1, 1), (2, 2)];
        let policy = "PREVIOUS [0,*) b(x)".to_string();

        let expected = vec![
            (1, vec![Data(true, vec![Int(2)])]),
            (2, vec![Data(true, vec![Int(5)])]),
        ];
        test_dataflow_unordered(policy, data, times, expected);
    }

    #[test]
    fn filter_test_value_1() {
        let data = vec![
            vec!["p(4, 5)"],
            vec!["p(6, 4)"],
            vec!["p(8, 7)"],
            vec!["p(5, 6)"],
        ];

        let times = vec![(0, 0), (1, 1), (2, 2), (3, 3), (4, 4)];

        let policy = "p(y, x) AND (x = 3)".to_string();

        println!("{}", parse_formula(&policy));

        let expected = vec![];

        test_dataflow(policy, data.clone(), times.clone(), expected);

        /*let policy = "p(y, x) AND NOT (x = 3)".to_string();

        //println!("{}", parse_formula(&policy));

        let expected = vec![
            (0, vec![Data(true, vec![(Int(4)), (Int(5))])]),
            (1, vec![Data(true, vec![(Int(6)), (Int(4))])]),
            (2, vec![Data(true, vec![(Int(8)), (Int(7))])]),
            (3, vec![Data(true, vec![(Int(5)), (Int(6))])]),
        ];

        //test_dataflow(policy, data.clone(), times.clone(), expected);*/
    }

    #[test]
    fn filter_test_value_2() {
        let data = vec![
            vec!["p(4, 4)"],
            vec!["p(3, 3)"],
            vec!["p(8, 8)"],
            vec!["p(6, 6)"],
        ];

        let times = vec![(0, 0), (1, 1), (2, 2), (3, 3), (4, 4)];

        let policy = "p(x, x) AND (x = 3)".to_string();

        println!("{}", parse_formula(&policy));

        let expected = vec![(1, vec![Data(true, vec![Int(3)])])];

        test_dataflow(policy, data.clone(), times.clone(), expected);

        //let policy = "p(x, x) AND NOT (x=3)".to_string();

        //println!("{}", parse_formula(&policy));

        /*let expected = vec![
            (0, vec![Data(true, vec![Int(4)])]),
            (2, vec![Data(true, vec![Int(8)])]),
            (3, vec![Data(true, vec![Int(6)])]),
        ];*/

        //test_dataflow(policy, data, times, expected);
    }

    #[test]
    fn filter_test_variable() {
        let data = vec![
            vec!["p(4, 3)"],
            vec!["p(6, 5)"],
            vec!["p(8, 7)"],
            vec!["p(5, 3)"],
        ];

        let times = vec![(0, 0), (1, 1), (2, 2), (3, 3), (4, 4)];

        let policy = "p(x, y) AND (x = y)".to_string();

        println!("{:?}", parse_formula(&policy));

        let expected = vec![];

        test_dataflow(policy, data.clone(), times.clone(), expected);

        //let policy = "p(x, y) AND NOT (x = y)".to_string();

        //println!("{}", parse_formula(&policy));

        /*let expected = vec![
            (0, vec![Data(true, vec![(Int(4)), (Int(3))])]),
            (1, vec![Data(true, vec![(Int(6)), (Int(5))])]),
            (2, vec![Data(true, vec![(Int(8)), (Int(7))])]),
            (3, vec![Data(true, vec![(Int(5)), (Int(3))])]),
        ];*/

        //test_dataflow(policy, data, times, expected);
    }

    #[test]
    fn match_vars_test() {
        //working example with constant and variable
        let ts = vec![Var("x".to_string())];
        let tuple = vec![Cst(Int(12))];
        assert_eq!(
            Some(HashMap::from([(Var("x".to_string()), Cst(Int(12)))])),
            match_vars(ts, tuple)
        );

        // two variables that match
        let ts = vec![Var("x".to_string()), Var("x".to_string())];
        let tuple = vec![Cst(Int(12)), Cst(Int(12))];
        assert_eq!(
            Some(HashMap::from([(Var("x".to_string()), Cst(Int(12)))])),
            match_vars(ts, tuple)
        );

        // two unmatching variables
        let ts = vec![Var("x".to_string()), Var("x".to_string())];
        let tuple = vec![Cst(Int(12)), Cst(Int(11))];
        assert_eq!(None, match_vars(ts, tuple));

        // two vaiables that match
        let ts = vec![Var("x".to_string()), Var("x".to_string())];
        let tuple = vec![Cst(Str("xx".to_string())), Cst(Str("xx".to_string()))];
        assert_eq!(
            Some(HashMap::from([(
                Var("x".to_string()),
                Cst(Str("xx".to_string()))
            )])),
            match_vars(ts, tuple)
        );

        let ts = vec![
            Var("y".to_string()),
            Var("x".to_string()),
            Var("x".to_string()),
            Var("y".to_string()),
        ];
        let tuple = vec![
            Cst(Str("ypsilon".to_string())),
            Cst(Int(12)),
            Cst(Int(12)),
            Cst(Str("ypsilon".to_string())),
        ];
        assert_eq!(
            Some(HashMap::from([
                (Var("y".to_string()), Cst(Str("ypsilon".to_string()))),
                (Var("x".to_string()), Cst(Int(12)))
            ])),
            match_vars(ts, tuple)
        );
    }

    fn test_dataflow(
        policy: String,
        datas: Vec<Vec<&str>>,
        times: Vec<(usize, usize)>,
        mut expected: Vec<(usize, Vec<Record>)>,
    ) {
        let (send, recv) = std::sync::mpsc::channel();
        let send = std::sync::Arc::new(std::sync::Mutex::new(send));

        let input_data: Vec<Vec<String>> = datas
            .clone()
            .into_iter()
            .map(|data| data.into_iter().map(|d| d.to_string()).collect())
            .collect();

        timely::execute(timely::Config::process(NUM_WORKERS), move |worker| {
            let send = send.lock().unwrap().clone();

            let (mut input, mut time_input, probe) = worker.dataflow::<usize, _, _>(|scope| {
                let (time_input, time_stream) = scope.new_input::<TimeFlowValues>();
                let (input, stream) = scope.new_input::<String>();

                let options = default_options();
                let (_attrs, output) =
                    create_dataflow(parse_formula(&policy), stream, time_stream, options);

                let probe = output.probe();
                output.capture_into(send);

                (input, time_input, probe)
            });

            let next_epoch = times[0].0;
            input.advance_to(next_epoch);
            worker.step_while(|| probe.less_than(input.time()));
            // Send data and step the workers
            for round in 0usize..times.len() - 1 {
                let next_epoch = times[round + 1].0;
                if worker.index() == 0 {
                    time_input.send(Timestamp(times[round + 1].1));
                    let mut data = input_data.get(round).unwrap_or(&Vec::new()).clone();
                    input.send_batch(&mut data);
                }
                time_input.advance_to(next_epoch);
                input.advance_to(next_epoch);
                worker.step_while(|| probe.less_than(input.time()));
            }

            time_input.close();
            input.close();
            worker.step();
        })
        .unwrap();

        // Vector holding a tuple (timepoint, vector with data for each timestamp)
        let mut actual_res: Vec<_> = recv
            .extract()
            .iter()
            .cloned()
            .map(|(time, tuples)| (time, tuples))
            .collect();

        println!("\nExpected:");
        print_operator_result(&expected);
        println!("\nActual:");
        print_operator_result(&actual_res);

        assert!(set_equality(&mut expected, &mut actual_res));
    }

    fn set_equality(
        res: &mut Vec<(usize, Vec<Record>)>,
        res1: &mut Vec<(usize, Vec<Record>)>,
    ) -> bool {
        if res.len() != res1.len() {
            return false;
        }

        res.sort();
        res1.sort();

        for i in 0..res.len() {
            let (tmp_tp, tmp_res) = res[i].clone();
            let (tmp_tp1, tmp_res1) = res1[i].clone();

            let set1: HashSet<Record> = tmp_res.clone().into_iter().collect();
            let set2: HashSet<Record> = tmp_res1.clone().into_iter().collect();

            if set1 != set2 || tmp_tp != tmp_tp1 {
                return false;
            }
        }

        return true;
    }

    fn print_operator_result(res: &Vec<(usize, Vec<Record>)>) {
        res.iter().for_each(|(time, tuples)| {
            println!("Tp {}: ", time.to_string());
            for tuple in tuples {
                print!("    ");
                match tuple.clone() {
                    Data(_, v) => {
                        for v_ in v {
                            print!(" {}  ", v_.to_string())
                        }
                    }
                    _ => {}
                };
                println!();
            }
        });
    }

    fn test_dataflow_unordered(
        policy: String,
        datas: Vec<Vec<&str>>,
        times: Vec<(usize, usize)>,
        mut expected: Vec<(usize, Vec<Record>)>,
    ) {
        let (send, recv) = std::sync::mpsc::channel();
        let send = std::sync::Arc::new(std::sync::Mutex::new(send));

        let input_data: Vec<Vec<String>> = datas
            .clone()
            .into_iter()
            .map(|data| data.into_iter().map(|d| d.to_string()).collect())
            .collect();

        //let graph = build_dependency_graph(&parse_formula(&policy));
        timely::execute(timely::Config::process(NUM_WORKERS), move |worker| {
            let send = send.lock().unwrap().clone();
            let (mut input, cap, mut time_input, time_cap, _probe) = worker
                .dataflow::<usize, _, _>(|scope| {
                    let ((time_input, time_cap), time_stream) =
                        scope.new_unordered_input::<TimeFlowValues>();
                    let ((input, input_cap), stream) = scope.new_unordered_input::<String>();
                    let options = default_options();
                    let (_attrs, output) =
                        create_dataflow(parse_formula(&policy), stream, time_stream, options);

                    let probe = output.probe();
                    output.capture_into(send);

                    (input, input_cap, time_input, time_cap, probe)
                });

            let _next_epoch = times[0].0;
            for round in 0usize..times.len() {
                let next_epoch = times[round].0;

                if worker.index() == 0 {
                    time_input
                        .session(cap.delayed(&next_epoch))
                        .give(Timestamp(times[round].1));
                    let data = input_data.get(round).unwrap_or(&Vec::new()).clone();
                    input
                        .session(cap.delayed(&next_epoch))
                        .give_iterator(data.into_iter());
                }
                worker.step();
            }
            let new_prod = times.len();
            input
                .session(cap.delayed(&new_prod))
                .give("<eos>".parse().unwrap());
            time_input.session(time_cap.delayed(&new_prod)).give(EOS);
        })
        .unwrap();

        // Vector holding a tuple (timepoint, vector with data for each timestamp)
        let mut actual_res: Vec<_> = recv
            .extract()
            .iter()
            .cloned()
            .map(|(time, tuples)| (time, tuples))
            .collect();

        // Printing results:
        println!("\nExpected:");
        print_operator_result(&expected);
        println!("\nActual:");
        print_operator_result(&actual_res);

        assert!(set_equality(&mut expected, &mut actual_res));
    }

    #[test]
    fn fact() {
        let data = vec![vec!["p(4)"], vec!["p(5)"], vec!["p(6)"]];

        let times = vec![(0, 0), (1, 1), (2, 2), (3, 3)];

        let policy = "p(x)".to_string();

        let expected = vec![
            (0, vec![Data(true, vec![(Int(4))])]),
            (1, vec![Data(true, vec![(Int(5))])]),
            (2, vec![Data(true, vec![(Int(6))])]),
        ];

        test_dataflow(policy, data, times, expected);
    }

    #[test]
    fn fact_multiple_args() {
        let data = vec![vec!["p(3, 4)"], vec!["p(5, 6)"], vec!["p(7, 8)"]];

        let times = vec![(0, 0), (1, 1), (2, 2), (3, 3)];

        let policy = "p(x, y)".to_string();

        let expected = vec![
            (0, vec![Data(true, vec![(Int(3)), (Int(4))])]),
            (1, vec![Data(true, vec![(Int(5)), (Int(6))])]),
            (2, vec![Data(true, vec![(Int(7)), (Int(8))])]),
        ];

        test_dataflow(policy, data, times, expected);
    }

    #[test]
    fn fact_different_types_or_arguments() {
        let data = vec![
            vec!["p(3, 'foo')"],
            vec!["p(5, 'bar')"],
            vec!["p(7, 'blah')"],
        ];

        let times = vec![(0, 0), (1, 1), (2, 2), (3, 3)];

        let policy = "p(x, y)".to_string();

        let expected = vec![
            (0, vec![Data(true, vec![Int(3), Str("foo".to_string())])]),
            (1, vec![Data(true, vec![Int(5), Str("bar".to_string())])]),
            (2, vec![Data(true, vec![Int(7), Str("blah".to_string())])]),
        ];

        test_dataflow(policy, data, times, expected);
    }

    #[test]
    fn conj_simple() {
        let data = vec![
            vec!["p(3)", "q(3)"],
            vec!["p(5)", "q(5)"],
            vec!["p(7)", "q(8)"],
        ];

        let times = vec![(0, 0), (1, 1), (2, 2), (3, 3)];

        let policy = "p(x) AND q(x)".to_string();

        let expected = vec![
            (0, vec![Data(true, vec![Int(3)])]),
            (1, vec![Data(true, vec![Int(5)])]),
        ];

        test_dataflow(policy, data, times, expected);
    }

    #[test]
    fn conj_with_join() {
        let data = vec![
            vec!["p(3, 4)", "q(3, 5)"],
            vec!["p(5, 6)", "q(5, 7)"],
            vec!["p(7, 8)", "q(8, 8)"],
        ];

        let times = vec![(0, 0), (1, 1), (2, 2), (3, 3)];

        let policy = "p(x, y) AND q(x, z)".to_string();

        let expected = vec![
            (0, vec![Data(true, vec![(Int(3)), (Int(4)), (Int(5))])]),
            (1, vec![Data(true, vec![(Int(5)), (Int(6)), (Int(7))])]),
        ];

        test_dataflow(policy, data, times, expected);
    }

    #[test]
    fn conj_neg_antijoin() {
        let data = vec![
            vec!["p(3)", "q(3)"],
            vec!["p(5)", "q(5)"],
            vec!["p(7)", "q(8)"],
        ];

        let times = vec![(0, 0), (1, 1), (2, 2), (3, 3)];

        let policy = "p(x) AND NOT q(x)".to_string();

        let expected = vec![(2, vec![Data(true, vec![(Int(7))])])];

        test_dataflow(policy, data, times, expected);
    }

    #[test]
    fn exists_conj_with_join() {
        let data = vec![
            vec!["p(3, 4)", "q(3, 5)"],
            vec!["p(5, 6)", "q(5, 7)"],
            vec!["p(7, 8)", "q(8, 8)"],
        ];

        let times = vec![(0, 0), (1, 1), (2, 2), (3, 3)];

        let policy = "EXISTSx.(p(x, y) AND q(x, z))".to_string();
        //let policy = "p(x, y)&&q(x, z)".to_string();

        let expected = vec![
            (0, vec![Data(true, vec![Int(4), Int(5)])]),
            (1, vec![Data(true, vec![Int(6), Int(7)])]),
        ];

        test_dataflow(policy, data, times, expected);
    }

    #[test]
    fn random_formula() {
        let data = vec![
            vec!["p(3, 4)", "q(3, 5)"],
            vec!["p(5, 6)", "q(5, 7)"],
            vec!["p(7, 8)", "q(8, 8)"],
        ];

        let times = vec![(0, 0), (1, 1), (2, 2), (3, 3)];

        let policy = "x2 = 40".to_string();

        println!("{}", parse_formula(&policy));

        let expected = vec![
            (0, vec![Data(true, vec![Int(40)])]),
            (1, vec![Data(true, vec![Int(40)])]),
            (2, vec![Data(true, vec![Int(40)])]),
        ];

        test_dataflow(policy.clone(), data, times.clone(), expected);
    }

    #[test]
    fn random_formula2() {
        let policy = "(((NOT (ONCE(0,*) (ONCE(0,*) P0(x8,x4,x5,x6)))) UNTIL(0,279) P1(x4,x7,x6,x3,x2,x8,x5,x1)) AND (x2 = 40))".to_string();

        let data = vec![
            vec!["P0(1,1,1,1)", "P0(2,2,2,2)", "P1(1,1,1,1,40,1,1,1)"],
            vec!["P0(3,3,3,3)"],
            vec!["P0(4,4,4,4)", "P0(5,5,5,5)"],
            vec!["P0(6,6,6,6)"],
            vec!["P1(1,1,1,1,40,1,1,1)"],
        ];
        let times = vec![(0, 0), (1, 1), (2, 2), (3, 3), (4, 4)];

        let expected = vec![];

        test_dataflow_unordered(policy, data, times.clone(), expected);
    }

    #[test]
    fn exists_conj_with_join_1() {
        let data = vec![
            vec!["p(4, 3)", "q(3, 5)"],
            vec!["p(6, 5)", "q(5, 7)"],
            vec!["p(8, 7)", "q(8, 8)"],
        ];

        let times = vec![(0, 0), (1, 1), (2, 2), (3, 3)];

        let policy = "EXISTSx.(p(y, x) AND q(x, z))".to_string();

        println!("{}", parse_formula(&policy));

        let expected = vec![
            (0, vec![Data(true, vec![Int(4), Int(5)])]),
            (1, vec![Data(true, vec![Int(6), Int(7)])]),
        ];

        test_dataflow(policy, data, times, expected);
    }

    #[test]
    fn eventually_test() {
        // EOS is missing
        let data = vec![vec!["q(3, 5)"], vec!["q(5, 7)"], vec!["q(8, 8)"]];

        let times = vec![(0, 0), (1, 1), (2, 2)];

        let policy = "TRUE UNTIL [0, 1] q(x,y)".to_string();

        let expected = vec![
            (
                0,
                vec![
                    Data(true, vec![Int(3), Int(5)]),
                    Data(true, vec![Int(5), Int(7)]),
                ],
            ),
            (
                1,
                vec![
                    Data(true, vec![Int(5), Int(7)]),
                    Data(true, vec![Int(8), Int(8)]),
                ],
            ),
            (2, vec![Data(true, vec![Int(8), Int(8)])]),
        ];

        test_dataflow_unordered(policy, data.clone(), times.clone(), expected);

        let policy = "TRUE UNTIL [10, 20] q(x,y)".to_string();

        let times = vec![(0, 10), (1, 20), (2, 30)];
        let expected = vec![
            (
                0,
                vec![
                    Data(true, vec![Int(5), Int(7)]),
                    Data(true, vec![Int(8), Int(8)]),
                ],
            ),
            (1, vec![Data(true, vec![Int(8), Int(8)])]),
        ];

        test_dataflow_unordered(policy, data, times, expected);
    }

    #[test]
    fn benchmark_test() {
        // EOS is missing
        let times = vec![(0, 0), (1, 1), (2, 2)];

        let data = vec![
            vec!["A(1, 1)", "B(1, 2)"],
            vec!["A(1, 2)", "B(2, 1)"],
            vec!["B(2, 2)", "A(2, 2)", "C(1,1)"],
        ];

        // join A && B
        let policy = "A(a,b) AND B(b,c)".to_string();

        let expected = vec![
            (0, vec![Data(true, vec![Int(1), Int(1), Int(2)])]),
            (1, vec![Data(true, vec![Int(1), Int(2), Int(1)])]),
            (2, vec![Data(true, vec![Int(2), Int(2), Int(2)])]),
        ];

        test_dataflow_unordered(policy, data.clone(), times.clone(), expected);

        // once and join
        println!("--------------------- Once and Join");
        let policy = "ONCE [0, 7] (A(a,b) AND B(b,c))".to_string();
        let expected = vec![
            (0, vec![Data(true, vec![Int(1), Int(1), Int(2)])]),
            (
                1,
                vec![
                    Data(true, vec![Int(1), Int(1), Int(2)]),
                    Data(true, vec![Int(1), Int(2), Int(1)]),
                ],
            ),
            (
                2,
                vec![
                    Data(true, vec![Int(1), Int(1), Int(2)]),
                    Data(true, vec![Int(1), Int(2), Int(1)]),
                    Data(true, vec![Int(2), Int(2), Int(2)]),
                ],
            ),
        ];

        test_dataflow_unordered(policy, data.clone(), times.clone(), expected);

        // eventually
        let policy = "EVENTUALLY [0, 7] C(z, x)".to_string();
        let expected = vec![
            (0, vec![Data(true, vec![Int(1), Int(1)])]),
            (1, vec![Data(true, vec![Int(1), Int(1)])]),
            (2, vec![Data(true, vec![Int(1), Int(1)])]),
        ];

        test_dataflow_unordered(policy, data.clone(), times.clone(), expected);

        // anti join
        println!("----------------- Anti Join -----------------");
        let data1 = vec![
            vec!["A(1, 1, 2)", "C(1, 1)"],
            vec!["A(1, 1, 2)", "A(1, 2, 1)", "C(1, 1)"],
            vec!["A(1, 1, 2)", "A(1, 2, 1)", "A(2, 2, 2)", "C(1, 1)"],
        ];
        let policy = "A(x, y, z) AND NOT C(z, x)".to_string();
        let expected = vec![
            (0, vec![Data(true, vec![Int(1), Int(1), Int(2)])]),
            (1, vec![Data(true, vec![Int(1), Int(1), Int(2)])]),
            (
                2,
                vec![
                    Data(true, vec![Int(1), Int(1), Int(2)]),
                    Data(true, vec![Int(2), Int(2), Int(2)]),
                ],
            ),
        ];

        test_dataflow_unordered(policy, data1.clone(), times.clone(), expected);
    }

    #[test]
    fn benchmark_full() {
        let times = vec![(0, 0), (1, 1), (2, 2)];

        let data = vec![
            vec!["A(1, 1)", "B(1, 2)"],
            vec!["A(1, 2)", "B(2, 1)"],
            vec!["B(2, 2)", "A(2, 2)", "C(1,1)"],
        ];
        let policy =
            "(ONCE[0, 7] (A(x,y) AND B(y,z))) AND NOT (EVENTUALLY[0, 7] C(z, x))".to_string();
        let expected = vec![
            (0, vec![Data(true, vec![Int(1), Int(1), Int(2)])]),
            (1, vec![Data(true, vec![Int(1), Int(1), Int(2)])]),
            (
                2,
                vec![
                    Data(true, vec![Int(1), Int(1), Int(2)]),
                    Data(true, vec![Int(2), Int(2), Int(2)]),
                ],
            ),
        ];

        test_dataflow_unordered(policy, data.clone(), times.clone(), expected);
    }

    #[test]
    fn test_union() {
        let data = vec![
            vec!["a(10, 20)", "b(20, 2)"],
            vec!["a(1, 2)", "b(2, 1)"],
            vec!["a(2, 2)", "b(4, 5)"],
        ];

        let times = vec![(0, 9), (1, 10), (2, 11)];
        let policy = "(a(a,b) OR b(b,a))".to_string();
        let expected = vec![
            (
                0,
                vec![
                    Data(true, vec![Int(2), Int(20)]),
                    Data(true, vec![Int(10), Int(20)]),
                ],
            ),
            (1, vec![Data(true, vec![Int(1), Int(2)])]),
            (
                2,
                vec![
                    Data(true, vec![Int(2), Int(2)]),
                    Data(true, vec![Int(5), Int(4)]),
                ],
            ),
        ];
        test_dataflow_unordered(policy, data, times, expected);
    }

    #[test]
    fn test_union1() {
        let data = vec![
            vec!["a(10, 20)", "b(20, 2)"],
            vec!["a(1, 2)", "b(2, 1)"],
            vec!["a(2, 2)", "b(4, 5)"],
        ];

        let times = vec![(0, 9), (1, 10), (2, 11)];
        let policy = "(a(a,b) OR b(a,b))".to_string();
        let expected = vec![
            (
                0,
                vec![
                    Data(true, vec![Int(10), Int(20)]),
                    Data(true, vec![Int(20), Int(2)]),
                ],
            ),
            (
                1,
                vec![
                    Data(true, vec![Int(1), Int(2)]),
                    Data(true, vec![Int(2), Int(1)]),
                ],
            ),
            (
                2,
                vec![
                    Data(true, vec![Int(2), Int(2)]),
                    Data(true, vec![Int(4), Int(5)]),
                ],
            ),
        ];
        test_dataflow_unordered(policy, data, times, expected);
    }

    #[test]
    fn test() {
        let mut v: Vec<String> = Vec::new();
        v.push("a".to_string());
        v.push("c".to_string());
        v.push("b".to_string());

        for x in v.clone() {
            print!(" {} ", x);
        }

        println!();

        v.sort_by(|a, b| a.to_lowercase().cmp(&b.to_lowercase()));

        for x in v.clone() {
            print!(" {} ", x);
        }
    }

    #[test]
    fn wikipedia_case_study() {
        let x = "edit(user,0) AND ONCE[1,3] (edit(user,0) AND ONCE[1,3] edit(user,0))".to_string();
        let graph = parse_formula(&x); //generate_evaluation_plan(&parse_formula(&x));
        let exp = build_conj(
            build_fact("edit", vec!["user", "0"]),
            build_once(
                build_conj(
                    build_fact("edit", vec!["user", "0"]),
                    build_once(
                        build_fact("edit", vec!["user", "0"]),
                        TimeInterval::new(TS::FINITE(1), TS::FINITE(3)),
                    ),
                ),
                TimeInterval::new(TS::FINITE(1), TS::FINITE(3)),
            ),
        );
        assert_eq!(graph, exp);
        println!("{}", graph)
    }

    fn test_dataflow_unordered_timed(
        policy: String,
        datas: Vec<Vec<String>>,
        times: Vec<(usize, usize)>,
        expected: &mut BTreeMap<usize, Vec<Vec<Constant>>>,
    ) {
        let (send, recv) = std::sync::mpsc::channel();
        let send = std::sync::Arc::new(std::sync::Mutex::new(send));

        let input_data: Vec<Vec<String>> = datas
            .clone()
            .into_iter()
            .map(|data| data.into_iter().map(|d| d.to_string()).collect())
            .collect();

        //let graph = build_dependency_graph(&parse_formula(&policy));
        //let time_execute = Instant::now();
        timely::execute(timely::Config::process(4), move |worker| {
            let send = send.lock().unwrap().clone();
            let (mut input, cap, mut time_input, time_cap, _probe) = worker
                .dataflow::<usize, _, _>(|scope| {
                    let ((time_input, time_cap), time_stream) =
                        scope.new_unordered_input::<TimeFlowValues>();
                    let ((input, input_cap), stream) = scope.new_unordered_input::<String>();
                    let options = default_options();
                    let (_attrs, output) =
                        create_dataflow(parse_formula(&policy), stream, time_stream, options);

                    let probe = output.probe();
                    output.capture_into(send);

                    (input, input_cap, time_input, time_cap, probe)
                });

            if worker.index() == 0 {
                for round in 0usize..times.len() {
                    let next_epoch = times[round].0;
                    time_input
                        .session(time_cap.delayed(&next_epoch))
                        .give(Timestamp(times[round].1));
                    let data = input_data.get(round).unwrap_or(&Vec::new()).clone();
                    input
                        .session(cap.delayed(&next_epoch))
                        .give_iterator(data.into_iter());
                    worker.step();
                }
                let new_prod = times.len();
                time_input.session(time_cap.delayed(&new_prod)).give(EOS);
                input
                    .session(cap.delayed(&new_prod))
                    .give("<eos>".parse().unwrap());
            }
        })
        .unwrap();

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
    }

    fn verify_results(
        actual: BTreeMap<usize, Vec<Vec<Constant>>>,
        expected: BTreeMap<usize, Vec<Vec<Constant>>>,
    ) -> bool {
        if actual.len() != expected.len() {
            println!("Unequal length");
            return false;
        }

        for (key, values) in actual {
            if let Some(exp_values) = expected.get(&key) {
                if values.len() != exp_values.len() {
                    println!("Unequal number of records at Keys: ({})", key.clone());
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
        return true;
    }
}
