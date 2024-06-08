#![allow(dead_code)]
#![warn(unused_imports)]
#[warn(unused_assignments)]
use std::cmp::{max, min};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::iter::zip;

use timely::dataflow::channels::pact::{ParallelizationContract, Pipeline};
use timely::dataflow::channels::pact::Exchange as PactExchange;

use timely::dataflow::operators::{Operator, Map, Capability, FrontierNotificator, Filter, Exchange};

use timely::dataflow::{Scope, Stream};

use constants::calculate_hash;
use dataflow_constructor::types::{FlowValues::Data as Data, FlowValues::MetaData as MetaData, OperatorOptions, Record as Record, TimeFlowValues};

use dataflow_constructor::partial_sequence::{contains, Intervals, PartialSequence, SatisfactionDs};
use parser::formula_syntax_tree::{Arg, Constant};

use timely::dataflow::channels::pushers::Tee;
use timely::dataflow::operators::generic::{FrontieredInputHandle, OperatorInfo, OutputHandle};
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::progress::{Antichain, Timestamp};
use dataflow_constructor::observation_sequence::{InfinityIntervalsReturn, ObservationSequence, TimeSeq};
use dataflow_constructor::observation_sequence::InfinityIntervalsReturn::Literal;
use dataflow_constructor::types::TimeFlowValues::Timestamp as FTimestamp;
use timeunits::TimeInterval;

pub trait OperatorsWithSummary<G, D1> where G: Scope, D1: timely::Data {
    fn binary_frontier_sum<D2, D3, B, L, P1, P2>(&self, other: &Stream<G, D2>, pact1: P1, pact2: P2, name: &str, summary_time: <G::Timestamp as Timestamp>::Summary, summary_data: <G::Timestamp as Timestamp>::Summary, constructor: B) -> Stream<G, D3>
        where
            D2: timely::Data,
            D3: timely::Data,
            B: FnOnce(Capability<G::Timestamp>, OperatorInfo) -> L,
            L: FnMut(&mut FrontieredInputHandle<G::Timestamp, D1, P1::Puller>,
                &mut FrontieredInputHandle<G::Timestamp, D2, P2::Puller>,
                &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>)+'static,
            P1: ParallelizationContract<G::Timestamp, D1>,
            P2: ParallelizationContract<G::Timestamp, D2>;

    fn unary_frontier_sum<D2, B, L, P>(&self, pact: P, name: &str, summary: <G::Timestamp as Timestamp>::Summary, constructor: B) -> Stream<G, D2>
        where
            D2: timely::Data, // Data,
            B: FnOnce(Capability<G::Timestamp>, OperatorInfo) -> L,
            L: FnMut(&mut FrontieredInputHandle<G::Timestamp, D1, P::Puller>,
                &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>)+'static,
            P: ParallelizationContract<G::Timestamp, D1>;

}
impl<G: Scope, D1: timely::Data> OperatorsWithSummary<G, D1> for Stream<G, D1> {
    fn binary_frontier_sum<D2, D3, B, L, P1, P2>(&self, other: &Stream<G, D2>, pact1: P1, pact2: P2, name: &str, summary_time: <G::Timestamp as Timestamp>::Summary, summary_data: <G::Timestamp as Timestamp>::Summary, constructor: B) -> Stream<G, D3>
        where
            D2: timely::Data,
            D3: timely::Data,
            B: FnOnce(Capability<G::Timestamp>, OperatorInfo) -> L,
            L: FnMut(&mut FrontieredInputHandle<G::Timestamp, D1, P1::Puller>,
                &mut FrontieredInputHandle<G::Timestamp, D2, P2::Puller>,
                &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>)+'static,
            P1: ParallelizationContract<G::Timestamp, D1>,
            P2: ParallelizationContract<G::Timestamp, D2> {

        let mut builder = OperatorBuilder::new(name.to_owned(), self.scope());
        let operator_info = builder.operator_info();

        let (mut output, stream) = builder.new_output();
        let mut input1 = builder.new_input_connection(self, pact1, vec![Antichain::from_elem(summary_data.clone())]);
        let mut input2 = builder.new_input_connection(other, pact2, vec![Antichain::from_elem(summary_time.clone())]);

        builder.build(move |mut capabilities| {
            // capabilities should be a single-element vector.
            let capability = capabilities.pop().unwrap();
            let mut logic = constructor(capability, operator_info);
            move |frontiers| {
                let mut input1_handle = FrontieredInputHandle::new(&mut input1, &frontiers[0]);
                let mut input2_handle = FrontieredInputHandle::new(&mut input2, &frontiers[1]);
                let mut output_handle = output.activate();
                logic(&mut input1_handle, &mut input2_handle, &mut output_handle);
            }
        });


        stream
    }

    fn unary_frontier_sum<D2, B, L, P>(&self, pact: P, name: &str, summary: <G::Timestamp as Timestamp>::Summary, constructor: B) -> Stream<G, D2>
        where
            D2: timely::Data,
            B: FnOnce(Capability<G::Timestamp>, OperatorInfo) -> L,
            L: FnMut(&mut FrontieredInputHandle<G::Timestamp, D1, P::Puller>,
                &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>)+'static,
            P: ParallelizationContract<G::Timestamp, D1> {

        let mut builder = OperatorBuilder::new(name.to_owned(), self.scope());
        let operator_info = builder.operator_info();

        let (mut output, stream) = builder.new_output();
        let mut input = builder.new_input_connection(self, pact, vec![Antichain::from_elem(summary.clone())]);

        builder.build(move |mut capabilities| {
            let capability = capabilities.pop().unwrap();
            let mut logic = constructor(capability, operator_info);
            move |frontiers| {
                let mut input_handle = FrontieredInputHandle::new(&mut input, &frontiers[0]);
                let mut output_handle = output.activate();
                logic(&mut input_handle, &mut output_handle);
            }
        });

        stream
    }
}

fn ternary_frontier<B, L, G, P1, P2, P3>(
    lhs_stream : Stream<G, Record>, rhs_stream : Stream<G, Record>, time_stream: Stream<G, TimeFlowValues>,
    lhs_pact : P1, rhs_pact : P2, time_pact: P3, constructor: B) -> Stream<G, Record>
    where
        G: Scope<Timestamp = usize>,
        B: FnOnce(Capability<usize>, OperatorInfo) -> L,
        L: FnMut(&mut FrontieredInputHandle<G::Timestamp, TimeFlowValues, P3::Puller>,
            &mut FrontieredInputHandle<G::Timestamp, Record, P1::Puller>,
            &mut FrontieredInputHandle<G::Timestamp, Record, P2::Puller>,
            &mut OutputHandle<G::Timestamp, Record, Tee<G::Timestamp, Record>>)+'static,
        P1: ParallelizationContract<usize, Record>,
        P2: ParallelizationContract<usize, Record>,
        P3: ParallelizationContract<usize, TimeFlowValues> {
    let scope = time_stream.scope();
    let mut builder = OperatorBuilder::new("Ternary".to_owned(), scope.clone());
    let operator_info = builder.operator_info();

    let mut time_input = builder.new_input(&time_stream, time_pact);
    let mut lhs_input = builder.new_input(&lhs_stream, lhs_pact);
    let mut rhs_input = builder.new_input(&rhs_stream, rhs_pact);
    let (mut output_wrapper, output_stream) = builder.new_output();

    builder.build(move |mut capability| {
        let capability = capability.pop().unwrap();
        let mut logic = constructor(capability, operator_info);
        move |frontiers| {
            let mut time_handle = FrontieredInputHandle::new(&mut time_input, &frontiers[0]);
            let mut lhs_handle = FrontieredInputHandle::new(&mut lhs_input, &frontiers[1]);
            let mut rhs_handle = FrontieredInputHandle::new(&mut rhs_input, &frontiers[2]);
            let mut output_handle = output_wrapper.activate();
            logic(&mut time_handle, &mut lhs_handle, &mut rhs_handle, &mut output_handle);
        }
    });
    output_stream
}

pub trait SupportOperators<G: Scope<Timestamp = usize>> {
    fn distribute(&self, visitor: &mut usize, side: bool, lhs_attrs: &Vec<String>, rhs_attrs: &Vec<String>) -> (Vec<String>, Stream<G, Record>);

    fn exhaust(&self, visitor: &mut usize, time_stream: Stream<G, TimeFlowValues>, fv: Vec<String>, options: OperatorOptions) -> (Vec<String>, Stream<G, Record>);
}

impl<G: Scope<Timestamp = usize>> SupportOperators<G> for Stream<G, Record> {
    // // has to be used before temporal operators to ensure that each worker receives all necessary metadata
    fn distribute(&self, visitor: &mut usize, side: bool, lhs_attrs: &Vec<String>, rhs_attrs: &Vec<String>) -> (Vec<String>, Stream<G, Record>) {
        *visitor = visitor.clone() + 1;
        let peers = self.scope().peers() as u64;
        let (_, lhs_key_indices, rhs_key_indices) =
            find_common_bound_variables(lhs_attrs.clone(), rhs_attrs.clone());

        let key_indices = if side {
            lhs_key_indices
        } else {
            // Indicates if the operator is Once or Eventually
            if rhs_key_indices.is_empty() {
                let mut tmp = Vec::with_capacity(rhs_attrs.len());
                for i in 0..rhs_attrs.len() {
                    tmp.push(i);
                }
                tmp
            } else {
                rhs_key_indices
            }
        };

        let output = self
            .flat_map(move |data| {
                let range = match &data {
                    Data(_, rec) => {
                        // empty vectors on the right hand side should be processed on one worker
                        if !rec.is_empty() || !side {
                            let k1 = split_keys_ref(rec, &key_indices);
                            let hash = calculate_hash(&k1);
                            hash..hash + 1
                        } else {
                            0..peers
                        }
                    }
                    _ => 0..peers,
                };
                range.map(move |i| (i, data.clone()))
            })
            .exchange(|ix| ix.0)
            .map(|i| i.1);

        let attrs = if side { lhs_attrs } else { rhs_attrs };
        (attrs.clone(), output)
    }

    fn exhaust(&self, visitor: &mut usize, time_stream: Stream<G, TimeFlowValues>, free_variables : Vec<String>, options: OperatorOptions) -> (Vec<String>, Stream<G, Record>) {
        *visitor = visitor.clone() + 1;
        let mut vector1 = Vec::new();
        let mut vector2 = Vec::new();


        let out_stream = match options.get_output_mode() {
            0 => {//write to file at the end
                Filter::filter(self, move |rec| match rec {
                    Data(true, _) => true,
                    _ => false
                })
            },
            1 => {//print to stdin
                let mut stash : Vec<(usize, Vec<Constant>)> = Vec::with_capacity(8);
                let mut tp_to_ts = HashMap::with_capacity(8);
                self.binary(&time_stream, Pipeline, Pipeline, "Stdout", move |_,_| move |data_input, time_input, _output| {
                    time_input.for_each(|t, data| {
                        data.swap(&mut vector2);
                        for rec in vector2.drain(..) {
                            if let FTimestamp(x) = rec {
                                tp_to_ts.entry(*t.time()).or_insert(x);
                            }
                        }

                        if !stash.is_empty() {
                            stash.retain(|(tp, arg)| {
                                if let Some(ts) = tp_to_ts.get(tp) {
                                    println!("@{ts} (time point {}): {}", tp, format_record(arg.clone()));
                                    false
                                } else {
                                    true
                                }
                            });
                        }
                    });

                    data_input.for_each(|time, data| {
                        data.swap(&mut vector1);
                        for rec in vector1.drain(..) {
                            match rec {
                                Data(true, arg) => {
                                    if let Some(ts) = tp_to_ts.get(time.time()) {
                                        println!("@{ts} (time point {}): {}", time.time(), format_record(arg));
                                    } else {
                                        stash.push((*time.time(), arg))
                                    }
                                }
                                _ => {}
                            }
                        }
                    });
                })
            },
            2 => { // continues file write
                let batch_size = options.get_output_batch();
                let mut stash : Vec<(usize, Vec<Constant>)> = Vec::with_capacity(8);
                let mut out_stash : String = "".to_string();
                let mut counter = 0;
                let mut tp_to_ts = HashMap::with_capacity(8);
                self.binary(&time_stream, Pipeline, Pipeline, "Stdout", move |_,_| move |data_input, time_input, _output| {
                    time_input.for_each(|t, data| {
                        data.swap(&mut vector2);
                        for rec in vector2.drain(..) {
                            if let FTimestamp(x) = rec {
                                tp_to_ts.entry(*t.time()).or_insert(x);
                            }
                        }

                        if !stash.is_empty() {
                            stash.retain(|(tp, arg)| {
                                if let Some(ts) = tp_to_ts.get(tp) {
                                    println!("@{ts} (time point {}): {}", tp, format_record(arg.clone()));
                                    false
                                } else {
                                    true
                                }
                            });
                        }
                    });

                    data_input.for_each(|time, data| {
                        data.swap(&mut vector1);
                        for rec in vector1.drain(..) {
                            match rec {
                                Data(true, arg) => {
                                    if let Some(ts) = tp_to_ts.get(time.time()) {
                                        counter += 1;
                                        out_stash.push_str(&format!("@{} (time point {}): {}\n", ts, time.time(), format_record(arg)));
                                        if counter >= batch_size {
                                            println!("{}", out_stash);
                                            counter = 0;
                                            out_stash = "".to_string();
                                        }
                                    } else {
                                        stash.push((*time.time(), arg))
                                    }
                                }
                                _ => {}
                            }
                        }
                    });
                })
            }
            _ => {
                //disregard output
                Filter::filter(self, move |_| false)
            }
        };

        (free_variables, out_stream)
    }
}


// We need to use Record instead of D, because we need to be able to access the
// individual data elements and perform .get operations on the vector. I need
// to know that it is a vector in order to use its functions.
pub trait Operators<G: Scope<Timestamp = usize>> {
    fn join(
        &self,
        visitor: &mut usize,
        rhs_stream: &Stream<G, Record>,
        lhs_attrs: &Vec<String>,
        rhs_attrs: &Vec<String>,
    ) -> (Vec<String>, Stream<G, Record>);

    fn union(
        &self,
        visitor: &mut usize,
        rhs_stream: &Stream<G, Record>,
        lhs_attrs: &Vec<String>,
        rhs_attrs: &Vec<String>,
    ) -> (Vec<String>, Stream<G, Record>);

    fn anti_join(
        &self,
        visitor: &mut usize,
        rhs_stream: &Stream<G, Record>,
        lhs_attrs: &Vec<String>,
        rhs_attrs: &Vec<String>,
    ) -> (Vec<String>, Stream<G, Record>);

    fn projection(
        &self,
        visitor: &mut usize,
        attrs: &Vec<String>,
        wanted_attrs: &Vec<String>,
    ) -> (Vec<String>, Stream<G, Record>);

    fn since(
        self,
        visitor: &mut usize,
        rhs_stream: Stream<G, Record>,
        time_stream: Stream<G, TimeFlowValues>,
        lhs_attrs: &Vec<String>,
        rhs_attrs: &Vec<String>,
        interval: TimeInterval,
        deduplication: bool,
    ) -> (Vec<String>, Stream<G, Record>);

    fn until(
        self,
        visitor: &mut usize,
        rhs_stream: Stream<G, Record>,
        time_stream: Stream<G, TimeFlowValues>,
        lhs_attrs: &Vec<String>,
        rhs_attrs: &Vec<String>,
        interval: TimeInterval,
        deduplication: bool,
    ) -> (Vec<String>, Stream<G, Record>);

    fn filter(
        &self,
        visitor: &mut usize,
        attrs: Vec<String>,
        var: String,
        val: Arg,
    ) -> (Vec<String>, Stream<G, Record>);

    fn neg_filter(
        &self,
        visitor: &mut usize,
        attrs: Vec<String>,
        var: String,
        val: Arg,
    ) -> (Vec<String>, Stream<G, Record>);

    fn equality(
        &self,
        visitor: &mut usize,
        attrs: &Vec<String>,
        target: &String,
        new_attr: &String,
    ) -> (Vec<String>, Stream<G, Record>);

    fn next(
        &self,
        visitor: &mut usize,
        time_stream: &Stream<G, TimeFlowValues>,
        attrs: &Vec<String>,
        interval : TimeInterval
    ) -> (Vec<String>, Stream<G, Record>);

    fn next_zero_inf(
        &self,
        visitor: &mut usize,
        attrs: &Vec<String>
    ) -> (Vec<String>, Stream<G, Record>);

    fn prev(
        &self,
        visitor: &mut usize,
        time_stream: &Stream<G, TimeFlowValues>,
        attrs: &Vec<String>,
        interval : TimeInterval
    ) -> (Vec<String>, Stream<G, Record>);

    fn prev_zero_inf(
        &self,
        visitor: &mut usize,
        time_stream: &Stream<G, TimeFlowValues>,
        attrs: &Vec<String>,
    ) -> (Vec<String>, Stream<G, Record>);

    fn once(
        &self,
        visitor: &mut usize,
        time_stream: &Stream<G, TimeFlowValues>,
        attrs: &Vec<String>,
        interval: TimeInterval,
        deduplication: bool,
    ) -> (Vec<String>, Stream<G, Record>);

    fn once_zero_inf(
        &self,
        visitor: &mut usize,
        time_stream: &Stream<G, TimeFlowValues>,
        attrs: &Vec<String>
    ) -> (Vec<String>, Stream<G, Record>);

    fn eventually(
        &self,
        visitor: &mut usize,
        time_stream: &Stream<G, TimeFlowValues>,
        attrs: &Vec<String>,
        interval: TimeInterval,
        deduplication: bool,
    ) -> (Vec<String>, Stream<G, Record>);

    fn eventually_zero_inf(
        &self,
        visitor: &mut usize,
        attrs: &Vec<String>
    ) -> (Vec<String>, Stream<G, Record>);

    fn neg_since(
        self,
        visitor: &mut usize,
        rhs_stream: Stream<G, Record>,
        time_stream: Stream<G, TimeFlowValues>,
        lhs_attrs: &Vec<String>,
        rhs_attrs: &Vec<String>,
        interval: TimeInterval,
        deduplication: bool,
    ) -> (Vec<String>, Stream<G, Record>);

    fn neg_until(
        self,
        visitor: &mut usize,
        rhs_stream: Stream<G, Record>,
        time_stream: Stream<G, TimeFlowValues>,
        lhs_attrs: &Vec<String>,
        rhs_attrs: &Vec<String>,
        interval: TimeInterval,
        deduplication: bool,
    ) -> (Vec<String>, Stream<G, Record>);
}

impl<G: Scope<Timestamp = usize>> Operators<G> for Stream<G, Record> {
    fn join(&self, visitor: &mut usize, rhs_stream: &Stream<G, Record>, lhs_attrs: &Vec<String>, rhs_attrs: &Vec<String>, ) -> (Vec<String>, Stream<G, Record>) {
        *visitor = visitor.clone() + 1;
        // keys to join on
        let (new_attrs, lhs_key_indices, rhs_key_indices) = find_common_bound_variables2(lhs_attrs.clone(), rhs_attrs.clone());
        //println!("Join {:?}  {:?}     lhs {:?}  rhs: {:?}", lhs_attrs, rhs_attrs, lhs_key_indices, rhs_key_indices);

        // data structures
        let mut lhs_stash = HashMap::new();
        let mut rhs_stash = HashMap::new();
        let mut vector1 = Vec::new();
        let mut vector2 = Vec::new();

        // Key distribution for internal partition
        let lhs_pact = key_exchange_non_temporal!(lhs_key_indices);
        let rhs_pact = key_exchange_non_temporal!(rhs_key_indices);

        let output_stream =
            self.binary_notify(&rhs_stream, lhs_pact, rhs_pact, "Join", None,
              move |lhs_input, rhs_input, output, notificator| {
                  lhs_input.for_each(|time, data| {
                      data.swap(&mut vector1);
                      for rec in vector1.drain(..) {
                          match rec {
                              Data(_, tuple) => {
                                  //println!("LHS:  {:?}", tuple);
                                  let (key, non_key) = split_keys(tuple.clone(), &lhs_key_indices);
                                  save_keyed_tuple(&key, &non_key, *time, &mut lhs_stash);
                                  join_with_original_order(key, non_key, lhs_key_indices.clone(), *time, &rhs_stash, true).into_iter().for_each(|t| {
                                      output.session(&time).give(Data(true, t))
                                  });
                                  /*join_with_other(key, non_key, *time, &rhs_stash, true).into_iter().for_each(|t| {
                                      output.session(&time).give(Data(true, t))
                                  });*/
                              }
                              _ => output.session(&time).give(rec),
                          }
                      };
                      notificator.notify_at(time.delayed(&time.time()));
                  });

                  rhs_input.for_each(|time, data| {
                      data.swap(&mut vector2);
                      for rec in vector2.drain(..) {
                          match rec {
                              Data(_, tuple) => {
                                  //println!("RHS:  {:?}", tuple);
                                  let (key, non_key) = split_keys(tuple.clone(), &rhs_key_indices);
                                  save_keyed_tuple(&key, &non_key, *time, &mut rhs_stash);
                                  join_with_original_order(key, non_key, lhs_key_indices.clone(), *time, &lhs_stash, false).into_iter().for_each(|t| {
                                      output.session(&time).give(Data(true, t))
                                  });
                                  /*join_with_other(key, non_key, *time, &lhs_stash, false).into_iter().for_each(|t| {
                                      output.session(&time).give(Data(true, t))
                                  });*/
                              }
                              _ => output.session(&time).give(rec),
                          }
                      };
                      notificator.notify_at(time.delayed(&time.time()));
                  });

                  while let Some((cap, _tp)) = notificator.next() {
                      let tp = cap.time().clone();
                      lhs_stash.remove(&tp).unwrap_or_default();
                      rhs_stash.remove(&tp).unwrap_or_default();
                  }
              },
        );

        (new_attrs, output_stream)
    }

    fn union(&self, visitor: &mut usize, rhs_stream: &Stream<G, Record>, lhs_attrs: &Vec<String>, rhs_attrs: &Vec<String>, ) -> (Vec<String>, Stream<G, Record>) {
        *visitor = visitor.clone() + 1;
        let (_new_attr_order, lhs_key_indices, rhs_key_indices) =
            find_common_bound_variables2(lhs_attrs.clone(), rhs_attrs.clone());
        let is_equal_key = lhs_attrs == rhs_attrs;

        //println!("Union {:?}  {:?}", lhs_attrs, rhs_attrs);

        let mut acc_data = HashMap::new();
        let lhs_pact = key_exchange_non_temporal!(lhs_key_indices);
        let rhs_pact = key_exchange_non_temporal!(rhs_key_indices);
        let mut vector1 = Vec::new();
        let mut vector2 = Vec::new();

        let output_stream =
            self.binary_notify(
                &rhs_stream, lhs_pact, rhs_pact, "Union", None,
                move |lhs_input, rhs_input, output, notificator| {
                    lhs_input.for_each(|time, data| {
                        data.swap(&mut vector1);
                        for rec in vector1.drain(..) {
                            match rec {
                                Data(_, tuple) => {
                                    //let ordered_tuple = reorder_attributes(tuple, &lhs_key_indices);
                                    let entry = acc_data.entry(time.time().clone()).or_insert_with(||HashSet::with_capacity(8));
                                    if !entry.contains(&tuple) {
                                        output.session(&time).give(Data(true, tuple.clone()));
                                        entry.insert(tuple);
                                    }
                                }
                                _ => output.session(&time).give(rec),
                            }
                        };
                        notificator.notify_at(time.delayed(&time.time()));
                    });

                    rhs_input.for_each(|time, data| {
                        data.swap(&mut vector2);
                        for rec in vector2.drain(..) {
                            match rec {
                                Data(_, tuple) => {
                                    let ordered_tuple = if is_equal_key { tuple } else { reorder_attributes(tuple, &rhs_key_indices) };
                                    let entry = acc_data.entry(time.time().clone()).or_insert_with(||HashSet::with_capacity(8));
                                    if !entry.contains(&ordered_tuple) {
                                        output.session(&time).give(Data(true, ordered_tuple.clone()));
                                        entry.insert(ordered_tuple);
                                    }
                                }
                                _ => output.session(&time).give(rec),
                            }
                        };
                        notificator.notify_at(time.delayed(&time.time()));
                    });

                    while let Some((cap, _tp)) = notificator.next() {
                        let tp = cap.time().clone();
                        acc_data.remove(&tp).unwrap_or_default();
                    }
                },
        );

        (lhs_attrs.clone(), output_stream)
    }

    fn anti_join(&self, visitor: &mut usize, rhs_stream: &Stream<G, Record>, lhs_attrs: &Vec<String>, rhs_attrs: &Vec<String>, ) -> (Vec<String>, Stream<G, Record>) {
        *visitor = visitor.clone() + 1;

        let (_, lhs_key_indices, rhs_key_indices) =
            find_common_bound_variables(lhs_attrs.clone(), rhs_attrs.clone());
        let lhs_pact = key_exchange_non_temporal!(lhs_key_indices);
        let rhs_pact = key_exchange_non_temporal!(rhs_key_indices);

        let mut lhs_stash = HashMap::new();
        let mut rhs_stash = HashMap::new();

        let mut vector1 = Vec::new();
        let mut vector2 = Vec::new();

        let output_stream = self.binary_notify(
            &rhs_stream,
            lhs_pact,
            rhs_pact,
            "Anti Join",
            None,
            move |lhs_input, rhs_input, output, notificator| {
                lhs_input.for_each(|time, data| {
                    data.swap(&mut vector1);
                    for rec in vector1.drain(..) {
                        match rec {
                            Data(_, tuple) => {
                                save_tuple(tuple, *time, &mut lhs_stash);
                            }
                            // (FALSE AND NOT P)
                            _ => output.session(&time).give(rec),
                        }
                    };
                    notificator.notify_at(time.delayed(&time.time()));
                });

                rhs_input.for_each(|time, data| {
                    data.swap(&mut vector2);
                    for rec in vector2.drain(..) {
                        match rec {
                            Data(_, tuple) => {
                                save_tuple(tuple, *time, &mut rhs_stash);
                            }
                            // out put false for example (P AND NOT TRUE)
                            _ => output.session(&time).give(rec),
                        }
                    };
                    notificator.notify_at(time.delayed(&time.time()));
                });

                while let Some((cap, _tp)) = notificator.next() {
                    let tp = cap.time().clone();
                    let lhs = lhs_stash.remove(&(tp)).unwrap_or_default();
                    let rhs = rhs_stash.remove(&(tp)).unwrap_or_default();

                    if !lhs.is_empty() && !rhs.is_empty(){
                        for x in anti_join_difference(lhs, rhs, lhs_key_indices.clone(), rhs_key_indices.clone()) {
                            output.session(&cap).give(Data(true, x))
                        }
                    } else if rhs.is_empty() {
                        for x in lhs {
                            output.session(&cap).give(Data(true, x))
                        }
                    }
                }
            },
        );

        (lhs_attrs.clone(), output_stream)
    }

    fn projection(&self, visitor: &mut usize, attrs: &Vec<String>, unwanted_vars: &Vec<String>, ) -> (Vec<String>, Stream<G, Record>) {
        *visitor = visitor.clone() + 1;
        let (indices, new_attrs) = get_wanted_indices(&attrs, &unwanted_vars);
        let stream = self.map(move |rec| {
            match rec {
                Data(t, tuple) => Data(t, get_wanted_values(tuple, &indices)),
                // forward metadata unchanged
                _ => rec,
            }
        });
        (new_attrs, stream)
    }

    fn since(self, visitor: &mut usize, rhs_stream: Stream<G, Record>, time_stream: Stream<G, TimeFlowValues>, lhs_attrs: &Vec<String>, rhs_attrs: &Vec<String>, interval: TimeInterval, deduplication: bool,) -> (Vec<String>, Stream<G, Record>) {
        // update visitor
        *visitor = visitor.clone() + 1;
        // get the common subset and the location of the key variables in the rhs
        let (_, rhs_index_location) = get_common_variables(lhs_attrs.clone(), rhs_attrs.clone());
        let worker_index = self.scope().index().clone();

        let out = if !deduplication {deduplicated_output} else {not_deduplicated_output};
        // data structures
        // alpha_tuple -> SatisfactionDs
        let mut alphas: HashMap<Vec<Constant>, PartialSequence> = HashMap::with_capacity(8);
        // tp -> ts mapping
        let mut time_table: HashMap<usize, usize> = HashMap::with_capacity(8);
        // Result: save time together with data, in each iteration iterate of each non empty entrance and send data
        let mut results: HashMap<usize, Vec<Record>> = HashMap::with_capacity(8);

        let mut alpha_queue : HashMap<usize, Vec<Vec<Constant>>> = HashMap::with_capacity(8);
        let mut beta_queue : HashMap<usize, Vec<Vec<Constant>>> = HashMap::with_capacity(8);

        let mut unique_beta : HashMap<usize, HashSet<Vec<Constant>>> = HashMap::with_capacity(8);
        let mut unique_res : HashMap<usize, HashSet<Vec<Constant>>> = HashMap::with_capacity(8);

        let mut times : Vec<usize> = Vec::with_capacity(8);

        let output_stream = ternary_frontier(self, rhs_stream, time_stream, Pipeline, Pipeline, Pipeline, move |_, _| {
            let mut time_vec = Vec::new();
            let mut lhs_vec = Vec::new();
            let mut rhs_vec = Vec::new();

            move |time_input, lhs_input,  rhs_input, output| {
                time_input.for_each(|time, data| {
                    let tp = time.time().clone();

                    data.swap(&mut time_vec);
                    for tfv in time_vec.drain(..) {
                        if let TimeFlowValues::Timestamp(ts) = tfv {
                                time_table.entry(tp).or_insert_with(|| ts);
                        }
                    }

                    if let Some(val) = alpha_queue.get_mut(&tp) {
                        for tuple in val.drain(..) {
                            out(process_alpha(tp, &mut time_table, tuple, &mut alphas, interval, true), &mut results, &mut unique_res);
                        }
                    }

                    if let Some(val) = beta_queue.get_mut(&tp) {
                        for tuple in val.drain(..) {
                            out(process_beta(tp, &mut time_table, tuple, rhs_index_location.clone(), &mut alphas, interval, true, &mut unique_beta), &mut results, &mut unique_res);
                        }
                    }

                    times.push(tp);
                    results.iter_mut().for_each(|(tp_tmp, data)| {
                        if tp_tmp >= time.time() {
                            output.session(&time.delayed(tp_tmp)).give_iterator(data.drain(..));
                        }
                    });
                });

                lhs_input.for_each(|time, data| {
                    let tp = time.time().clone();

                    data.swap(&mut lhs_vec);
                    for rec in lhs_vec.drain(..) {
                        match rec {
                            Data(_, tuple) => {
                                // todo try to deduct from context if we can output without knowing the ts
                                if time_table.get(&tp).is_some() {
                                    out(process_alpha(tp, &mut time_table, tuple, &mut alphas, interval, true), &mut results, &mut unique_res);
                                } else {
                                    alpha_queue.entry(tp).or_insert_with(|| Vec::new()).push(tuple);
                                }
                            }
                            // empty ts
                            MetaData(flag, is_true) => {
                                if worker_index == 0 {
                                    output.session(&time).give(MetaData(flag, is_true));
                                }
                            }
                        }
                    };

                    results.iter_mut().for_each(|(tp_tmp, data)| {
                        if tp_tmp >= time.time() {
                            output.session(&time.delayed(tp_tmp)).give_iterator(data.drain(..));
                        }
                    });
                });


                rhs_input.for_each(|time, data| {
                    let tp = time.time().clone();

                    data.swap(&mut rhs_vec);
                    for rec in rhs_vec.drain(..) {
                        match rec {
                            Data(_, tuple) => {
                                if time_table.get(&tp).is_some() {
                                    out(process_beta(tp, &mut time_table, tuple, rhs_index_location.clone(), &mut alphas, interval, true, &mut unique_beta), &mut results, &mut unique_res);
                                } else {
                                    beta_queue.entry(tp).or_insert_with(|| Vec::new()).push(tuple);
                                }
                            }
                            MetaData(flag, is) => {
                                if worker_index == 0 {
                                    output.session(&time).give(MetaData(flag, is))
                                }
                            }
                        }
                    };

                    results.iter_mut().for_each(|(tp_tmp, data)| {
                        if tp_tmp >= time.time() {
                            output.session(&time.delayed(tp_tmp)).give_iterator(data.drain(..));
                        }
                    });
                });

                let frontiers = &[time_input.frontier(), lhs_input.frontier(), rhs_input.frontier()];
                times.retain(|time| {
                    if frontiers.iter().all(|f| !f.less_equal(&time)) {
                        let tp = time.clone();
                        alphas.iter_mut().for_each(|(_, ds)| {
                            ds.safe_remove_subscriber(&mut time_table, interval, tp);
                        });

                        results.remove(&tp);
                        unique_res.remove(&tp).unwrap_or_default();
                        unique_beta.remove(&tp).unwrap_or_default();
                        beta_queue.remove(&tp).unwrap_or_default();
                        alpha_queue.remove(&tp).unwrap_or_default();
                        false
                    } else {
                        true
                    }
                });
            }
        });

        (rhs_attrs.clone(), output_stream)
    }

    fn until(self, visitor: &mut usize, rhs_stream: Stream<G, Record>, time_stream: Stream<G, TimeFlowValues>, lhs_attrs: &Vec<String>, rhs_attrs: &Vec<String>, interval: TimeInterval, deduplication: bool,) -> (Vec<String>, Stream<G, Record>) {
        *visitor = visitor.clone() + 1;
        let (_, rhs_index_location) = get_common_variables(lhs_attrs.clone(), rhs_attrs.clone());
        let worker_index = self.scope().index().clone();

        let out = if !deduplication {deduplicated_output} else {not_deduplicated_output};

        // data structures
        // alpha_tuple -> SatisfactionDs
        let mut alphas: HashMap<Vec<Constant>, PartialSequence> = HashMap::with_capacity(8);
        // tp -> ts mapping
        let mut time_table: HashMap<usize, usize> = HashMap::with_capacity(8);
        let mut ts_to_tp: HashMap<usize, (usize, usize)> = HashMap::with_capacity(8);
        // Result: safe time together with data, in each iteration iterate of each non empty entrance and send data
        let mut results: HashMap<usize, Vec<Record>> = HashMap::with_capacity(8);

        let mut unique_beta : HashMap<usize, HashSet<Vec<Constant>>> = HashMap::with_capacity(8);
        let mut unique_res : HashMap<usize, HashSet<Vec<Constant>>> = HashMap::new();

        let mut alpha_queue : HashMap<usize, Vec<Vec<Constant>>> = HashMap::with_capacity(8);
        let mut beta_queue : HashMap<usize, Vec<Vec<Constant>>> = HashMap::with_capacity(8);

        let mut received = false;
        let mut lowest_tp_ts = 0;

        let mut end_of_stream_lhs = false;
        let mut end_of_stream_tp_lhs = 0;
        let mut end_of_stream_rhs = false;
        let mut end_of_stream_tp_rhs = 0;

        let output_stream = ternary_frontier(self, rhs_stream, time_stream, Pipeline, Pipeline, Pipeline,move |capability,_| {
            //let mut notificator : FrontierNotificator<usize> = FrontierNotificator::new();
            let mut times : Vec<usize> = Vec::with_capacity(8);
            let mut time_vec = Vec::new();
            let mut lhs_vec = Vec::new();
            let mut rhs_vec = Vec::new();
            let mut cap = Some(capability);

            move |time_input, lhs_input,  rhs_input, output| {
                time_input.for_each(|time, data| {
                    let tp = time.time().clone();

                    data.swap(&mut time_vec);
                    for ftv in time_vec.drain(..) {
                        if let TimeFlowValues::Timestamp(ts) = ftv {
                            time_table.entry(tp).or_insert_with(|| ts);
                            if !received {
                                lowest_tp_ts = tp;
                                received = true;

                                ts_to_tp.entry(ts).or_insert_with(|| (tp, tp));
                            } else {
                                lowest_tp_ts = min(lowest_tp_ts, tp);

                                if ts_to_tp.contains_key(&ts) {
                                    if let Some(tup) = ts_to_tp.get_mut(&ts) {
                                        tup.0 = min(tup.0, tp);
                                        tup.1 = max(tup.1, tp);
                                    }
                                } else {
                                    ts_to_tp.entry(ts).or_insert_with(|| (tp, tp));
                                }
                            }
                        }
                    }

                    if let Some(val) = beta_queue.get_mut(&tp) {
                        for tuple in val.drain(..) {
                            out(process_beta(tp, &mut time_table, tuple, rhs_index_location.clone(), &mut alphas, interval, false, &mut unique_beta), &mut results, &mut unique_res);
                        }
                    }

                    if let Some(val) = alpha_queue.get_mut(&tp) {
                        for tuple in val.drain(..) {
                            out(process_alpha(tp, &mut time_table, tuple, &mut alphas, interval, false), &mut results, &mut unique_res);
                        }
                    }

                    times.push(tp);
                });

                lhs_input.for_each(|time, data| {
                    let tp = time.time().clone();

                    data.swap(&mut lhs_vec);
                    for rec in lhs_vec.drain(..) {
                        match rec {
                            Data(_, tuple) => {
                                if time_table.get(&tp).is_some() {
                                    out(process_alpha(tp, &mut time_table, tuple, &mut alphas, interval, false), &mut results, &mut unique_res);
                                } else {
                                    alpha_queue.entry(tp).or_insert_with(|| Vec::new()).push(tuple);
                                }
                            }
                            // empty ts
                            MetaData(flag, is_true) => {
                                if !flag {
                                    end_of_stream_lhs = true;
                                    end_of_stream_tp_lhs = tp;
                                }

                                if worker_index == 0 {
                                    output.session(&time).give(MetaData(flag, is_true));
                                }
                            }
                        }
                    };
                });

                rhs_input.for_each(|time, data| {
                    let tp = time.time().clone();

                    data.swap(&mut rhs_vec);
                    for rec in rhs_vec.drain(..) {
                        match rec {
                            Data(_, tuple) => {
                                if time_table.get(&tp).is_some() {
                                    out(process_beta(tp, &mut time_table, tuple, rhs_index_location.clone(), &mut alphas, interval, false, &mut unique_beta), &mut results, &mut unique_res);
                                } else {
                                    beta_queue.entry(tp).or_insert_with(|| Vec::new()).push(tuple);
                                }
                            }
                            MetaData(flag, is) => {
                                if !flag {
                                    end_of_stream_rhs = true;
                                    end_of_stream_tp_rhs = tp;
                                }

                                if worker_index == 0 {
                                    output.session(&time).give(MetaData(flag, is));
                                }
                            }
                        }
                    };
                });

                results.iter_mut().for_each( |(tp_tmp, data)| {
                    if let Some(cap) = cap.as_mut() {
                        output.session(&cap.delayed(tp_tmp)).give_iterator(data.drain(..));
                    }
                });

                let frontiers = &[time_input.frontier(), lhs_input.frontier(), rhs_input.frontier()];
                let mut remove_times : HashSet<usize> = HashSet::new();
                times.iter().for_each(|time| {
                    if frontiers.iter().all(|f| !f.less_equal(&time)) {
                        let current_tp = time.clone();
                        let has_ts = time_table.contains_key(&current_tp);

                        alphas.iter_mut().for_each(|(_, ds)| {
                            ds.safe_remove_subscriber(&mut time_table, interval, current_tp);
                        });

                        // never release capability if infinite
                        if !interval.is_infinite() && has_ts {
                            let b = interval.get_raw_end();
                            // get the ts of the frontier tp
                            let current_ts = *time_table.entry(current_tp).or_default();

                            if !(b > current_ts) {
                                let cut_off = current_ts - b;
                                let mut to_remove = Vec::new();

                                if let Some((_low, up)) = ts_to_tp.get_mut(&cut_off) {
                                    let range : Vec<usize> = (lowest_tp_ts..(*up+1)).collect();
                                    to_remove = range;
                                }

                                for t in to_remove {
                                    remove_times.insert(t);
                                    if let Some(cap) = cap.as_mut() {
                                        cap.downgrade(&(t+1));
                                        lowest_tp_ts = t+1;
                                    }

                                    results.remove(&t);
                                    unique_res.remove(&t).unwrap_or_default();
                                    unique_beta.remove(&t).unwrap_or_default();
                                    beta_queue.remove(&t).unwrap_or_default();
                                    alpha_queue.remove(&t).unwrap_or_default();
                                }
                            }

                            if end_of_stream_lhs && end_of_stream_rhs && end_of_stream_tp_lhs == end_of_stream_tp_rhs && end_of_stream_tp_rhs == time+1 {
                                cap = None
                            }
                        }
                    }
                });

                times.retain(|time| {
                    !remove_times.contains(time)
                });
            }
        });

        (rhs_attrs.clone(), output_stream)
    }

    fn filter(&self, visitor: &mut usize, attrs: Vec<String>, var: String, val: Arg, ) -> (Vec<String>, Stream<G, Record>) {
        *visitor = visitor.clone() + 1;

        //println!("FILTER {:?}  {:?}", attrs, var);

        let lhs_indices = get_var_indices(&attrs, var);
        let output = match val {
            Arg::Cst(x) => {
                Filter::filter(self, move |rec| -> bool {match rec {
                    Data(_, tuple) => {variable_eq_value(tuple, x.clone(), &lhs_indices)},
                    _ => true,
                }})
            }
            Arg::Var(x) => {
                let rhs_indices = get_var_indices(&attrs, x);
                Filter::filter(self, move |rec| match rec {
                    Data(_, tuple) => variable_eq_variable(tuple, &rhs_indices, &lhs_indices),
                    _ => true,
                })
            }
        };

        (attrs, output)
    }

    fn neg_filter(&self, visitor: &mut usize, attrs: Vec<String>, var: String, val: Arg, ) -> (Vec<String>, Stream<G, Record>) {
        *visitor = visitor.clone() + 1;

        let lhs_indices = get_var_indices(&attrs, var);
        let output = match val {
            Arg::Cst(x) => {
                Filter::filter(self, move |rec| match rec {
                    Data(_, tuple) => !variable_eq_value(tuple, x.clone(), &lhs_indices),
                    _ => true,
                })
            }
            Arg::Var(x) => {
                let rhs_indices = get_var_indices(&attrs, x);
                Filter::filter(self, move |rec| match rec {
                    Data(_, tuple) => !variable_eq_variable(tuple, &rhs_indices, &lhs_indices),
                    _ => true,
                })
            }
        };

        (attrs, output)
    }

    fn equality(&self, visitor: &mut usize, attrs: &Vec<String>, target: &String, new_attr: &String, ) -> (Vec<String>, Stream<G, Record>) {
        *visitor = visitor.clone() + 1;

        let mut target_index = 0;
        for (ind, fv) in attrs.into_iter().enumerate() {
            if fv == target {
                target_index = ind;
            }
        }
        // add the new index at the end and find the index from where to copy
        let mut new_attrs = attrs.clone();
        new_attrs.push(new_attr.clone());

        let stream = self.map(move |rec| {
            match rec {
                Data(t, mut tuple) => {
                    //let mut tup = tuple.clone();
                    tuple.push(tuple[target_index].clone());
                    Data(t, tuple)
                }
                // forward metadata unchanged
                _ => rec,
            }
        });

        (new_attrs, stream)
    }

    fn next(&self, visitor: &mut usize, time_stream: &Stream<G, TimeFlowValues>, attrs: &Vec<String>, interval : TimeInterval) -> (Vec<String>, Stream<G, Record>) {
        *visitor = visitor.clone() + 1;
        let worker_index = self.scope().index().clone();

        let mut tp_to_ts : HashMap<usize, usize> = HashMap::with_capacity(8);
        let mut stash : HashMap<usize, Vec<Vec<Constant>>> = HashMap::new();
        let mut end_of_stream = false;
        let mut end_of_stream_tp = 0;

        let output = self.binary_frontier(time_stream, Pipeline, Pipeline, "Next", |capability,_| {
            let mut times : Vec<usize> = Vec::with_capacity(8);
            let mut time_vec = Vec::new();
            let mut data_vec = Vec::new();
            let mut cap = Some(capability);

            move |data_input, time_input, output| {
                time_input.for_each(|time, data| {
                    let tp = time.time().clone();
                    data.swap(&mut time_vec);
                    for ftv in time_vec.drain(..) {
                        if let TimeFlowValues::Timestamp(ts) = ftv {
                            tp_to_ts.entry(tp).or_insert_with(|| ts);
                            for res in next_predecessor(tp, ts, &mut stash, &mut tp_to_ts, interval) {
                                output.session(&time).give(res);
                            }
                        }
                    }

                    for (tmp_tp, res) in next_empty_stash(&mut stash, &mut tp_to_ts, interval) {
                        if let Some(time) = cap.as_mut() {
                            output.session(&time.delayed(&tmp_tp)).give(res);
                        }
                    }
                    times.push(tp);
                });

                data_input.for_each(|time, data| {
                    let tp = time.time().clone();
                    data.swap(&mut data_vec);
                    for rec in data_vec.drain(..) {
                        match rec {
                            Data(_, tuple) => {
                                if tp_to_ts.contains_key(&tp) {
                                    let ts = *tp_to_ts.entry(tp).or_default();
                                    for res in next_successor(tuple, tp, ts, &mut stash, &mut tp_to_ts, interval) {
                                        if let Some(time) = cap.as_mut() {
                                            output.session(&time.delayed(&(tp-1))).give(res);
                                        }
                                    }

                                    for (tmp_tp, res) in next_empty_stash(&mut stash, &mut tp_to_ts, interval) {
                                        if let Some(time) = cap.as_mut() {
                                            output.session(&time.delayed(&tmp_tp)).give(res);
                                        }
                                    }
                                } else {
                                    stash.entry(tp).or_insert_with(|| Vec::new()).push(tuple);
                                }
                            }
                            MetaData(flag, is) => {
                                if !flag {
                                    end_of_stream = true;
                                    end_of_stream_tp = tp;
                                }
                                if worker_index == 0 {
                                    output.session(&time).give(MetaData(flag, is));
                                }
                            }
                        }
                    };
                });

                let frontiers = &[time_input.frontier(), data_input.frontier()];
                let remove_times : HashSet<usize> = HashSet::new();
                times.iter().for_each(|time| {
                    if frontiers.iter().all(|f| !f.less_equal(&time)) {
                        let current_tp = time.clone();
                        if !interval.is_infinite() && current_tp > 0 {
                            let cut_off = current_tp - 1;
                            if let Some(cap) = cap.as_mut() {
                                if cap.time().clone() < cut_off {
                                    cap.downgrade(&cut_off);
                                }
                            }
                        }
                        if end_of_stream && current_tp+1 == end_of_stream_tp {
                            cap = None;
                        }
                    }
                });

                times.retain(|time| {
                    !remove_times.contains(time)
                });
            }
        });

        (attrs.clone(), output)
    }

    fn next_zero_inf(&self, visitor: &mut usize, attrs: &Vec<String>) -> (Vec<String>, Stream<G, Record>) {
        *visitor = visitor.clone() + 1;
        let worker_index = self.scope().index().clone();

        let mut end_of_stream = false;
        let mut end_of_stream_tp = 0;

        let output = self.unary_frontier(Pipeline, "Next_zero_infinity", |capability,_| {
            let mut data_vec = Vec::new();
            let mut times = Vec::with_capacity(8);
            let mut cap = Some(capability);
            move |data_input, output| {
                data_input.for_each(|time, data| {
                    let tp = time.time().clone();
                    data.swap(&mut data_vec);
                    for rec in data_vec.drain(..) {
                        match rec {
                            Data(_, tuple) => {
                                if tp > 0 {
                                    if let Some(caps) = cap.as_mut() {
                                        output.session(&caps.delayed(&(tp - 1))).give(Data(true, tuple))
                                    }
                                }
                            }
                            MetaData(flag, is) => {
                                if !flag {
                                    end_of_stream = true;
                                    end_of_stream_tp = tp;
                                }
                                if worker_index == 0 {
                                    output.session(&time).give(MetaData(flag, is))
                                }
                            }
                        }
                    }
                    times.push(tp);
                });

                times.retain(|tp| {
                    if [data_input.frontier()].iter().all(|f| !f.less_equal(tp)) {
                        let current_tp = tp.clone();
                        if current_tp > 0 {
                            let cut_off = current_tp - 1;
                            if let Some(cap) = cap.as_mut() {
                                if cap.time().clone() < cut_off {
                                    cap.downgrade(&cut_off);
                                }
                            }
                        }
                        false
                    } else {
                        true
                    }
                });

                if [data_input.frontier()].iter().all(|f| !f.less_equal(&end_of_stream_tp)) {
                    cap = None;
                }
            }
        });
        (attrs.clone(), output)
    }

    fn prev(&self, visitor: &mut usize, time_stream: &Stream<G, TimeFlowValues>, attrs: &Vec<String>, interval : TimeInterval) -> (Vec<String>, Stream<G, Record>) {
        *visitor = visitor.clone() + 1;

        let mut tp_to_ts : HashMap<usize, usize> = HashMap::new();
        let mut stash : HashMap<usize, Vec<Vec<Constant>>> = HashMap::new();
        let worker_index = self.scope().index().clone();

        let output = self.binary_frontier(time_stream, Pipeline, Pipeline, "Prev", |_,_| {
            let mut time_vec = Vec::new();
            let mut data_vec = Vec::new();
            move |data_input, time_input, output| {
                time_input.for_each(|time, data| {
                    let tp = time.time().clone();
                    data.swap(&mut time_vec);
                    for ftv in time_vec.drain(..) {
                        if let TimeFlowValues::Timestamp(ts) = ftv {
                            tp_to_ts.entry(tp).or_insert_with(|| ts);
                            for res in prev_successor(tp, ts, &mut stash, &mut tp_to_ts, interval) {
                                output.session(&time).give(res);
                            }
                        }
                    }

                    for (tmp_tp, res) in prev_empty_stash(&mut stash, &mut tp_to_ts, interval) {
                        if tmp_tp > tp {
                            output.session(&time.delayed(&(tp+1))).give(res);
                        } else {
                            output.session(&time).give(res);
                        }
                    }
                });

                data_input.for_each(|time, data| {
                    let tp = time.time().clone();
                    data.swap(&mut data_vec);
                    for rec in data_vec.drain(..) {
                        match rec {
                            Data(_, tuple) => {
                                if tp_to_ts.contains_key(&tp) {
                                    let ts = *tp_to_ts.entry(tp).or_default();
                                    for res in prev_predecessor(tuple, tp, ts, &mut stash, &mut tp_to_ts, interval) {
                                        output.session(&time.delayed(&(tp + 1))).give(res);
                                    }
                                } else {
                                    stash.entry(tp).or_insert_with(|| Vec::new()).push(tuple);
                                }
                            }
                            MetaData(flag, is) => {
                                if worker_index == 0 {
                                    // true increase by 1 for next send -1 if the interval is satisfied
                                    output.session(&time).give(MetaData(flag, is))
                                }
                            }
                        }
                    };
                });

                stash.retain(|_, entries| {
                    !entries.is_empty()
                });

            }
        });

        (attrs.clone(), output)
    }

    fn prev_zero_inf(&self, visitor: &mut usize, time_stream: &Stream<G, TimeFlowValues>, attrs: &Vec<String>) -> (Vec<String>, Stream<G, Record>) {
        *visitor = visitor.clone() + 1;

        let worker_index = self.scope().index().clone();

        let mut end_of_stream = false;
        let mut end_of_stream_tp = 0;

        let mut max_tp = 0;
        let mut stash : HashMap<usize, Vec<Vec<Constant>>> = HashMap::new();

        let output = self.binary_frontier(time_stream, Pipeline, Pipeline, "Prev_zero_inf", |capability,_| {
            let mut data_vec = Vec::new();
            let mut time_vec = Vec::new();
            let mut times : Vec<usize> = Vec::with_capacity(8);
            let mut cap = Some(capability);

            move |data_input, time_input, output| {
                time_input.for_each(|time, data| {
                    data.swap(&mut time_vec);
                    for rec in time_vec.drain(..) {
                        match rec {
                            TimeFlowValues::Timestamp(_) => {
                                let tp = time.time().clone();
                                if tp >= max_tp {
                                    max_tp = tp;
                                    stash.retain(|k, v| {
                                        if *k < max_tp {
                                            for tup in v.drain(..) {
                                                if let Some(capas) = cap.as_mut() {
                                                    output.session(&capas.delayed(&(k + 1))).give(Data(true, tup));
                                                }
                                            }
                                            false
                                        } else {
                                            true
                                        }
                                    });
                                }
                            }
                            TimeFlowValues::EOS => {}
                        }

                        times.push(time.time().clone());
                        times.sort();
                    }
                });

                data_input.for_each(|time, data| {
                    let tp = time.time().clone();
                    data.swap(&mut data_vec);
                    for rec in data_vec.drain(..) {
                        match rec {
                            Data(_, tuple) => {
                                if tp < max_tp {
                                    if let Some(capas) = cap.as_mut() {
                                        output.session(&capas.delayed(&(tp+1))).give(Data(true, tuple));
                                    }
                                } else {
                                    stash.entry(tp).or_insert_with(|| Vec::new()).push(tuple);
                                }

                                stash.retain(|k, v| {
                                    if *k < max_tp {
                                        for tup in v.drain(..) {
                                            if let Some(capas) = cap.as_mut() {
                                                output.session(&capas.delayed(&(k+1))).give(Data(true, tup));
                                            }
                                        }
                                        false
                                    } else {
                                        true
                                    }
                                });
                                max_tp = max(max_tp, tp);
                            }
                            MetaData(flag, is) => {
                                if !flag {
                                    end_of_stream = true;
                                    end_of_stream_tp = tp;
                                }
                                if worker_index == 0 {
                                    output.session(&time).give(MetaData(flag, is))
                                }
                            }
                        }

                    }
                });

                times.retain(|tp| {
                    if [data_input.frontier(), time_input.frontier()].iter().all(|f| !f.less_equal(tp)) {
                        if let Some(cap) = cap.as_mut() {
                            cap.downgrade(&(tp+1));
                        }
                        false
                    } else {
                        true
                    }
                });

                if [data_input.frontier(), time_input.frontier()].iter().all(|f| !f.less_equal(&end_of_stream_tp)) {
                    cap = None;
                }
            }
        });
        (attrs.clone(), output)
    }



    fn once(&self, visitor: &mut usize, time_stream: &Stream<G, TimeFlowValues>, attrs: &Vec<String>, interval: TimeInterval, deduplication: bool) -> (Vec<String>, Stream<G, Record>) {
        *visitor = visitor.clone() + 1;
        // get the common subset and the location of the key variables in the rhs
        let worker_index = self.scope().index().clone();
        // data structures
        let mut obs_seq = ObservationSequence::init();
        // tp -> ts mapping
        let mut tp_to_ts: HashMap<usize, usize> = HashMap::with_capacity(8);
        // Result: save time together with data, in each iteration iterate of each non empty entrance and send data
        let mut results: HashMap<usize, Vec<Record>> = HashMap::with_capacity(8);

        // beta tp, beta ts, upper bound, lower bound, produced
        // tuple -> Vec<(beta_tp : usize, lower_bound_output : usize, upper_bound_output : usize)>
        let mut new_beta : HashMap<usize, HashMap<Vec<Constant>, (usize, usize, usize, bool)>> = HashMap::with_capacity(8);
        let mut unique_beta : HashMap<usize, HashSet<Vec<Constant>>> = HashMap::with_capacity(8);
        let mut unique_res : HashMap<usize, HashSet<Vec<Constant>>> = HashMap::with_capacity(8);

        let mut received = false;
        let mut highest_tp_ts = 0;

        let mut eos_tp = 0;
        let mut eos_flag = false;

        let out = if !deduplication {stream_deduplicated_output} else {stream_not_deduplicated_output};

        let output_stream = self.binary_frontier(time_stream, Pipeline, Pipeline, "Once", move |capability, _info| {
            let mut notificator = FrontierNotificator::new();
            let mut times : Vec<usize> = Vec::with_capacity(8);
            let mut data_vec = Vec::new();
            let mut time_vec = Vec::new();
            let mut cap = Some(capability);

            move |data_input, time_input, output| {
                time_input.for_each(|time, data| {
                    let tp = time.time().clone();
                    notificator.notify_at(time.delayed(&tp));

                    let mut relevant_tp : HashSet<usize> = HashSet::with_capacity(4);
                    // the betas at the tp also need to know about the new ts/tp
                    relevant_tp.insert(tp);
                    data.swap(&mut time_vec);
                    for ftv in time_vec.drain(..) {
                        if let TimeFlowValues::Timestamp(ts) = ftv {
                            tp_to_ts.entry(tp).or_insert_with(|| ts);
                            obs_seq.insert(tp, ts);
                            if !received {
                                highest_tp_ts = tp;
                                received = true;
                            } else {
                                highest_tp_ts = max(highest_tp_ts, tp);
                            }

                            let a = interval.get_raw_start();
                            let lower = if a > ts { None } else { Some(ts - a) };
                            if interval.is_infinite() {
                                if lower.is_some() {
                                    match obs_seq.associated_upper_bound_ts(lower.unwrap()) {
                                        Literal(a) => {
                                            for i in 0..a + 1 {
                                                relevant_tp.insert(i);
                                            }
                                        }
                                        InfinityIntervalsReturn::Interval(a, b) => {
                                            for i in a..b + 1 { relevant_tp.insert(i); }
                                        }
                                        _ => {}
                                    };
                                }
                            } else {
                                let b = interval.get_raw_end();
                                let upper = if b > ts { None } else { Some(ts - b) };

                                if lower.is_some() && upper.is_some() {
                                    //println!("   Search space {}   {}", upper.unwrap(), lower.unwrap());
                                    match obs_seq.associated_interval_ts_exact(upper.unwrap(), lower.unwrap()) {
                                        Literal(a) => { relevant_tp.insert(a); }
                                        InfinityIntervalsReturn::Interval(a, b) => {
                                            for i in a..b + 1 { relevant_tp.insert(i); }
                                        }
                                        _ => {}
                                    };
                                } else if lower.is_some() {
                                    //println!("   Search space {}", lower.unwrap());
                                    match obs_seq.associated_upper_bound_ts(lower.unwrap()) {
                                        Literal(a) => {
                                            for i in 0..a + 1 {
                                                relevant_tp.insert(i);
                                            }
                                        }
                                        InfinityIntervalsReturn::Interval(a, b) => {
                                            for i in a..b + 1 { relevant_tp.insert(i); }
                                        }
                                        _ => {}
                                    };
                                }
                            }
                        }
                    }

                    if interval.is_infinite() {
                        for (rec, range) in process_once_time_infinite(&mut new_beta, &mut obs_seq, interval, highest_tp_ts, relevant_tp) {
                            if !range.is_empty() {
                                let mut vals = Vec::with_capacity(range.len());
                                for x in range {
                                    vals.push(x);
                                }
                                out(vals, rec, &mut results, &mut unique_res);
                            }
                        }
                    } else {
                        for (rec, range) in process_once_time_finite(&mut new_beta, &mut obs_seq, interval, relevant_tp) {
                            if !range.is_empty() {
                                let mut vals = Vec::with_capacity(range.len());
                                for x in range {
                                    vals.push(x);
                                }
                                out(vals, rec, &mut results, &mut unique_res);
                            }
                        }
                    }

                    times.push(tp);
                    times.sort();
                });

                data_input.for_each(|time, data| {
                    let tp = time.time().clone();
                    notificator.notify_at(time.delayed(&tp));
                    data.swap(&mut data_vec);
                    for rec in data_vec.drain(..) {
                        match rec {
                            // Process Beta
                            Data(_, tuple) => {
                                let has_tp = unique_beta.contains_key(&tp);
                                let is_contained = if has_tp {
                                    unique_beta.entry(tp).or_insert_with(|| HashSet::new()).contains(&tuple)
                                }else { false };

                                if !is_contained {
                                    unique_beta.entry(tp).or_insert_with(|| HashSet::new()).insert(tuple.clone());
                                    // beta part, subscribe beta
                                    if interval.is_infinite() {
                                        // infinite interval
                                        let vals = process_once_data_infinite(tuple.clone(), tp, highest_tp_ts, interval, &mut new_beta, &mut obs_seq);
                                        if !vals.is_empty() {
                                            out(vals, tuple, &mut results, &mut unique_res);
                                        }
                                    } else {
                                        // finite interval
                                        let vals = process_once_data_finite(tuple.clone(), tp, interval, &mut new_beta, &mut obs_seq);
                                        if !vals.is_empty() {
                                            out(vals, tuple, &mut results, &mut unique_res);
                                        }
                                    }
                                }
                            }
                            MetaData(flag, is) => {
                                if !flag {
                                    eos_flag = true;
                                    eos_tp = tp;
                                }
                                if worker_index == 0 {
                                    output.session(&time).give(MetaData(flag, is));
                                }
                            }
                        }
                    };
                });

                // send result at respective time point
                results.iter_mut().for_each(|(tp_tmp, data)| {
                    if let Some(cap) = cap.as_mut() {
                        output.session(&cap.delayed(tp_tmp)).give_iterator(data.drain(..));
                    }
                });

                /*let frontiers = &[time_input.frontier(), data_input.frontier()];
                times.retain(|time| {
                    if frontiers.iter().all(|f| !f.less_equal(&time)) {
                        let tp = time.clone();

                        if let Some(cap) = cap.as_mut() {
                            cap.downgrade(&(tp+1));
                        }

                        // no clean up if infinite
                        if !interval.is_infinite() {
                            if let Some(frontier_ts) = tp_to_ts.get(&tp) {
                                // compute highest tp
                                let highest_ts = frontier_ts + interval.get_raw_end();
                                new_beta.retain(|k, _v| {
                                    if let Some(beta_ts) = tp_to_ts.get(k) {
                                        let beta_high = beta_ts + interval.get_raw_end();
                                        !(highest_ts > beta_high)
                                    } else {
                                        true
                                    }
                                });
                            }
                        }

                        results.remove(&tp);
                        obs_seq.clean_up(tp, *tp_to_ts.entry(tp).or_default());
                        unique_res.remove(&tp).unwrap_or_default();
                        unique_beta.remove(&tp).unwrap_or_default();

                        if eos_flag && eos_tp == tp+1 {
                            cap = None;
                        }
                        false
                    } else {
                        true
                    }
                });*/

                notificator.for_each(&[time_input.frontier(), data_input.frontier()], |time, _| {
                    let tp = time.time().clone();

                    if let Some(cap) = cap.as_mut() {
                        cap.downgrade(&(tp+1));
                    }

                    // no clean up if infinite
                    if !interval.is_infinite() {
                        if let Some(frontier_ts) = tp_to_ts.get(&tp) {
                            // compute highest tp
                            let highest_ts = frontier_ts + interval.get_raw_end();
                            new_beta.retain(|k, _v| {
                                if let Some(beta_ts) = tp_to_ts.get(k) {
                                    let beta_high = beta_ts + interval.get_raw_end();
                                    !(highest_ts > beta_high)
                                } else {
                                    true
                                }
                            });
                        }
                    }

                    results.remove(&tp);
                    obs_seq.clean_up(tp, *tp_to_ts.entry(tp).or_default());
                    unique_res.remove(&tp).unwrap_or_default();
                    unique_beta.remove(&tp).unwrap_or_default();

                    if eos_flag && eos_tp == tp {
                        cap = None;
                    }
                });
            }
        });

        (attrs.clone(), output_stream)
    }

    fn once_zero_inf(&self, visitor: &mut usize, time_stream: &Stream<G, TimeFlowValues>, attrs: &Vec<String>) -> (Vec<String>, Stream<G, Record>) {
        *visitor = visitor.clone() + 1;
        // get the common subset and the location of the key variables in the rhs
        let worker_index = self.scope().index().clone();
        let mut betas : HashMap<Vec<Constant>, (usize, usize)> = HashMap::new();

        let mut end_of_stream = false;
        let mut end_of_stream_tp = 0;

        let mut max_tp = 0;

        let output = self.binary_frontier(&time_stream, Pipeline, Pipeline, "Once_zero_inf", |capability, _| {
            let mut data_vec = Vec::new();
            let mut time_vec = Vec::new();
            let mut cap = Some(capability);
            let mut times = Vec::with_capacity(8);

            move |data_input, time_input, output| {
                time_input.for_each(|time, data| {
                    let tp = time.time().clone();
                    data.swap(&mut time_vec);
                    for rec in time_vec.drain(..) {
                        match rec {
                            TimeFlowValues::Timestamp(_) => {
                                if tp >= max_tp {
                                    max_tp = tp;
                                    let mut update = Vec::new();
                                    for (key, (low, high)) in betas.iter_mut() {
                                        if high.clone() < max_tp {
                                            update.push((key.clone(), (*low, max_tp)));
                                            if let Some(capas) = cap.as_mut() {
                                                for v in *high+1..max_tp+1 {
                                                    output.session(&capas.delayed(&v)).give(Data(true, key.clone()));
                                                }
                                            }
                                        }
                                    }

                                    for (key, tup) in update {
                                        betas.insert(key, tup);
                                    }
                                }
                            }
                            TimeFlowValues::EOS => {}
                        }

                        times.push(time.time().clone());
                        times.sort();
                    }
                });

                data_input.for_each(|time, data| {
                    let tp = time.time().clone();
                    data.swap(&mut data_vec);
                    for rec in data_vec.drain(..) {
                        match rec {
                            Data(_, tuple) => {
                                if tp > max_tp {
                                    // produce new output for all existing alphas
                                    let mut update = Vec::new();
                                    for (key, (low, high)) in betas.iter_mut() {
                                        if high.clone() < tp {
                                            update.push((key.clone(), (*low, tp)));
                                            if let Some(capas) = cap.as_mut() {
                                                for v in *high+1..tp+1 {
                                                    output.session(&capas.delayed(&v)).give(Data(true, key.clone()));
                                                }
                                            }
                                        }
                                    }

                                    for (key, tup) in update {
                                        betas.insert(key, tup);
                                    }
                                }

                                max_tp = max(max_tp, tp);

                                if betas.contains_key(&tuple) {
                                    // potentially produce new output for a existing beta
                                    let (current_tp, highest_tp) = *betas.entry(tuple.clone()).or_default();
                                    if tp < current_tp {
                                        betas.insert(tuple.clone(), (tp, highest_tp));
                                        for v in tp..current_tp {
                                            output.session(&time.delayed(&v)).give(Data(true,tuple.clone()));
                                        }
                                    }
                                } else {
                                    // add new beta
                                    betas.insert(tuple.clone(), (tp, max_tp));
                                    for v in tp..max_tp+1 {
                                        output.session(&time.delayed(&v)).give(Data(true, tuple.clone()));
                                    }
                                }
                            }
                            MetaData(flag, is) => {
                                if !flag {
                                    end_of_stream = true;
                                    end_of_stream_tp = tp;
                                }
                                if worker_index == 0 {
                                    output.session(&time).give(MetaData(flag, is));
                                }
                            }
                        }
                    }

                    times.push(tp);
                    times.sort();
                });

                let frontiers = &[data_input.frontier(), time_input.frontier()];
                times.retain(|time| {
                    if frontiers.iter().all(|f| !f.less_equal(&time)) {
                        let tp = time.clone();
                        if let Some(cap) = cap.as_mut() {
                            cap.downgrade(&(tp+1));
                        }
                        false
                    } else {
                        true
                    }
                });

                if frontiers.iter().all(|f| !f.less_equal(&end_of_stream_tp)) {
                    cap = None;
                }
            }
        });
        (attrs.clone(), output)
    }

    fn eventually(&self, visitor: &mut usize, time_stream: &Stream<G, TimeFlowValues>, attrs: &Vec<String>, interval: TimeInterval, deduplication: bool) -> (Vec<String>, Stream<G, Record>) {
        *visitor = visitor.clone() + 1;
        // get the common subset and the location of the key variables in the rhs
        let worker_index = self.scope().index().clone();
        // data structures
        let mut obs_seq = ObservationSequence::init();
        // tp -> ts mapping
        let mut tp_to_ts: HashMap<usize, usize> = HashMap::with_capacity(8);
        let mut ts_to_tp: HashMap<usize, (usize, usize)> = HashMap::with_capacity(8);
        // Result: save time together with data, in each iteration iterate of each non empty entrance and send data
        let mut results: HashMap<usize, Vec<Record>> = HashMap::with_capacity(8);

        // beta tp, beta ts, upper bound, lower bound, produced
        // tuple -> Vec<(beta_tp : usize, lower_bound_output : usize, upper_bound_output : usize)>
        let mut new_beta : HashMap<usize, HashMap<Vec<Constant>, (usize, usize, usize, bool)>> = HashMap::with_capacity(8);
        let mut unique_beta : HashMap<usize, HashSet<Vec<Constant>>> = HashMap::with_capacity(8);
        let mut unique_res : HashMap<usize, HashSet<Vec<Constant>>> = HashMap::with_capacity(8);

        let mut received = false;
        let mut lowest_tp_ts = 0;
        let mut highest_tp_ts = 0;

        let mut eos_flag = false;
        let mut eos_tp = 0;

        let out = if !deduplication {stream_deduplicated_output} else {stream_not_deduplicated_output};

        let output_stream = self.binary_frontier(time_stream, Pipeline, Pipeline, "Eventually", move |capability, _info| {
            let mut notificator = FrontierNotificator::new();
            let mut data_vec = Vec::new();
            let mut time_vec = Vec::new();
            let mut cap = Some(capability);

            move |data_input, time_input, output| {
                time_input.for_each(|time, data| {
                    let tp = time.time().clone();
                    notificator.notify_at(time.delayed(&tp));
                    //println!("Time stream @{}", tp);

                    let mut relevant_tp : HashSet<usize> = HashSet::with_capacity(4);
                    // the betas at the tp also need to know about the new ts/tp
                    relevant_tp.insert(tp);
                    data.swap(&mut time_vec);
                    for ftv in time_vec.drain(..) {
                        if let TimeFlowValues::Timestamp(ts) = ftv {
                            tp_to_ts.entry(tp).or_insert_with(|| ts);
                            obs_seq.insert(tp, ts);
                            if !received {
                                lowest_tp_ts = tp;
                                highest_tp_ts = tp;
                                received = true;

                                ts_to_tp.entry(ts).or_insert_with(|| (tp, tp));
                            } else {
                                lowest_tp_ts = min(lowest_tp_ts, tp);
                                highest_tp_ts = max(highest_tp_ts, tp);

                                if ts_to_tp.contains_key(&ts) {
                                    if let Some(tup) = ts_to_tp.get_mut(&ts) {
                                        tup.0 = min(tup.0, tp);
                                        tup.1 = max(tup.1, tp);
                                    }
                                } else {
                                    ts_to_tp.entry(ts).or_insert_with(|| (tp, tp));
                                }
                            }

                            let lower = interval.get_raw_start() + ts;
                            if interval.is_infinite() {
                                match obs_seq.associated_upper_bound_ts(lower) {
                                    Literal(a) => { for i in a..highest_tp_ts + 1 { relevant_tp.insert(i); } }
                                    InfinityIntervalsReturn::Interval(a, _) => { for i in a..highest_tp_ts + 1 { relevant_tp.insert(i); } }
                                    _ => {}
                                };
                            } else {
                                let upper = interval.get_raw_end() + ts;
                                //println!("   Search space {}   {}", upper.unwrap(), lower.unwrap());
                                match obs_seq.associated_interval_ts_exact(lower, upper) {
                                    Literal(a) => {
                                        for i in a..highest_tp_ts + 1 { relevant_tp.insert(i); }
                                    }
                                    InfinityIntervalsReturn::Interval(a, b) => {
                                        for i in a..b + 1 { relevant_tp.insert(i); }
                                    }
                                    _ => {}
                                };
                            }
                        }
                    }

                    if interval.is_infinite() {
                        for (rec, range) in process_eventually_time_infinite(&mut new_beta, &mut obs_seq, interval, relevant_tp) {
                            if !range.is_empty() {
                                let mut vals = Vec::with_capacity(range.len());
                                for x in range {
                                    vals.push(x);
                                }
                                out(vals, rec, &mut results, &mut unique_res);
                            }
                        }
                    } else {
                        for (rec, range) in process_eventually_time_finite(&mut new_beta, &mut obs_seq, interval, relevant_tp) {
                            if !range.is_empty() {
                                let mut vals = Vec::with_capacity(range.len());
                                for x in range {
                                    vals.push(x);
                                }
                                out(vals, rec, &mut results, &mut unique_res);
                            }
                        }
                    }
                });

                data_input.for_each(|time, data| {
                    let tp = time.time().clone();
                    notificator.notify_at(time.delayed(&tp));
                    data.swap(&mut data_vec);
                    for rec in data_vec.drain(..) {
                        match rec {
                            // Process Beta
                            Data(_, tuple) => {
                                // beta part, subscribe beta
                                let has_tp = unique_beta.contains_key(&tp);
                                let is_contained = if has_tp {
                                    unique_beta.entry(tp).or_insert_with(|| HashSet::new()).contains(&tuple)
                                }else { false };

                                if !is_contained {
                                    unique_beta.entry(tp).or_insert_with(|| HashSet::new()).insert(tuple.clone());
                                    if interval.is_infinite() {
                                        // infinite interval
                                        let vals = process_eventually_data_infinite(tuple.clone(), tp, interval, &mut new_beta, &mut obs_seq);
                                        if !vals.is_empty() {
                                            out(vals, tuple, &mut results, &mut unique_res);
                                        }
                                    } else {
                                        // finite interval
                                        let vals = process_eventually_data_finite(tuple.clone(), tp, interval, &mut new_beta, &mut obs_seq);
                                        if !vals.is_empty() {
                                            out(vals, tuple, &mut results, &mut unique_res);
                                        }
                                    }
                                }
                            }
                            MetaData(flag, is) => {
                                if !flag {
                                    eos_flag = true;
                                    eos_tp = tp;
                                }
                                if worker_index == 0 {
                                    output.session(&time).give(MetaData(flag, is));
                                }
                            }
                        }
                    };
                });

                // send result at respective time point
                results.iter_mut().for_each(|(tp_tmp, data)| {
                    if let Some(cap) = cap.as_mut() {
                        output.session(&cap.delayed(tp_tmp)).give_iterator(data.drain(..));
                    }
                });

                notificator.for_each(&[time_input.frontier(), data_input.frontier()], |time, _| {
                    let current_tp = time.time().clone();
                    let has_ts = tp_to_ts.contains_key(&current_tp);

                    // never release capability if infinite
                    if !interval.is_infinite() && has_ts {
                        let b = interval.get_raw_end();

                        // get the ts of the frontier tp
                        let current_ts = *tp_to_ts.entry(current_tp).or_default();

                        // get all tps for this ts and below
                        if !(b > current_ts) {
                            let cut_off = current_ts - b;
                            let mut to_remove = Vec::new();

                            if let Some((_low, up)) = ts_to_tp.get_mut(&cut_off) {
                                let range : Vec<usize> = (lowest_tp_ts..(*up+1)).collect();
                                to_remove = range;
                            }

                            for t in to_remove {
                                if let Some(cap) = cap.as_mut() {
                                    cap.downgrade(&(t+1));
                                    lowest_tp_ts = t+1;
                                }

                                new_beta.remove(&t);
                                unique_res.remove(&t).unwrap_or_default();
                                unique_beta.remove(&t).unwrap_or_default();
                                results.remove(&t);
                                obs_seq.clean_up(t, *tp_to_ts.entry(t).or_default());
                            }
                        }
                    }
                    //panic!("@{}", current_tp);
                    if eos_flag && current_tp == eos_tp {
                        //panic!("@{}", current_tp);
                        cap = None;
                    }
                });
            }
        });

        (attrs.clone(), output_stream)
    }

    fn eventually_zero_inf(&self, visitor: &mut usize, attrs: &Vec<String>) -> (Vec<String>, Stream<G, Record>) {
        *visitor = visitor.clone() + 1;
        // get the common subset and the location of the key variables in the rhs
        let worker_index = self.scope().index().clone();
        let mut unique_res : HashMap<Vec<Constant>, usize> = HashMap::with_capacity(8);

        let mut end_of_stream = false;
        let mut end_of_stream_tp = 0;

        let output = self.unary_frontier(Pipeline, "Eventually_zero_inf", |capability, _| {
            let mut data_vec = Vec::new();
            let mut cap = Some(capability);

            move |input, output| {
                input.for_each(|time, data| {
                    let tp = time.time().clone();
                    data.swap(&mut data_vec);
                    for rec in data_vec.drain(..) {
                        match rec {
                            Data(_, tuple) => {
                                let range = if unique_res.contains_key(&tuple) {
                                    let curr_last = unique_res.entry(tuple.clone()).or_default().clone();
                                    if tp >= curr_last {
                                        unique_res.insert(tuple.clone(), tp + 1);
                                        curr_last..tp+1
                                    } else { 0..0 }
                                } else {
                                    unique_res.insert(tuple.clone(), tp+1);
                                    0..tp+1
                                };

                                if let Some(cap) = cap.as_mut() {
                                    for v in range {
                                        output.session(&cap.delayed(&v)).give(Data(true, tuple.clone()));
                                    }
                                }
                            }
                            MetaData(flag, is) => {
                                if !flag {
                                    end_of_stream = true;
                                    end_of_stream_tp = tp;
                                }
                                if worker_index == 0 {
                                    output.session(&time).give(MetaData(flag, is));
                                }
                            }
                        }
                    }
                });

                if [input.frontier()].iter().all(|f| !f.less_equal(&end_of_stream_tp)) {
                    cap = None;
                }
            }
        });
        (attrs.clone(), output)
    }

    fn neg_since(self, visitor: &mut usize, rhs_stream: Stream<G, Record>, time_stream: Stream<G, TimeFlowValues>, lhs_attrs: &Vec<String>, rhs_attrs: &Vec<String>, interval: TimeInterval, deduplication: bool) -> (Vec<String>, Stream<G, Record>) {
        // update visitor
        *visitor = visitor.clone() + 1;
        // get the common subset and the location of the key variables in the rhs
        let (_, rhs_index_location) = get_common_variables(lhs_attrs.clone(), rhs_attrs.clone());
        let worker_index = self.scope().index().clone();

        let out = if !deduplication {deduplicated_output} else {not_deduplicated_output};
        // data structures
        // alpha_tuple -> SatisfactionDs
        let mut alphas: HashMap<Vec<Constant>, (PartialSequence, HashMap<usize, Vec<Vec<Constant>>>)> = HashMap::with_capacity(8);
        // tp -> ts mapping
        let mut time_table: HashMap<usize, usize> = HashMap::with_capacity(8);
        // Result: save time together with data, in each iteration iterate of each non empty entrance and send data
        let mut results: HashMap<usize, Vec<Record>> = HashMap::with_capacity(8);
        let mode = true;

        let mut unique_beta : HashMap<usize, HashSet<Vec<Constant>>> = HashMap::with_capacity(8);
        let mut unique_res : HashMap<usize, HashSet<Vec<Constant>>> = HashMap::with_capacity(8);

        let mut rhs_eos_tp = 0;
        let mut rhs_eos = true;

        let mut lhs_eos_tp = 0;
        let mut lhs_eos = true;

        let output_stream = ternary_frontier(self, rhs_stream, time_stream, Pipeline, Pipeline, Pipeline, move |_, _| {
            let mut time_vec = Vec::new();
            let mut lhs_vec = Vec::new();
            let mut rhs_vec = Vec::new();
            let mut notificator = FrontierNotificator::new();

            move |time_input, lhs_input,  rhs_input, output| {
                time_input.for_each(|time, data| {
                    let tp = time.time().clone();
                    notificator.notify_at(time.delayed(&tp));
                    data.swap(&mut time_vec);
                    for ftv in time_vec.drain(..) {
                        if let TimeFlowValues::Timestamp(ts) = ftv {
                            time_table.entry(tp).or_insert_with(|| ts);
                        }
                    }
                });

                lhs_input.for_each(|time, data| {
                    let tp = time.time().clone();
                    notificator.notify_at(time.delayed(&tp));
                    data.swap(&mut lhs_vec);
                    for rec in lhs_vec.drain(..) {
                        match rec {
                            Data(_, tuple) => {
                                if !alphas.contains_key(&tuple) {
                                    let mut ds = PartialSequence::empty(mode);
                                    ds.insert(tp);
                                    alphas.insert(tuple.clone(), (ds, HashMap::new()));
                                } else {
                                    alphas.entry(tuple.clone()).or_default().0.insert(tp);
                                }
                            }
                            // empty ts
                            MetaData(flag, is_true) => {
                                if !flag {
                                    lhs_eos_tp = tp;
                                    lhs_eos = true;
                                }
                                if worker_index == 0 {
                                    output.session(&time).give(MetaData(flag, is_true));
                                }
                            }
                        }
                    };
                });


                rhs_input.for_each(|time, data| {
                    let tp = time.time().clone();
                    notificator.notify_at(time.delayed(&tp));
                    data.swap(&mut rhs_vec);
                    for rec in rhs_vec.drain(..) {
                        match rec {
                            Data(_, tuple) => {
                                // add betas
                                let key_assignments = split_keys_values(tuple.clone(), rhs_index_location.clone());

                                let tp_exists = unique_beta.contains_key(&tp);
                                let beta_exists = unique_beta.entry(tp).or_default().contains(&tuple);

                                if !(tp_exists && beta_exists) {
                                        // get alpha
                                        if !alphas.contains_key(&key_assignments) {
                                            let mut tmp = HashMap::with_capacity(2);
                                            tmp.insert(tp, vec![tuple.clone()]);
                                            alphas.insert(key_assignments.clone(), (PartialSequence::empty(mode), tmp));
                                        } else {
                                            // add subscriber
                                            let (_, map) = alphas.entry(key_assignments).or_default();
                                            map.entry(tp).or_default().push(tuple.clone());
                                        }

                                        if interval.get_raw_start() == 0 {
                                            //println!("Send at RHS: @{tp}  {:?}", tuple);
                                            output.session(&time).give(Data(true, tuple.clone()));
                                            unique_res.entry(tp).or_default().insert(tuple.clone());
                                        }

                                        // add new beta
                                        unique_beta.entry(tp).or_default().insert(tuple);
                                }
                            }
                            MetaData(flag, is) => {
                                if !flag {
                                    rhs_eos_tp = tp;
                                    rhs_eos = true;
                                }
                                if worker_index == 0 {
                                    output.session(&time).give(MetaData(flag, is))
                                }
                            }
                        }
                    };
                });

                notificator.for_each(&[time_input.frontier(), lhs_input.frontier(), rhs_input.frontier()],|time, _| {
                    let tp = time.time().clone();

                    if !(rhs_eos && lhs_eos && rhs_eos_tp == lhs_eos_tp && lhs_eos_tp == tp) {
                        // given the alphas and betas, eliminate betas and or output values
                        out(anti_temporal_since(tp, &mut alphas, &mut time_table, interval), &mut results, &mut unique_res);

                        // send results
                        results.iter_mut().for_each(|(tp_tmp, data)| {
                            output.session(&time.delayed(tp_tmp)).give_iterator(data.drain(..));
                        });

                        results.remove(&tp);
                        unique_beta.remove(&tp);
                        unique_res.remove(&tp);
                    }
                });
            }
        });

        (rhs_attrs.clone(), output_stream)
    }

    fn neg_until(self, visitor: &mut usize, rhs_stream: Stream<G, Record>, time_stream: Stream<G, TimeFlowValues>, lhs_attrs: &Vec<String>, rhs_attrs: &Vec<String>, interval: TimeInterval, deduplication: bool) -> (Vec<String>, Stream<G, Record>) {
        // update visitor
        *visitor = visitor.clone() + 1;
        // get the common subset and the location of the key variables in the rhs
        let (_, rhs_index_location) = get_common_variables(lhs_attrs.clone(), rhs_attrs.clone());
        let worker_index = self.scope().index().clone();

        let out = if !deduplication {deduplicated_output} else {not_deduplicated_output};
        // data structures
        // alpha_tuple -> SatisfactionDs
        let mut alphas: HashMap<Vec<Constant>, (PartialSequence, HashMap<usize, Vec<Vec<Constant>>>)> = HashMap::with_capacity(8);
        // tp -> ts mapping
        let mut time_table: HashMap<usize, usize> = HashMap::with_capacity(8);
        let mut ts_to_tp: HashMap<usize, (usize, usize)> = HashMap::with_capacity(8);
        // Result: save time together with data, in each iteration iterate of each non empty entrance and send data
        let mut results: HashMap<usize, Vec<Record>> = HashMap::with_capacity(8);
        let mode = false;

        let mut unique_beta : HashMap<usize, HashSet<Vec<Constant>>> = HashMap::with_capacity(8);
        let mut unique_res : HashMap<usize, HashSet<Vec<Constant>>> = HashMap::with_capacity(8);

        let mut rhs_eos_tp = 0;
        let mut rhs_eos = false;
        let mut lhs_eos_tp = 0;
        let mut lhs_eos = false;

        let mut received = false;
        let mut lowest_tp_ts = 0;

        let mut alpha_control : HashSet<usize> = HashSet::with_capacity(4);

        let output_stream = ternary_frontier(self, rhs_stream, time_stream, Pipeline, Pipeline, Pipeline, move |capability, _| {
            let mut cap = Some(capability);
            let mut time_vec = Vec::new();
            let mut lhs_vec = Vec::new();
            let mut rhs_vec = Vec::new();
            let mut notificator = FrontierNotificator::new();

            move |time_input, lhs_input,  rhs_input, output| {
                time_input.for_each(|time, data| {
                    let tp = time.time().clone();
                    notificator.notify_at(time.delayed(&tp));
                    data.swap(&mut time_vec);
                    for ftv in time_vec.drain(..) {
                        if let TimeFlowValues::Timestamp(ts) = ftv {
                            time_table.entry(tp).or_insert_with(|| ts);
                            if !received {
                                lowest_tp_ts = tp;
                                received = true;
                                ts_to_tp.entry(ts).or_insert_with(|| (tp, tp));
                            } else {
                                lowest_tp_ts = min(lowest_tp_ts, tp);
                                if ts_to_tp.contains_key(&ts) {
                                    if let Some(tup) = ts_to_tp.get_mut(&ts) {
                                        tup.0 = min(tup.0, tp);
                                        tup.1 = max(tup.1, tp);
                                    }
                                } else {
                                    ts_to_tp.entry(ts).or_insert_with(|| (tp, tp));
                                }
                            }
                        }
                    }
                });

                lhs_input.for_each(|time, data| {
                    let tp = time.time().clone();
                    notificator.notify_at(time.delayed(&tp));
                    data.swap(&mut lhs_vec);
                    for rec in lhs_vec.drain(..) {
                        match rec {
                            Data(_, tuple) => {
                                if !alphas.contains_key(&tuple) {
                                    let mut ds = PartialSequence::empty(mode);
                                    ds.insert(tp);
                                    alphas.insert(tuple.clone(), (ds, HashMap::new()));
                                } else {
                                    alphas.entry(tuple.clone()).or_default().0.insert(tp);
                                }

                                alpha_control.insert(tp);
                            }
                            // empty ts
                            MetaData(flag, is_true) => {
                                if !flag {
                                    lhs_eos_tp = tp;
                                    lhs_eos = true;
                                }
                                if worker_index == 0 {
                                    output.session(&time).give(MetaData(flag, is_true));
                                }
                            }
                        }
                    };
                });

                rhs_input.for_each(|time, data| {
                    let tp = time.time().clone();
                    notificator.notify_at(time.delayed(&tp));
                    data.swap(&mut rhs_vec);
                    for rec in rhs_vec.drain(..) {
                        match rec {
                            Data(_, tuple) => {
                                // add betas
                                let key_assignments = split_keys_values(tuple.clone(), rhs_index_location.clone());
                                let tp_exists = unique_beta.contains_key(&tp);
                                let beta_exists = unique_beta.entry(tp).or_default().contains(&tuple);
                                if !(tp_exists && beta_exists) {
                                    // get alpha
                                    if !alphas.contains_key(&key_assignments) {
                                        let mut tmp = HashMap::with_capacity(2);
                                        tmp.insert(tp, vec![tuple.clone()]);
                                        alphas.insert(key_assignments.clone(), (PartialSequence::empty(mode), tmp));
                                    } else {
                                        // add subscriber
                                        let (_, map) = alphas.entry(key_assignments).or_default();
                                        map.entry(tp).or_default().push(tuple.clone());
                                    }

                                    if interval.get_raw_start() == 0 {
                                        //println!("Send at RHS: @{tp}  {:?}", tuple);
                                        output.session(&time).give(Data(true, tuple.clone()));
                                        unique_res.entry(tp).or_default().insert(tuple.clone());
                                    }

                                    // add new beta
                                    unique_beta.entry(tp).or_default().insert(tuple);
                                }
                            }
                            MetaData(flag, is) => {
                                if !flag {
                                    rhs_eos_tp = tp;
                                    rhs_eos = true;
                                }
                                if worker_index == 0 {
                                    output.session(&time).give(MetaData(flag, is))
                                }
                            }
                        }
                    };
                });

                notificator.for_each(&[time_input.frontier(), lhs_input.frontier(), rhs_input.frontier()],|time, _| {
                    let frontier_tp = time.time().clone();

                    // given the alphas and betas, eliminate betas and or output values
                    out(anti_temporal_until(frontier_tp, &mut alphas, &mut time_table, interval,lowest_tp_ts), &mut results, &mut unique_res);

                    // send results
                    if let Some(capas) = cap.as_mut() {
                        results.iter_mut().for_each(|(tp_tmp, data)| {
                            output.session(&capas.delayed(tp_tmp)).give_iterator(data.drain(..));
                        });
                    }

                    if !interval.is_infinite(){
                        let b = interval.get_raw_end();
                        // get the ts of the frontier tp
                        let current_ts = *time_table.entry(frontier_tp).or_default();

                        if !(b > current_ts) {
                            let cut_off = current_ts - b;
                            let mut to_remove = Vec::new();

                            if let Some((_low, up)) = ts_to_tp.get_mut(&cut_off) {
                                let range : Vec<usize> = (lowest_tp_ts..(*up+1)).collect();
                                to_remove = range;
                            }

                            for t in to_remove {
                                if let Some(cap) = cap.as_mut() {
                                    cap.downgrade(&(t+1));
                                    lowest_tp_ts = t+1;
                                }

                                results.remove(&t);
                                unique_res.remove(&t).unwrap_or_default();
                                unique_beta.remove(&t).unwrap_or_default();
                            }
                        }
                    }

                    if rhs_eos && lhs_eos && rhs_eos_tp == lhs_eos_tp && lhs_eos_tp == frontier_tp {
                        cap = None
                    }
                });
            }
        });

        (rhs_attrs.clone(), output_stream)
    }
}

pub(crate) fn anti_temporal_until(frontier: usize, alphas : &mut HashMap<Vec<Constant>, (PartialSequence, HashMap<usize, Vec<Vec<Constant>>>)>, tp_to_ts : &mut HashMap<usize, usize>, interval : TimeInterval, lowest_tp : usize) -> Vec<(usize, Vec<Constant>)> {
    let mut res = Vec::new();
    let mut keys = Vec::with_capacity(alphas.len());
    for key in alphas.keys() { keys.push(key.clone()); }

    for key in keys {
        let (alpha_ds, tmp) = alphas.entry(key.clone()).or_default();
        for (sub, values) in tmp.iter_mut() {
            if tp_to_ts.contains_key(sub) && *sub <= frontier {
                for tup in values.drain(..) {
                    let beta_ts = *tp_to_ts.entry(sub.clone()).or_default();
                    for tps in (lowest_tp..*sub).rev() {
                        if !contains(alpha_ds.satisfactions.clone(), tps) {
                            let frontier_ts = *tp_to_ts.entry(tps.clone()).or_default();
                            if if interval.is_infinite() { false } else { frontier_ts + interval.get_raw_end() < beta_ts } {
                                break
                            }

                            if mem_until(beta_ts, frontier_ts, interval) {
                                res.push((tps, tup.clone()));
                            }
                        } else {
                            break
                        }
                    }
                }
            }
        }

        tmp.retain(|_,v| {
            !v.is_empty()
        })
    }

    return res
}


// todo reconsider alpha frontier as most important
pub(crate) fn anti_temporal_since(frontier_tp : usize, alphas : &mut HashMap<Vec<Constant>, (PartialSequence, HashMap<usize, Vec<Vec<Constant>>>)>, tp_to_ts : &mut HashMap<usize, usize>, interval : TimeInterval) -> Vec<(usize, Vec<Constant>)> {
    let mut res = Vec::new();
    let frontier_ts = *tp_to_ts.entry(frontier_tp).or_default();

    let mut keys = Vec::with_capacity(alphas.len());
    for key in alphas.keys() {
        keys.push(key.clone());
    }

    for key in keys {
        let (alpha_ds, tmp) = alphas.entry(key).or_default();
        for (sub, values) in tmp.iter_mut() {
            if *sub < frontier_tp {
                values.retain(|tup| {
                    // need to consider that frontier tp is above bound
                    //println!("Sub {:?}  !contains {:?}  bound {}  frontier tp {}", sub, !contains(alpha_ds.satisfactions.clone(), frontier_tp), bound, frontier_tp);
                    if !contains(alpha_ds.satisfactions.clone(), frontier_tp) {
                        let beta_ts = *tp_to_ts.entry(sub.clone()).or_default();
                        //println!("beta ts {}   frontier ts {} ",beta_ts, frontier_ts);
                        let outdated = if !interval.is_infinite() { false } else { beta_ts + interval.get_raw_end() > frontier_ts };
                        if outdated {
                            false
                        } else {
                            if mem_since(beta_ts, frontier_ts, interval) {
                                res.push((frontier_tp, tup.clone()));
                            }
                            true
                        }
                    } else {
                        false
                    }
                });
            }
        }
        tmp.retain(|_,v| {!v.is_empty()});
    }

    return res
}

fn process_interval(val : &mut (usize,usize,usize,bool), lower: usize, upper: usize) -> Vec<usize> {
    return if val.3 {
        let left_interval = if val.1 > lower {
            let x = lower..val.1;
            val.1 = lower;
            x
        } else {0..0};

        let right_interval = if val.2 < upper {
            let x = val.2+1..upper+1;
            val.2 = upper;
            x
        } else {0..0};

        left_interval.chain(right_interval).collect::<Vec<usize>>()
    } else {
        val.1 = lower;
        val.2 = upper;
        val.3 = true;
        (lower..upper+1).collect::<Vec<usize>>()
    }
}
fn process_literal(val : &mut (usize,usize,usize,bool), tp: usize) -> Vec<usize> {
    return if val.3 {
        // check relevancy
        if tp < val.2 && val.1 < tp {
            return vec![]
        }

        let left_interval = if val.1 > tp {
            let x = tp..val.1;
            val.1 = tp;
            x
        } else if val.2 < tp {
            let x = val.2+1..tp+1;
            val.2 = tp;
            x
        } else {0..0};

        left_interval.collect::<Vec<usize>>()
    } else {
        val.1 = tp;
        val.2 = tp;
        val.3 = true;
        vec![tp]
    }
}

pub fn process_eventually_time_finite(betas : &mut HashMap<usize, HashMap<Vec<Constant>, (usize, usize, usize, bool)>>, obs_seq : &mut ObservationSequence, interval: TimeInterval, relevant_tps : HashSet<usize>) -> HashMap<Vec<Constant>, HashSet<usize>> {
    //println!("Process Alpha Eventually finite");
    let mut res : HashMap<Vec<Constant>, HashSet<usize>> = HashMap::with_capacity(8);
    for relevant_tp in relevant_tps {
        if let Some(beta) = betas.get_mut(&relevant_tp) {
            for (rec, val) in beta.iter_mut() {
                let beta_tp = val.0;

                match obs_seq.associated_interval_tp(beta_tp) {
                    Literal(beta_ts) => {
                        //println!("          Literal case");
                        let lower_bound_ts = if beta_ts >= interval.get_raw_end() { beta_ts - interval.get_raw_end() } else { 0 };
                        let upper_bound_ts = if beta_ts >= interval.get_raw_start() { beta_ts - interval.get_raw_start() } else { 0 };
                        let out_of_bounds_negative = interval.get_raw_start() > beta_ts;
                        let zero_ts_satisfied = match obs_seq.zero_ts() {
                            None => {false}
                            Some(low_ts) => {
                                if !out_of_bounds_negative {
                                    beta_ts - interval.get_raw_start() >= low_ts
                                } else {
                                    false
                                }
                            }
                        };
                        let lowest_ts_satisfied = match obs_seq.lowest_ts() {
                            None => {false}
                            Some(lowest_ts) => {
                                if !out_of_bounds_negative {
                                    lower_bound_ts >= lowest_ts
                                } else {
                                    false
                                }
                            }
                        };

                        let lowest_satisfied = (zero_ts_satisfied || lowest_ts_satisfied ) && !out_of_bounds_negative;

                        if lowest_satisfied {
                            match obs_seq.associated_interval_ts_exact(lower_bound_ts, upper_bound_ts) {
                                Literal(b) => {
                                    //println!("              Literal case @{} {}", beta_tp, b);
                                    if interval.get_raw_start() == 0 {
                                        let x = process_interval(val, b, beta_tp);
                                        //println!("{:?}", x);
                                        if !x.is_empty() {
                                            //println!("        @{beta_tp} {:?}  {:?}", rec.clone(), x.clone());
                                            for n in x {
                                                res.entry(rec.clone()).or_insert_with(|| HashSet::new()).insert(n);
                                            }
                                        }
                                    } else {
                                        let x = process_literal(val, b);
                                        //println!("{:?}", x);
                                        if !x.is_empty() {
                                            //println!("        @{beta_tp} {:?}  {:?}", rec.clone(), x.clone());
                                            for n in x {
                                                res.entry(rec.clone()).or_insert_with(|| HashSet::new()).insert(n);
                                            }
                                        }
                                    }
                                }
                                InfinityIntervalsReturn::Interval(a, b) => {
                                    //println!("              Interval case @{}  a{} b{}", beta_tp, a, b);
                                    let up = if interval.get_raw_start() == 0 { beta_tp } else { b };
                                    let x = process_interval(val, a, up);
                                    //println!("{:?}", x);
                                    if !x.is_empty() {
                                        //println!("        @{beta_tp} {:?}  {:?}", rec.clone(), x.clone());
                                        for n in x {
                                            res.entry(rec.clone()).or_insert_with(|| HashSet::new()).insert(n);
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    InfinityIntervalsReturn::Interval(low_ts, up_ts) => {
                        let lower = if low_ts >= interval.get_raw_end() {low_ts - interval.get_raw_end()} else {0};
                        let out_of_bounds_low = interval.get_raw_end() > low_ts;
                        let higher = if up_ts >= interval.get_raw_start() {up_ts - interval.get_raw_start()} else {0};
                        let out_of_bounds_higher = interval.get_raw_start() > up_ts;

                        // || instead &&
                        let out_of_bounds_negative = !out_of_bounds_low || !out_of_bounds_higher;

                        let zero_ts_satisfied = match obs_seq.zero_ts() {
                            None => {false}
                            Some(lowest_ts) => {
                                if !out_of_bounds_low {
                                    low_ts - interval.get_raw_end() >= lowest_ts
                                } else {
                                    false
                                }
                            }
                        };
                        let lowest_ts_satisfied = match obs_seq.lowest_ts() {
                            None => {false}
                            Some(lowest_ts) => {
                                if !out_of_bounds_low {
                                    lower >= lowest_ts
                                } else {
                                    false
                                }
                            }
                        };

                        let lowest_satisfied = (zero_ts_satisfied || lowest_ts_satisfied ) && !out_of_bounds_negative;
                        //println!("              {:?}  low {} upper {}    {} {} {}", rec, lower, higher, lowest_satisfied, zero_ts_satisfied, lowest_ts_satisfied);
                        if lowest_satisfied {
                            match obs_seq.associated_interval_ts_inexact(lower, higher) {
                                Literal(b) => {
                                    //println!("              Literal case @{} {}", beta_tp, b);
                                    if interval.get_raw_start() == 0 {
                                        let x = process_interval(val, b, beta_tp);
                                        //println!("{:?}", x);
                                        if !x.is_empty() {
                                            //println!("        @{beta_tp} {:?}  {:?}", rec.clone(), x.clone());
                                            for n in x {
                                                res.entry(rec.clone()).or_insert_with(|| HashSet::new()).insert(n);
                                            }
                                        }
                                    } else {
                                        let x = process_literal(val, b);
                                        //println!("{:?}", x);
                                        if !x.is_empty() {
                                            //println!("        @{beta_tp} {:?}  {:?}", rec.clone(), x.clone());
                                            for n in x {
                                                res.entry(rec.clone()).or_insert_with(|| HashSet::new()).insert(n);
                                            }
                                        }
                                    }
                                }
                                InfinityIntervalsReturn::Interval(a, b) => {
                                    //println!("              Interval @{}  a{} b{}\n", beta_tp, a, b);
                                    let up = if interval.get_raw_start() == 0 { beta_tp } else { b };
                                    let x = process_interval(val, a, up);
                                    if !x.is_empty() {
                                        //println!("        @{beta_tp} {:?}  {:?}", rec.clone(), x.clone());
                                        for n in x {
                                            res.entry(rec.clone()).or_insert_with(|| HashSet::new()).insert(n);
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    res
}
pub fn process_eventually_data_finite(tuple: Vec<Constant>, tp: usize, interval: TimeInterval, betas: &mut HashMap<usize,HashMap<Vec<Constant>, (usize, usize, usize, bool)>>, obs_seq : &mut ObservationSequence) ->  Vec<usize> {
    //println!("      Process Beta Eventually Finite");
    return match obs_seq.associated_interval_tp(tp) {
        Literal(beta_ts) => {
            //println!("          Literal case");
            let lower_bound_ts = if beta_ts >= interval.get_raw_end() {beta_ts - interval.get_raw_end()} else {0};
            let upper_bound_ts = if beta_ts >= interval.get_raw_start() {beta_ts - interval.get_raw_start()} else {0};
            let out_of_bounds_negative = interval.get_raw_start() > beta_ts;
            // check if lower_bound_ts exists or lowest exists
            let zero_ts_satisfied = match obs_seq.zero_ts() {
                None => {false}
                Some(low_ts) => {
                    if !out_of_bounds_negative {
                        beta_ts - interval.get_raw_start() >= low_ts
                    } else {
                        false
                    }
                }
            };
            let lowest_ts_satisfied = match obs_seq.lowest_ts() {
                None => {false}
                Some(lowest_ts) => {
                    if !out_of_bounds_negative {
                        lower_bound_ts >= lowest_ts
                    } else {
                        false
                    }
                }
            };

            let lowest_satisfied = (zero_ts_satisfied || lowest_ts_satisfied ) && !out_of_bounds_negative;
            //println!("              {:?}   low {} upper {}    {}", tuple, lower_bound_ts, upper_bound_ts, lowest_satisfied);
            if lowest_satisfied {
                match obs_seq.associated_interval_ts_exact(lower_bound_ts, upper_bound_ts) {
                    Literal(b) => {
                        //println!("              Literal @{}  b{}\n", tp, b);
                        let irrelevant = tp == 0 && interval.get_raw_start() != 0;
                        if !irrelevant {
                            let up = if interval.get_raw_start() == 0 {tp} else {b};
                            let vals : Vec<usize> = (b..up+1).collect();
                            betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, b, up, true));
                            vals
                        } else {
                            vec![]
                        }
                    }
                    InfinityIntervalsReturn::Interval(a, b) => {
                        //println!("              Interval @{}  a{} b{}\n", tp, a, b);
                        let up = if interval.get_raw_start() == 0 {tp} else {b};
                        let vals : Vec<usize> = (a..up+1).collect();
                        betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, a, up, true));
                        vals
                    }
                    InfinityIntervalsReturn::Empty => {
                        //println!("              Literal Empty @{}\n", tp);
                        if interval.get_raw_start() == 0 {
                            betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, tp, tp, true));
                            vec![tp]
                        } else {
                            betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, 0, 0, false));
                            vec![]
                        }
                    }
                }
            } else {
                if interval.get_raw_start() == 0 {
                    betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, tp, tp, true));
                    vec![tp]
                } else {
                    betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, 0, 0, false));
                    vec![]
                }
            }
        }
        // compute two intervals and get the ts of the overlapping interval and query for tps given the timepoints
        InfinityIntervalsReturn::Interval(low_ts, up_ts) => {
            //println!("          Interval case {} {}", low_ts, up_ts);
            let lower = if low_ts >= interval.get_raw_end() {low_ts - interval.get_raw_end()} else {0};
            let out_of_bounds_low = interval.get_raw_end() > low_ts;

            let higher = if up_ts >= interval.get_raw_start() {up_ts - interval.get_raw_start()} else {0};
            let out_of_bounds_higher = interval.get_raw_start() > up_ts;

            let out_of_bounds_negative = !out_of_bounds_low || !out_of_bounds_higher;

            let zero_ts_satisfied = match obs_seq.zero_ts() {
                None => {false}
                Some(lowest_ts) => {
                    if !out_of_bounds_low {
                        low_ts - interval.get_raw_end() >= lowest_ts
                    } else {
                        false
                    }
                }
            };
            let lowest_ts_satisfied = match obs_seq.lowest_ts() {
                None => {false}
                Some(lowest_ts) => {
                    if !out_of_bounds_low {
                        lower >= lowest_ts
                    } else {
                        false
                    }
                }
            };

            let lowest_satisfied = (zero_ts_satisfied || lowest_ts_satisfied ) && !out_of_bounds_negative;
            //println!("          low_ts {} - a{} == {}", low_ts, interval.get_raw_start(), (low_ts as i64 - interval.get_raw_start() as i64));
            //println!("              {:?}  low {} upper {}    {}", tuple, lower, higher, lowest_satisfied);
            if lowest_satisfied {
                match obs_seq.associated_interval_ts_inexact(lower, higher) {
                    Literal(b) => {
                        //println!("              Literal @{}  {} a{} b{}\n", tp, b, lower, higher);
                        let up = if interval.get_raw_start() == 0 {tp} else {b};
                        let vals : Vec<usize> = (b..up+1).collect();
                        betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, b, up, true));
                        vals
                    }
                    InfinityIntervalsReturn::Interval(a, b) => {
                        //println!("              Interval @{}  a{} b{}\n", tp, a, b);
                        let up = if interval.get_raw_start() == 0 {tp} else {b};
                        let vals : Vec<usize> = (a..up+1).collect();
                        betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, a, up, true));
                        //println!("{:?}", vals);
                        vals
                    }
                    InfinityIntervalsReturn::Empty => {
                        //println!("              Interval Empty @{}\n", tp);
                        if interval.get_raw_start() == 0 {
                            betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, tp, tp, true));
                            vec![tp]
                        } else {
                            betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, 0, 0, false));
                            vec![]
                        }
                    }
                }
            } else {
                if interval.get_raw_start() == 0 {
                    betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, tp, tp, true));
                    vec![tp]
                } else {
                    betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, 0, 0, false));
                    vec![]
                }
            }
        }
        _ => {
            //println!("              Match Empty @{}", tp);
            if interval.get_raw_start() == 0 {
                betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, tp, tp, true));
                vec![tp]
            } else {
                betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, 0, 0, false));
                vec![]
            }
        }
    }
}

pub fn process_eventually_time_infinite(betas : &mut HashMap<usize, HashMap<Vec<Constant>, (usize, usize, usize, bool)>>, obs_seq : &mut ObservationSequence, interval: TimeInterval, relevant_tps : HashSet<usize>) -> HashMap<Vec<Constant>, HashSet<usize>> {
    //println!("Process Alpha Eventually infinite");
    let mut res : HashMap<Vec<Constant>, HashSet<usize>> = HashMap::with_capacity(8);
    for relevant_tp in relevant_tps {
        if let Some(beta) = betas.get_mut(&relevant_tp) {
            for (rec, val) in beta.iter_mut() {
                let beta_tp = val.0;
                match obs_seq.associated_interval_tp(beta_tp) {
                    Literal(beta_ts) => {
                        //println!("          Literal case @{}", beta_tp);
                        let upper_bound_ts = if beta_ts >= interval.get_raw_start() { beta_ts - interval.get_raw_start() } else { 0 };
                        let out_of_bounds = interval.get_raw_start() > beta_ts;
                        //println!("              low {}      {} {} = {}", upper_bound_ts, beta_ts, interval.get_raw_start(), out_of_bounds);

                        if !out_of_bounds {
                            match obs_seq.associated_upper_bound_ts(upper_bound_ts) {
                                Literal(b) => {
                                    //println!("              Literal case @{} {}", beta_tp, b);
                                    let up = if interval.get_raw_start() == 0 { beta_ts } else { b };
                                    let x = process_interval(val, 0, up);
                                    //println!("@{} {:?}",beta_tp, x);
                                    if !x.is_empty() {
                                        for n in x {
                                            res.entry(rec.clone()).or_insert_with(|| HashSet::new()).insert(n);
                                        }
                                    }
                                }
                                InfinityIntervalsReturn::Interval(_a, b) => {
                                    //println!("              Interval case @{} a{} b{}", beta_tp, a, b);
                                    let up = if interval.get_raw_start() == 0 { beta_tp } else { b };
                                    let x = process_interval(val, 0, up);
                                    //println!("@{} {:?}",beta_tp, x);
                                    if !x.is_empty() {
                                        for n in x {
                                            res.entry(rec.clone()).or_insert_with(|| HashSet::new()).insert(n);
                                        }
                                    }
                                }
                                _ => {
                                    if interval.get_raw_start() == 0 {
                                        let x = process_interval(val, 0, beta_tp);
                                        if !x.is_empty() {
                                            //println!("        @{beta_tp} {:?}  {:?}", rec.clone(), x.clone());
                                            for n in x {
                                                res.entry(rec.clone()).or_insert_with(|| HashSet::new()).insert(n);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    InfinityIntervalsReturn::Interval(low_ts, _up_ts) => {
                        //println!("          Interval case @{}  {} {}", beta_tp, low_ts, up_ts);
                        let higher = if low_ts >= interval.get_raw_start() { low_ts - interval.get_raw_start() } else { 0 };
                        let out_of_bounds = interval.get_raw_start() > low_ts;
                        //println!("           {} {} = {}", low_ts, interval.get_raw_start(), out_of_bounds);

                        if !out_of_bounds {
                            match obs_seq.associated_upper_bound_ts(higher) {
                                Literal(b) => {
                                    //println!("              Literal case @{} {}", beta_tp, b);
                                    let up = if interval.get_raw_start() == 0 {beta_tp} else {b};
                                    let x = process_interval(val, 0, up);
                                    //println!("@{} {:?}",beta_tp, x);
                                    if !x.is_empty() {
                                        for n in x {
                                            res.entry(rec.clone()).or_insert_with(|| HashSet::new()).insert(n);
                                        }
                                    }
                                }
                                InfinityIntervalsReturn::Interval(_a, b) => {
                                    //println!("              Interval @{}  a{} b{}\n", beta_tp, a, b);
                                    let up = if interval.get_raw_start() == 0 { beta_tp } else { b };
                                    let x = process_interval(val, 0, up);
                                    //println!("@{} {:?}",beta_tp, x);
                                    if !x.is_empty() {
                                        for n in x {
                                            res.entry(rec.clone()).or_insert_with(|| HashSet::new()).insert(n);
                                        }
                                    }
                                }
                                _ => {
                                    if interval.get_raw_start() == 0 {
                                        let x = process_interval(val, 0, beta_tp);
                                        if !x.is_empty() {
                                            //println!("        @{beta_tp} {:?}  {:?}", rec.clone(), x.clone());
                                            for n in x {
                                                res.entry(rec.clone()).or_insert_with(|| HashSet::new()).insert(n);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    res
}
pub fn process_eventually_data_infinite(tuple: Vec<Constant>, tp: usize, interval: TimeInterval, betas: &mut HashMap<usize, HashMap<Vec<Constant>, (usize, usize, usize, bool)>>, obs_seq : &mut ObservationSequence) ->  Vec<usize> {
    //println!("      Process Beta Eventually infinite");
    return match obs_seq.associated_interval_tp(tp) {
        Literal(beta_ts) => {
            //println!("          Literal case");
            let upper_bound_ts = if beta_ts >= interval.get_raw_start() {beta_ts - interval.get_raw_start()} else {0};
            let out_of_bounds = interval.get_raw_start() > beta_ts;
            //println!("          @{} [{}]", tp, upper_bound_ts);
            if !out_of_bounds {
                match obs_seq.associated_interval_ts_exact(upper_bound_ts, upper_bound_ts) {
                    Literal(b) => {
                        //println!("              Literal @{}  b{}\n", tp, b);
                        let irrelevant = tp == 0 && interval.get_raw_start() != 0;
                        if !irrelevant {
                            let up = if interval.get_raw_start() == 0 {tp} else {b};
                            let vals : Vec<usize> = (0..up+1).collect();
                            betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, 0, up, true));
                            vals
                        } else {
                            vec![]
                        }
                    }
                    InfinityIntervalsReturn::Interval(_a, b) => {
                        //println!("              Interval @{}  a{} b{}\n", tp, a, b);
                        let up = if interval.get_raw_start() == 0 {tp} else {b};
                        let vals : Vec<usize> = (0..up+1).collect();
                        betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, 0, up, true));
                        vals
                    }
                    InfinityIntervalsReturn::Empty => {
                        //println!("              Interval Empty @{}\n", tp);
                        if interval.get_raw_start() == 0 {
                            betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, 0, tp, true));
                            vec![tp]
                        } else {
                            betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, 0, 0, false));
                            vec![]
                        }
                    }
                }
            } else {
                betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, 0, 0, false));
                vec![]
            }
        }
        // compute two intervals and get the ts of the overlapping interval and query for tps given the timepoints
        InfinityIntervalsReturn::Interval(low_ts, _up_ts) => {
            //println!("          Interval case {} {}", low_ts, up_ts);
            //let lower = if up_ts >= interval.get_raw_end() {up_ts - interval.get_raw_end()} else {0};
            //println!("          up_ts {} - b{} == {}", up_ts, interval.get_raw_end(), (up_ts as i64 - interval.get_raw_end() as i64));
            let higher = if low_ts >= interval.get_raw_start() {low_ts - interval.get_raw_start()} else {0};
            let out_of_bounds = interval.get_raw_start() > low_ts;
            //println!("          low_ts {} - a{} == {}", low_ts, interval.get_raw_start(), (low_ts as i64 - interval.get_raw_start() as i64));

            if !out_of_bounds {
                match obs_seq.associated_lower_bound_ts(higher) {
                    Literal(b) => {
                        //println!("              Literal @{}  {} a{}\n", tp, b, higher);
                        let up = if interval.get_raw_start() == 0 {tp} else {b};
                        let vals : Vec<usize> = (0..up+1).collect();
                        betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, 0, up, true));
                        vals
                    }
                    InfinityIntervalsReturn::Interval(_a, b) => {
                        //println!("              Interval @{}  a{} b{}\n", tp, a, b);
                        let up = if interval.get_raw_start() == 0 {tp} else {b};
                        let vals : Vec<usize> = (0..up+1).collect();
                        betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, 0, up, true));
                        vals
                    }
                    InfinityIntervalsReturn::Empty => {
                        //println!("              Interval Empty @{}\n", tp);
                        if interval.get_raw_start() == 0 {
                            let vals : Vec<usize> = (0..tp+1).collect();
                            betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, 0, tp, true));
                            vals
                        } else {
                            betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, 0, 0, false));
                            vec![]
                        }
                    }
                }
            } else {
                betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, 0, 0, false));
                vec![]
            }
        }
        _ => {
            //println!("              Match Empty @{}", tp);
            if interval.get_raw_start() == 0 {
                let vals : Vec<usize> = (0..tp+1).collect();
                betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, 0, tp, true));
                vals
            } else {
                betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, 0, 0, false));
                vec![]
            }
        }
    }
}

// optimise beta
pub fn process_once_time_finite(betas : &mut HashMap<usize, HashMap<Vec<Constant>, (usize, usize, usize, bool)>>, obs_seq : &mut ObservationSequence, interval: TimeInterval, relevant_tps : HashSet<usize>) -> HashMap<Vec<Constant>, HashSet<usize>> {
    //println!("Process Alpha finite");
    let mut res : HashMap<Vec<Constant>, HashSet<usize>> = HashMap::with_capacity(8);
    for relevant_tp in relevant_tps {
        if let Some(beta) = betas.get_mut(&relevant_tp) {
            for (rec, val) in beta.iter_mut() {
                let beta_tp = val.0;
                //println!("{beta_tp}  relevant {relevant_tp}");
                match obs_seq.associated_interval_tp(beta_tp) {
                    Literal(beta_ts) => {
                        //println!("          Literal case {:?}", rec);
                        let lower_bound_ts = beta_ts + interval.get_raw_start();
                        let upper_bound_ts = beta_ts + interval.get_raw_end();

                        //println!("              low {} upper {}", lower_bound_ts, upper_bound_ts);
                        match obs_seq.associated_interval_ts_exact(lower_bound_ts, upper_bound_ts) {
                            Literal(b) => {
                                //println!("         Literal");
                                if interval.get_raw_start() == 0 {
                                    //println!("            Literal case {} {}", beta_tp, b);
                                    let x = process_interval(val, beta_tp, b);
                                    if !x.is_empty() {
                                        //println!("        @{beta_tp} {:?}  {:?}", rec.clone(), x.clone());
                                        for n in x {
                                            res.entry(rec.clone()).or_insert_with(|| HashSet::new()).insert(n);
                                        }
                                    }
                                } else {
                                    let x = process_literal(val, b);
                                    if !x.is_empty() {
                                        //println!("        @{beta_tp} {:?}  {:?}", rec.clone(), x.clone());
                                        for n in x {
                                            res.entry(rec.clone()).or_insert_with(|| HashSet::new()).insert(n);
                                        }
                                    }
                                }
                            }
                            InfinityIntervalsReturn::Interval(a, b) => {
                                let low = if interval.get_raw_start() == 0 { beta_tp } else { a };
                                let x = process_interval(val, low, b);
                                if !x.is_empty() {
                                    //println!("        @{beta_tp} {:?}  {:?}", rec.clone(), x.clone());
                                    for n in x {
                                        res.entry(rec.clone()).or_insert_with(|| HashSet::new()).insert(n);
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    InfinityIntervalsReturn::Interval(low_ts, up_ts) => {
                        //println!("          Interval case {:?}", rec);
                        let snd_lower_bound_ts = up_ts + interval.get_raw_start();
                        let fst_upper_bound_ts = low_ts + interval.get_raw_end();

                        match obs_seq.associated_interval_ts_inexact(snd_lower_bound_ts, fst_upper_bound_ts) {
                            Literal(b) => {
                                //println!("              Literal case {} {}", beta_tp, b);
                                if interval.get_raw_start() == 0 {
                                    let x = process_interval(val, beta_tp, b);
                                    if !x.is_empty() {
                                        //println!("        @{beta_tp} {:?}  {:?}", rec.clone(), x.clone());
                                        for n in x {
                                            res.entry(rec.clone()).or_insert_with(|| HashSet::new()).insert(n);
                                        }
                                    }
                                } else {
                                    /*println!("              process Literal");*/
                                    let x = process_literal(val, b);
                                    if !x.is_empty() {
                                        //println!("        @{beta_tp} {:?}  {:?}", rec.clone(), x.clone());
                                        for n in x {
                                            res.entry(rec.clone()).or_insert_with(|| HashSet::new()).insert(n);
                                        }
                                    }
                                }
                            }
                            InfinityIntervalsReturn::Interval(a, b) => {
                                //println!("              Interval @{}  {} {}\n", beta_tp, a, b);
                                let low = if interval.get_raw_start() == 0 { beta_tp } else { a };
                                let x = process_interval(val, low, b);
                                if !x.is_empty() {
                                    //println!("        @{beta_tp} {:?}  {:?}", rec.clone(), x.clone());
                                    for n in x {
                                        res.entry(rec.clone()).or_insert_with(|| HashSet::new()).insert(n);
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    res
}
pub fn process_once_data_finite(tuple: Vec<Constant>, tp: usize, interval: TimeInterval, betas: &mut HashMap<usize, HashMap<Vec<Constant>, (usize, usize, usize, bool)>>, obs_seq : &mut ObservationSequence) ->  Vec<usize> {
    //println!("      Process Beta Finite");
    return match obs_seq.associated_interval_tp(tp) {
        Literal(beta_ts) => {
            //println!("          Literal case {:?}", tuple);
            let lower_bound_ts = beta_ts + interval.get_raw_start();
            let upper_bound_ts = beta_ts + interval.get_raw_end();

            match obs_seq.associated_interval_ts_exact(lower_bound_ts, upper_bound_ts) {
                Literal(b) => {
                    //println!("              Literal @{}  b{}\n", tp, b);
                    if interval.get_raw_start() == 0 {
                        let vals : Vec<usize> = (tp..b+1).collect();
                        betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, tp, b, true));
                        vals
                    } else {
                        betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, b, b, true));
                        vec![b]
                    }
                }
                InfinityIntervalsReturn::Interval(a, b) => {
                    //println!("              Interval @{}  a{} b{}\n", tp, a, b);
                    if interval.get_raw_start() == 0 {
                        let vals : Vec<usize> = (tp..b+1).collect();
                        betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, tp, b, true));
                        vals
                    } else {
                        let vals : Vec<usize> = (a..b+1).collect();
                        betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, a, b, true));
                        vals
                    }
                }
                _ => {
                    //println!("              Interval Empty @{}\n", tp);
                    if interval.get_raw_start() == 0 {
                        betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, tp, tp, true));
                        vec![tp]
                    } else {
                        betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, 0, 0, false));
                        vec![]
                    }
                }
            }
        }
        // compute two intervals and get the ts of the overlapping interval and query for tps given the timepoints
        InfinityIntervalsReturn::Interval(low_ts, up_ts) => {
            //println!("          Interval case {} {} {:?}", low_ts, up_ts, tuple);
            let snd_lower_bound_ts = up_ts + interval.get_raw_start();
            let fst_upper_bound_ts = low_ts + interval.get_raw_end();

            // the interval upper bound is not certain hence it cannot include the literal to the right


            //println!("{:?}", obs_seq.observations);
            match obs_seq.associated_interval_ts_inexact(snd_lower_bound_ts, fst_upper_bound_ts) {
                Literal(b) => {
                    //println!("              Literal @{}  {} a{} b{}\n", tp, b, snd_lower_bound_ts, fst_upper_bound_ts);
                    if interval.get_raw_start() == 0 {
                        let vals : Vec<usize> = (tp..b+1).collect();
                        betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, tp, b, true));
                        vals
                    } else {
                        betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, b, b, true));
                        vec![b]
                    }
                }
                InfinityIntervalsReturn::Interval(a, b) => {
                    //println!("              Interval @{}  a{} b{}\n", tp, a, b);
                    if interval.get_raw_start() == 0 {
                        let vals : Vec<usize> = (tp..b+1).collect();
                        betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, tp, b, true));
                        vals
                    } else {
                        let vals : Vec<usize> = (a..b+1).collect();
                        betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, a, b, true));
                        vals
                    }
                }
                _ => {
                   // println!("              Interval Empty @{}\n", tp);
                    if interval.get_raw_start() == 0 {
                        betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, tp, tp, true));
                        vec![tp]
                    } else {
                        betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, 0, 0, false));
                        vec![]
                    }
                }
            }
        }
        _ => {
            //println!("              Match Empty @{}", tp);
            if interval.get_raw_start() == 0 {
                betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, tp, tp, true));
                vec![tp]
            } else {
                betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, 0, 0, false));
                vec![]
            }
        }
    }
}

pub fn process_once_time_infinite(betas : &mut HashMap<usize, HashMap<Vec<Constant>, (usize, usize, usize, bool)>>, obs_seq : &mut ObservationSequence, interval: TimeInterval, highest_tp: usize, relevant_tps : HashSet<usize>) -> HashMap<Vec<Constant>, HashSet<usize>> {
    let mut res : HashMap<Vec<Constant>, HashSet<usize>> = HashMap::with_capacity(8);
    for relevant_tp in relevant_tps {
        if let Some(beta) = betas.get_mut(&relevant_tp) {
            for (rec, val) in beta.iter_mut() {
                let beta_tp = val.0;
                match obs_seq.associated_interval_tp(beta_tp) {
                    Literal(beta_ts) => {
                        //println!("Literal case");
                        let lower_bound_ts = beta_ts + interval.get_raw_start();
                        match obs_seq.associated_lower_bound_ts(lower_bound_ts) {
                            Literal(a) => {
                                if interval.get_raw_start() == 0 {
                                    //println!("  0 Literal low {beta_tp} highest {}", highest_tp);
                                    //println!("Val before {:?}", val);
                                    let x = process_interval(val, beta_tp, highest_tp);
                                    // println!("        @{beta_tp} {:?}  {:?}", rec.clone(), x.clone());
                                    if !x.is_empty() {
                                        for n in x {
                                            res.entry(rec.clone()).or_insert_with(|| HashSet::new()).insert(n);
                                        }
                                    }
                                    //println!("Val after {:?}", val);
                                } else {
                                    //println!("  Literal low {a} highest {}", highest_tp);
                                    let x = process_interval(val, a, highest_tp);
                                    if !x.is_empty() {
                                        //println!("        @{beta_tp} {:?}  {:?}", rec.clone(), x.clone());
                                        for n in x {
                                            res.entry(rec.clone()).or_insert_with(|| HashSet::new()).insert(n);
                                        }
                                    }
                                }
                            }
                            InfinityIntervalsReturn::Interval(a, b) => {
                                //println!("  Interval");
                                let low = if interval.get_raw_start() == 0 { beta_tp } else { a };
                                let high = if highest_tp > b { highest_tp } else { b };
                                let x = process_interval(val, low, high);
                                if !x.is_empty() {
                                    //println!("        @{beta_tp} {:?}  {:?}", rec.clone(), x.clone());
                                    for n in x {
                                        res.entry(rec.clone()).or_insert_with(|| HashSet::new()).insert(n);
                                    }
                                }
                            }
                            _ => {
                                if highest_tp >= beta_tp {
                                    if interval.get_raw_start() == 0 {
                                        let x = process_interval(val, beta_tp, highest_tp);
                                        if !x.is_empty() {
                                            //println!("        @{beta_tp} {:?}  {:?}", rec.clone(), x.clone());
                                            for n in x {
                                                res.entry(rec.clone()).or_insert_with(|| HashSet::new()).insert(n);
                                            }
                                        }
                                    }
                                }
                                //println!("   Empty {}", beta_tp);
                            }
                        }
                    }
                    // compute two intervals and get the ts of the overlapping interval and query for tps given the timepoints
                    InfinityIntervalsReturn::Interval(_low_ts, up_ts) => {
                        //println!("Interval case");
                        let snd_lower_bound_ts = up_ts + interval.get_raw_start();
                        // compute potential intersection
                        match obs_seq.associated_lower_bound_ts(snd_lower_bound_ts) {
                            Literal(a) => {
                                //println!("  Literal");
                                let low = if interval.get_raw_start() == 0 { beta_tp } else { a };
                                let x = process_interval(val, low, highest_tp);
                                if !x.is_empty() {
                                    //println!("        @{beta_tp} {:?}  {:?}", rec.clone(), x.clone());
                                    for n in x {
                                        res.entry(rec.clone()).or_insert_with(|| HashSet::new()).insert(n);
                                    }
                                }
                            }
                            InfinityIntervalsReturn::Interval(a, b) => {
                                //println!("  Interval");
                                let low = if interval.get_raw_start() == 0 { beta_tp } else { a };
                                let high = if highest_tp > b { highest_tp } else { b };
                                let x = process_interval(val, low, high);
                                if !x.is_empty() {
                                    //println!("        @{beta_tp} {:?}  {:?}", rec.clone(), x.clone());
                                    for n in x {
                                        res.entry(rec.clone()).or_insert_with(|| HashSet::new()).insert(n);
                                    }
                                }
                            }
                            _ => {
                                if highest_tp >= beta_tp {
                                    if interval.get_raw_start() == 0 {
                                        let x = process_interval(val, beta_tp, highest_tp);
                                        if !x.is_empty() {
                                            //println!("        @{beta_tp} {:?}  {:?}", rec.clone(), x.clone());
                                            for n in x {
                                                res.entry(rec.clone()).or_insert_with(|| HashSet::new()).insert(n);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    //println!("");
    return res
}
pub fn process_once_data_infinite(tuple: Vec<Constant>, tp: usize, highest_tp_ts: usize, interval: TimeInterval, betas: &mut HashMap<usize, HashMap<Vec<Constant>, (usize, usize, usize, bool)>>, obs_seq : &mut ObservationSequence) ->  Vec<usize> {
    //println!("Beta infinite in @{}  {:?}", tp, tuple);
    return match obs_seq.associated_interval_tp(tp) {
        Literal(beta_ts) => {
            //println!("Literal Case");
            let lower_bound_ts = beta_ts + interval.get_raw_start();
            //println!("Lower bound {}", lower_bound_ts);
            match obs_seq.associated_lower_bound_ts(lower_bound_ts) {
                Literal(a) => {
                    let low = if interval.get_raw_start() == 0 {tp} else {a};
                    let vals : Vec<usize> = (low..highest_tp_ts+1).collect();
                    //println!("  Literal tp@{}  a{}", low, highest_tp_ts);
                    //println!("  {:?}", (tp, low, highest_tp_ts, true));
                    betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, low, highest_tp_ts, true));
                    vals
                }
                InfinityIntervalsReturn::Interval(a, b) => {
                    let low = if interval.get_raw_start() == 0 {tp} else {a};
                    let up = if highest_tp_ts > b {highest_tp_ts} else {b};
                    let vals: Vec<usize> = (low..up+1).collect();
                    //println!("  Interval tp@{}  low {} up {}", tp, low, up);
                    betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, low, up, true));
                    vals
                }
                InfinityIntervalsReturn::Empty => {
                    //println!("  Empty");
                    if interval.get_raw_start() == 0 {
                        let vals : Vec<usize> = (tp..highest_tp_ts+1).collect();
                        betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, tp, highest_tp_ts, true));
                        //println!("  Empty tp@{}  low {} up {}", tp, tp, highest_tp_ts);
                        vals
                    } else {
                        betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, 0, 0, false));
                        vec![]
                    }
                }
            }
        }
        // compute two intervals and get the ts of the overlapping interval and query for tps given the timepoints
        InfinityIntervalsReturn::Interval(_low_ts, up_ts) => {
            //println!("Interval Case");
            let snd_lower_bound_ts = up_ts + interval.get_raw_start();
            // compute potential intersection
            match obs_seq.associated_lower_bound_ts(snd_lower_bound_ts) {
                Literal(a) => {
                    //println!("  Literal tp@{}  a{}", tp, a);
                    let low = if interval.get_raw_start() == 0 {tp} else {a};
                    let vals : Vec<usize> = (low..highest_tp_ts+1).collect();
                    //println!("  Literal tp@{}  low {} up {}", tp, low, highest_tp_ts);
                    betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, low, highest_tp_ts, true));
                    vals
                }
                InfinityIntervalsReturn::Interval(a, b) => {
                    //println!("  Interval tp@{}  a{} b{}", tp, a, b);
                    let low = if interval.get_raw_start() == 0 {tp} else {a};
                    let up= if highest_tp_ts > b {highest_tp_ts} else {b};
                    let vals: Vec<usize> = (low..up+1).collect();
                    //println!("  Interval tp@{}  low {} up {}", tp, low, up);
                    betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, low, up, true));
                    vals
                }
                _ => {
                    //println!("  Empty");
                    betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, 0, 0, false));
                    vec![]
                }
            }
        }
        _ => {
            //println!("Infinity Case");
            if interval.get_raw_start() == 0 {
                betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, tp, tp, true));
                vec![tp]
            } else {
                betas.entry(tp).or_insert_with(|| HashMap::new()).insert(tuple, (tp, 0, 0, false));
                vec![]
            }
        }
    };
}


pub(crate) fn compute_satisfactions(alpha_ds: &mut PartialSequence, time_table: &HashMap<usize, usize>, time_interval: TimeInterval, beta_or_alpha : bool, record: Vec<Constant>, ) -> Vec<(usize, Vec<Constant>)> {
    let out = if beta_or_alpha {
        alpha_ds.single_output(record)
    } else {
        alpha_ds.output()
    };

    let fun = if alpha_ds.since_or_until {mem_since} else {mem_until};
    let mut new_res = Vec::with_capacity(out.len());

    out.iter().for_each(|(valuation, vals, bound)| {
        if vals.len() > 0 {
            let beta_ts = *time_table.get(&bound).unwrap_or(&0);
            for i in vals[0]..vals[1] + 1 {
                let alpha_ts = *time_table.get(&i).unwrap_or(&0);
                if fun(beta_ts, alpha_ts, time_interval) {
                    new_res.push((i, valuation.clone()));
                }
            }
        }
    });
    return new_res;
}

fn stream_deduplicated_output(vals : Vec<usize>, tuple: Vec<Constant>, results : &mut HashMap<usize, Vec<Record>>, unique_res : &mut HashMap<usize, HashSet<Vec<Constant>>>) {
    for tp_tmp in vals {
        let has_tp = unique_res.contains_key(&tp_tmp);
        let is_in_set = if has_tp { unique_res.entry(tp_tmp).or_default().contains(&tuple) } else { false };
        if !is_in_set {
            results.entry(tp_tmp).or_insert(Vec::new()).push(Data(true, tuple.clone()));
            unique_res.entry(tp_tmp).or_default().insert(tuple.clone());
        }
    }
}

fn stream_not_deduplicated_output(vals : Vec<usize>, tuple: Vec<Constant>, results : &mut HashMap<usize, Vec<Record>>, _unique_res : &mut HashMap<usize, HashSet<Vec<Constant>>>) {
    for tp_tmp in vals {
        results.entry(tp_tmp).or_insert(Vec::new()).push(Data(true, tuple.clone()));
    }
}



fn deduplicated_output(vals : Vec<(usize, Vec<Constant>)>, results : &mut HashMap<usize, Vec<Record>>, unique_res : &mut HashMap<usize, HashSet<Vec<Constant>>>) {
    for (tp_tmp, sat) in vals {
        let has_tp = unique_res.contains_key(&tp_tmp);
        let is_in_set = if has_tp { unique_res.entry(tp_tmp).or_default().contains(&sat) } else { false };
        if !is_in_set {
            results.entry(tp_tmp)
                .or_insert(Vec::new())
                .push(Data(true, sat.clone()));
            unique_res.entry(tp_tmp).or_default().insert(sat.clone());
        }
    }
}

fn not_deduplicated_output(vals : Vec<(usize, Vec<Constant>)>, results : &mut HashMap<usize, Vec<Record>>, _unique_res : &mut HashMap<usize, HashSet<Vec<Constant>>>) {
    for (tp_tmp, sat) in vals {
        results.entry(tp_tmp)
            .or_insert(Vec::new())
            .push(Data(true, sat.clone()));
    }
}



fn prev_successor(tp : usize, current : usize, stash: &mut HashMap<usize, Vec<Vec<Constant>>>, tp_to_ts: &mut HashMap<usize, usize>, time_interval: TimeInterval) -> Vec<Record> {
    let mut result : Vec<Record> = Vec::new();
    if tp > 0 {
        if !tp_to_ts.contains_key(&tp) {
            if let Some(stashed_tuples) = stash.get_mut(&(tp-1)) {
                let previous = *tp_to_ts.get(&(tp-1)).unwrap_or(&0);
                for tup in stashed_tuples.drain(..) {
                    if mem_since(previous, current, time_interval) {
                        result.push(Data(true, tup))
                    }
                }
            }
        }
    }
    result
}

fn prev_empty_stash(stash: &mut HashMap<usize, Vec<Vec<Constant>>>, tp_to_ts: &mut HashMap<usize, usize>, time_interval: TimeInterval) -> Vec<(usize, Record)> {
    let mut result = Vec::new();
    stash.iter_mut().for_each(|(k_tp, v_tuple)| {
        let exists = tp_to_ts.contains_key(k_tp);
        let next_exists = tp_to_ts.contains_key(&(k_tp+1));

        v_tuple.retain(|tup| {
            if exists && next_exists {
                let current= *tp_to_ts.get(k_tp).unwrap_or(&0);
                let next = *tp_to_ts.get(&(k_tp+1)).unwrap_or(&0);
                if mem_since(current, next, time_interval) {
                    result.push((*k_tp+1, Data(true, tup.clone())));
                }
                false
            } else {
                true
            }
        });
    });

    result
}

fn prev_predecessor(tuple : Vec<Constant>, tp : usize, current : usize, stash: &mut HashMap<usize, Vec<Vec<Constant>>>, tp_to_ts: &mut HashMap<usize, usize>, time_interval: TimeInterval) -> Vec<Record> {
    let mut result : Vec<Record> = Vec::new();
    if tp_to_ts.contains_key(&(tp+1)) {
        let next = *tp_to_ts.get(&(tp+1)).unwrap_or(&0);
        if mem_since(current, next, time_interval) {
            result.push(Data(true, tuple))
        }
    } else {
        stash.entry(tp).or_insert_with(|| Vec::new()).push(tuple);
    }
    result
}

fn next_predecessor(tp : usize, current : usize, stash: &mut HashMap<usize, Vec<Vec<Constant>>>, tp_to_ts: &mut HashMap<usize, usize>, time_interval: TimeInterval) -> Vec<Record> {
    let mut result : Vec<Record> = Vec::new();
    if let Some(next) = tp_to_ts.get(&(tp+1)) {
        if let Some(stashed_tuples) = stash.get_mut(&(tp+1)) {
            for tup in stashed_tuples.drain(..) {
                if mem_until(*next, current, time_interval) {
                    result.push(Data(true, tup));
                }
            }
        }
    }
    result
}

fn next_empty_stash(stash: &mut HashMap<usize, Vec<Vec<Constant>>>, tp_to_ts: &mut HashMap<usize, usize>, time_interval: TimeInterval) -> Vec<(usize, Record)> {
    let mut result = Vec::new();
    stash.iter_mut().for_each(|(k_tp, v_tuple)| {
        let positive = *k_tp > 0;
        let exists = tp_to_ts.contains_key(k_tp);
        let prev_exists = if positive {tp_to_ts.contains_key(&(k_tp-1))} else { false };

        v_tuple.retain(|tup| {
            if exists && prev_exists {
                let current= *tp_to_ts.get(k_tp).unwrap_or(&0);
                let previous = *tp_to_ts.get(&(k_tp-1)).unwrap_or(&0);
                if mem_until(current, previous, time_interval) {
                    result.push((k_tp-1, Data(true, tup.clone())));
                }
                false
            }else {
                true
            }
        });
    });

    result
}

fn next_successor(tuple: Vec<Constant>, tp : usize, current : usize, stash: &mut HashMap<usize, Vec<Vec<Constant>>>, tp_to_ts: &mut HashMap<usize, usize>, time_interval: TimeInterval) -> Vec<Record> {
    let mut result : Vec<Record> = Vec::new();
    if tp > 0 {
        if tp_to_ts.contains_key(&(tp-1)) {
            let previous = *tp_to_ts.get(&(tp-1)).unwrap_or(&0);
            if mem_until(current, previous, time_interval) {
                result.push(Data(true, tuple));
            }
        } else {
            stash.entry(tp).or_insert_with(|| Vec::new()).push(tuple);
        }
    }
    result
}

fn variable_eq_variable(tuple: &Vec<Constant>, val: &Vec<usize>, indices: &Vec<usize>) -> bool {
    // There is only one value in val
    if let Some(rhs) = tuple.get(val[0]) {
        for i in indices {
            if let Some(lhs) = tuple.get(*i) {
                if rhs != lhs {
                    return false
                }
            }
        }
        return true
    }
    return false
}

fn variable_eq_value(tuple: &Vec<Constant>, var: Constant, indices: &Vec<usize>) -> bool {
    for i in indices {
        if let Some(v) = tuple.get(*i) {
            if var != *v {
                return false
            }
        }
    }

    return true
}

pub(crate) fn var_equals_var(tuple: &Vec<Constant>, indices: &Vec<usize>) -> bool {
    let mut vars = BTreeSet::new();
    indices.iter().for_each(|i| {
        if let Some(value) = tuple.get(*i) {
            vars.insert(value);
        }
    });
    vars.len() <= 1
}

pub(crate) fn var_equals_val(tuple: &Vec<Constant>, val: &Arg, indices: &Vec<usize>) -> bool {
    let mut all_equal = true;
    indices.iter().for_each(|i| {
        if let Some(value) = tuple.get(*i) {
            match val {
                Arg::Cst(c) => {
                    match c {
                        Constant::Int(vi) => {
                            match value {
                                Constant::Int(vv) => {
                                    if vv != vi {
                                        all_equal = false;
                                    }
                                }
                                _ => {}
                            }
                        }
                        Constant::Str(vs) => {
                            match value {
                                Constant::Str(vv) => {
                                    if vs != vv {
                                        all_equal = false;
                                    }
                                }
                                _ => {}
                            }
                        }
                        Constant::JSONValue(vj) => {
                            match value {
                                Constant::JSONValue(vv) => {
                                    if vj != vv {
                                        all_equal = false;
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    });
    all_equal
}

fn format_record(rec: Vec<Constant>) -> String {
    let res = if rec.is_empty() {
        let res = "()".to_string();
        res
    } else if rec.len() == 1 {
        let mut res = "(".to_string();
        res += &*format!("{:?}", rec[0]).to_string();
        res = res + &*")".to_string();
        res
    } else if rec.len() == 2 {
        let mut res = "(".to_string();
        res += &*format!("{:?},", rec[0]).to_string();
        res += &*format!("{:?}", rec[1]).to_string();
        res = res + &*")".to_string();
        res
    } else {
        let mut res = "(".to_string();
        for arg in rec {
            res += &*format!("{},", arg).to_string()
        }
        res.pop();
        res = res + &*")".to_string();
        res
    };

    return res
}

fn anti_join_difference(lhs: HashSet<Vec<Constant>>, rhs: HashSet<Vec<Constant>>, lhs_keys: Vec<usize>, rhs_keys: Vec<usize>, ) -> Vec<Vec<Constant>> {
    let mut res: Vec<Vec<Constant>> = Vec::with_capacity(max(lhs.len(), rhs.len()));

    let mut rhs_hash_keys = HashSet::with_capacity(rhs.len());
    rhs.iter().for_each(|rh| {
        let mut tmp_vec = Vec::with_capacity(rhs_keys.len());
        rhs_keys.iter().for_each(|i| tmp_vec.push(rh[*i].clone()));
        rhs_hash_keys.insert(tmp_vec);
    });

    lhs.into_iter().for_each(|lh| {
        let mut lhs_signature = Vec::with_capacity(lhs_keys.len());
        lhs_keys.iter().for_each(|i| lhs_signature.push(lh[*i].clone()));

        if !rhs_hash_keys.contains(&lhs_signature) {
            res.push(lh)
        }
    });

    return res;
}

pub(crate) fn compute_alpha_satisfactions(alpha_ds: &mut PartialSequence, time_table: &HashMap<usize, usize>, time_interval: TimeInterval, beta_or_alpha : bool, record: Vec<Constant>, ) -> Vec<(usize, Vec<Constant>)> {
    let out = if beta_or_alpha {
        alpha_ds.single_output(record)
    } else {
        alpha_ds.output()
    };

    let fun = if alpha_ds.since_or_until {mem_since} else {mem_until};
    let mut new_res = Vec::with_capacity(out.len());

    out.iter().for_each(|(valuation, vals, bound)| {
        if vals.len() > 0 {
            let beta_ts = *time_table.get(&bound).unwrap_or(&0);
            for i in vals[0]..vals[1] + 1 {
                let alpha_ts = *time_table.get(&i).unwrap_or(&0);
                if fun(beta_ts, alpha_ts, time_interval) {
                    new_res.push((i, valuation.clone()));
                }
            }
        }
    });

    return new_res;
}

pub(crate) fn mem_until(beta_ts: usize, alpha_ts: usize, time_interval: TimeInterval) -> bool {
    let mut is_left_infinite = false;
    let mut a = 0;
    let mut is_right_infinite = false;
    let mut b = 0;

    match time_interval.get_start() {
        None => is_left_infinite = true,
        Some(x) => a = x,
    }

    match time_interval.get_end() {
        None => is_right_infinite = true,
        Some(x) => b = x,
    }

    if !is_right_infinite && !is_left_infinite {
        return alpha_ts + a <= beta_ts && alpha_ts + b >= beta_ts;
    }

    if is_right_infinite {
        // [a, inf) interval
        return alpha_ts + a <= beta_ts
    }

    return false
}

pub(crate) fn mem_since(beta_ts: usize, alpha_ts: usize, time_interval: TimeInterval) -> bool {
    let mut is_left_infinite = false;
    let mut a = 0;
    let mut is_right_infinite = false;
    let mut b = 0;

    match time_interval.get_start() {
        None => is_left_infinite = true,
        Some(x) => a = x,
    }

    match time_interval.get_end() {
        None => is_right_infinite = true,
        Some(x) => b = x,
    }

    if !is_right_infinite && !is_left_infinite {
        // [a, b] interval
        return beta_ts + a <= alpha_ts && alpha_ts <= beta_ts + b
    }

    if is_right_infinite && !(a > alpha_ts) {
        // [a, inf) interval
        return beta_ts <= alpha_ts - a
    }

    return false
}

// for since and until
pub(crate) fn process_beta(
    tp: usize, tp_to_ts: &mut HashMap<usize, usize>,
    tuple: Vec<Constant>, rhs_index_location: Vec<usize>, alphas: &mut HashMap<Vec<Constant>, PartialSequence>,
    interval: TimeInterval, mode: bool, unique_beta: &mut HashMap<usize, HashSet<Vec<Constant>>>) -> Vec<(usize, Vec<Constant>)> {
    let key_assignments = split_keys_values(tuple.clone(), rhs_index_location);

    // prevent duplication
    if unique_beta.contains_key(&tp) && unique_beta.entry(tp).or_default().contains(&tuple) {
        return vec![]
    }

    // get alpha
    if !alphas.contains_key(&key_assignments) {
        alphas.insert(key_assignments.clone(), PartialSequence::empty(mode));
    };

    // add subscriber
    let alphas_ds = alphas.entry(key_assignments).or_default();
    alphas_ds.add_subscriber(tuple.clone(), tp);

    // add new beta
    unique_beta.entry(tp).or_default().insert(tuple.clone());

    // check output
    let res = alphas_ds.subscriber.len();
    let mut satisfied = Vec::with_capacity(1 + res);

    // check if interval includes bound
    if match interval.get_start() { None => false, Some(x) => x == 0} {
        satisfied.push((tp, tuple.clone()));
    };

    let mut sat1 = Vec::with_capacity(alphas_ds.subscriber.len());
    sat1.append(&mut compute_alpha_satisfactions(alphas_ds, &tp_to_ts, interval, true, tuple));
    satisfied.append(&mut sat1);

    return satisfied;
}


// for since and until
pub(crate) fn process_alpha(tp: usize, tp_to_ts: &mut HashMap<usize, usize>, tuple: Vec<Constant>, alphas: &mut HashMap<Vec<Constant>, PartialSequence>, interval: TimeInterval, mode : bool, ) -> Vec<(usize, Vec<Constant>)> {
    // insert alpha tuple in data structure either by create a new entry or updating an existing one
    // also update all betas subscribed to the index entry or create an entry
    // insert alpha tuple in data structure either by create a new entry or updating an existing one
    if !alphas.contains_key(&tuple) {
        let mut ds = PartialSequence::empty(mode);
        ds.insert(tp);
        alphas.insert(tuple.clone(), ds);
    } else {
        alphas.entry(tuple.clone()).or_default().insert(tp);
    }

    let alpha_ds = alphas.entry(tuple).or_default();
    let mut subs_length = 0;
    alpha_ds.subscriber.iter().for_each(|(_k,v)| subs_length += v.len());
    let mut res = Vec::with_capacity(subs_length);
    res.append(&mut compute_alpha_satisfactions(alpha_ds, &tp_to_ts, interval, false, vec![]));
    return res;
}

fn is_relevant(current_tp: usize, bound: usize, mode: bool) -> bool {
    return if mode {
        // since case
        current_tp >= bound
    } else {
        current_tp <= bound
    };
}

pub(crate) fn get_wanted_indices(all: &Vec<String>, unwanted: &Vec<String>, ) -> (Vec<usize>, Vec<String>) {
    let mut indices = if all.len() > unwanted.len() {
        Vec::with_capacity(all.len()-unwanted.len())
    } else {
        Vec::new()
    };

    all.iter().enumerate().for_each(|(i, x)| {
        if !unwanted.contains(x) {
            indices.push(i);
        }
    });

    let mut attrs = all.clone();
    attrs.retain(|x| !unwanted.contains(&x));

    (indices, attrs)
}

pub(crate) fn get_wanted_values(tuple: Vec<Constant>, indices: &Vec<usize>) -> Vec<Constant> {
    let mut wanted_values = Vec::with_capacity(indices.len());

    indices.iter().for_each(|&i| {
        if let Some(value) = tuple.get(i) {
            wanted_values.push(value.clone());
        }
    });

    wanted_values
}

pub(crate) fn join_matching_tuples<D: Eq + Clone>(
    keys: &Vec<D>,
    non_keys1: &Vec<D>,
    non_keys2: &Vec<D>,
    is_lhs: bool,
) -> Vec<D> {
    return if is_lhs {
        let mut res: Vec<D> = non_keys1.clone();
        res.append(&mut (*keys).clone());
        res.append(&mut non_keys2.clone());
        res
    } else {
        let mut res: Vec<D> = non_keys2.clone();
        res.append(&mut (*keys).clone());
        res.append(&mut non_keys1.clone());
        res
    };
}

// get the common variables and the location of the common variables in fst
pub fn get_common_variables(lhs: Vec<String>, rhs: Vec<String>) -> (Vec<String>, Vec<usize>) {
    let mut common = Vec::new();
    let mut index = Vec::new();
    lhs.clone()
        .into_iter()
        .enumerate()
        .for_each(|(_i, var_name)| {
            rhs.clone()
                .into_iter()
                .enumerate()
                .for_each(|(j, var_name2)| {
                    if var_name == var_name2 {
                        common.push(var_name.clone());
                        index.push(j);
                    }
                })
        });
    (common, index)
}

// We assume here that all strings in 'fst' are unique and all strings in 'snd'
// are unique.
pub fn find_common_bound_variables(fst: Vec<String>, snd: Vec<String>, ) -> (Vec<String>, Vec<usize>, Vec<usize>) {
    let mut new_var_name_order = Vec::new();
    let mut lhs_key_indices = Vec::new();
    let mut rhs_key_indices = Vec::new();

    fst.clone().into_iter().enumerate()
        .for_each(|(i, var_name)| {
            snd.clone().into_iter().enumerate()
                .for_each(|(j, var_name2)| {
                    if var_name == var_name2 {
                        // First columns will be the common ones
                        new_var_name_order.push(var_name2);
                        lhs_key_indices.push(i);
                        rhs_key_indices.push(j);
                    }
                })
        });
    // After the common columns will be the non key columns from the first stream
    transfer_non_key_attrs_to(&mut new_var_name_order, fst, &lhs_key_indices);
    // Finally we add the non key columns from the second stream
    transfer_non_key_attrs_to(&mut new_var_name_order, snd, &rhs_key_indices);

    (new_var_name_order, lhs_key_indices, rhs_key_indices)
}

pub fn find_common_bound_variables1(fst: Vec<String>, snd: Vec<String>) -> (Vec<String>, Vec<usize>, Vec<usize>) {
    let mut lhs_key_indices = Vec::new();
    let mut rhs_key_indices = Vec::new();

    fst.clone()
        .into_iter()
        .enumerate()
        .for_each(|(i, var_name)| {
            snd.clone()
                .into_iter()
                .enumerate()
                .for_each(|(j, var_name2)| {
                    if var_name == var_name2 {
                        // First columns will be the common ones
                        lhs_key_indices.push(i);
                        rhs_key_indices.push(j);
                    }
                })
        });

    let mut keys = Vec::new();
    let mut non_keys_rhs = vec![];

    let mut non_keys_lhs: Vec<String> = vec![];
    for (i, &ref key) in fst.clone().iter().enumerate() {
        if !lhs_key_indices.contains(&i) {
            non_keys_lhs.push(key.clone())
        }
        else {
            keys.push(key.clone())
        }
    }

    for (i, &ref key) in snd.clone().iter().enumerate() {
        if !rhs_key_indices.contains(&i) {
            non_keys_rhs.push(key.clone())
        }
    }

    let mut tmp_order = non_keys_lhs.clone();
    tmp_order.append(&mut keys);
    tmp_order.append(&mut non_keys_rhs);

    (tmp_order, lhs_key_indices, rhs_key_indices)
}

pub fn find_common_bound_variables2(fst: Vec<String>, snd: Vec<String>, ) -> (Vec<String>, Vec<usize>, Vec<usize>) {
    let mut new_var_name_order = fst.clone();
    let mut lhs_key_indices = Vec::new();
    let mut rhs_key_indices = Vec::new();

    fst.clone().into_iter().enumerate()
        .for_each(|(i, var_name)| {
            snd.clone().into_iter().enumerate()
                .for_each(|(j, var_name2)| {
                    if var_name == var_name2 {
                        // First columns will be the common ones
                        lhs_key_indices.push(i);
                        rhs_key_indices.push(j);
                    }
                })
        });

    for s in snd {
        if !new_var_name_order.contains(&s) {
            new_var_name_order.push(s)
        }
    }

    (new_var_name_order, lhs_key_indices, rhs_key_indices)
}


fn transfer_non_key_attrs_to(attrs: &mut Vec<String>, vec: Vec<String>, key_indices: &Vec<usize>) {
    let (_keys, mut non_keys) = split_keys(vec, key_indices);
    attrs.append(&mut non_keys);
}

pub fn split_keys_values(vec: Vec<Constant>, key_indices: Vec<usize>) -> Vec<Constant> {
    let mut keys = Vec::with_capacity(key_indices.len());
    key_indices.iter().for_each(|i| {
        if let Some(elem) = vec.get(*i) {
            keys.push((*elem).clone());
        }
    });

    keys
}

pub fn split_keys<T: Clone>(vec: Vec<T>, key_indices: &Vec<usize>) -> (Vec<T>, Vec<T>) {
    let mut keys = Vec::with_capacity(vec.len());
    let mut non_keys = Vec::with_capacity(vec.len());

    key_indices.iter().for_each(|i| {
        if let Some(elem) = vec.get(*i) {
            keys.push(elem.clone());
        }
    });
    vec.into_iter().enumerate().for_each(|(i, elem)| {
        if !key_indices.contains(&i) {
            non_keys.push(elem);
        }
    });

    (keys, non_keys)
}

pub fn split_keys_ref<T: Clone>(vec: &Vec<T>, key_indices: &Vec<usize>) -> Vec<T> {
    let mut keys = Vec::with_capacity(key_indices.len());

    key_indices.iter().for_each(|i| {
        if let Some(elem) = vec.get(*i) {
            keys.push((*elem).clone());
        }
    });

    keys
}

pub(crate) fn save_keyed_tuple(key: &Vec<Constant>, non_key: &Vec<Constant>, time: usize, stash: &mut HashMap<usize, HashMap<Vec<Constant>, Vec<Vec<Constant>>>>, ) {
    stash.entry(time).or_insert_with(|| HashMap::with_capacity(16)).entry(key.clone())
        .or_insert_with(|| Vec::with_capacity(8)).push(non_key.clone());
}

pub(crate) fn save_tuple(tuple: Vec<Constant>, time: usize, stash: &mut HashMap<usize, HashSet<Vec<Constant>>>) {
    stash.entry(time).or_insert_with(||HashSet::with_capacity(8)).insert(tuple);
}

fn join_with_original_order(key: Vec<Constant>, non_key: Vec<Constant>, key_indices: Vec<usize>, time: usize, stash: &HashMap<usize, HashMap<Vec<Constant>, Vec<Vec<Constant>>>>, is_lhs: bool) -> Vec<Vec<Constant>> {
    let mut joined_tuples = Vec::new();
    if let Some(ohs_timed_data) = stash.get(&time) {
        if let Some(ohs_non_keys) = ohs_timed_data.get(&key) {
            if is_lhs {
                ohs_non_keys.into_iter().for_each(|ohs_non_key| {
                    let joined_tuple = join_matching_tuples_1(&key, &non_key, key_indices.clone(), &ohs_non_key);
                    joined_tuples.push(joined_tuple)
                });
            } else {
                ohs_non_keys.into_iter().for_each(|ohs_non_key| {
                    let joined_tuple = join_matching_tuples_1(&key, &ohs_non_key, key_indices.clone(), &non_key);
                    joined_tuples.push(joined_tuple)
                });
            }
        }
    }
    joined_tuples
}

pub(crate) fn join_matching_tuples_1<D: Eq + Clone>(lhs_keys: &Vec<D>, lhs_non_keys: &Vec<D>, key_indices : Vec<usize>, rhs_non_keys: &Vec<D>) -> Vec<D> {
    let length = lhs_keys.len() + lhs_non_keys.len() + rhs_non_keys.len();
    let mut tup = Vec::with_capacity(length);

    for k in lhs_non_keys { tup.push(k.clone()) }
    for (val, index) in zip(lhs_keys, key_indices) { tup.insert(index, val.clone()) }
    for k in rhs_non_keys { tup.push(k.clone())}

    tup
}

fn join_with_other(key: Vec<Constant>, non_key: Vec<Constant>, time: usize, stash: &HashMap<usize, HashMap<Vec<Constant>, Vec<Vec<Constant>>>>, is_lhs: bool) -> Vec<Vec<Constant>> {
    let mut joined_tuples = Vec::new();
    if let Some(ohs_timed_data) = stash.get(&time) {
        if let Some(ohs_non_keys) = ohs_timed_data.get(&key) {

            ohs_non_keys.into_iter().for_each(|ohs_non_key| {
                let joined_tuple = join_matching_tuples(&key, &non_key, &ohs_non_key, is_lhs);
                joined_tuples.push(joined_tuple)
            });
        }
    }
    joined_tuples
}

pub(crate) fn reorder_attributes(tuple: Vec<Constant>, indices: &Vec<usize>) -> Vec<Constant> {
    let mut keys = Vec::with_capacity(indices.len());
    indices.iter().for_each(|i| {
        if let Some(elem) = tuple.get(*i) {
            keys.push((*elem).clone());
        }
    });
    keys
}


pub(crate) fn restore_original_order(
    tuple: Vec<Arg>,
    fst: Vec<String>,
    snd: Vec<String>,
    new_var_order: Vec<String>,
) -> Vec<Arg> {
    let mut new_var_name_order = Vec::new();
    let mut lhs_key_indices = Vec::new();
    let mut rhs_key_indices = Vec::new();

    fst.clone()
        .into_iter()
        .enumerate()
        .for_each(|(i, var_name)| {
            snd.clone()
                .into_iter()
                .enumerate()
                .for_each(|(j, var_name2)| {
                    if var_name == var_name2 {
                        // First columns will be the common ones
                        new_var_name_order.push(var_name2);
                        lhs_key_indices.push(i);
                        rhs_key_indices.push(j);
                    }
                })
        });

    let mut tmp_snd = snd.clone();
    for t in rhs_key_indices.clone() {
        tmp_snd.remove(t);
    }
    let mut ordered_vec = fst.clone();
    ordered_vec.append(&mut tmp_snd);

    let mut pos: Vec<usize> = Vec::new();
    // find variable position from original in new_var_order
    for v in ordered_vec.clone() {
        new_var_order.iter().enumerate().for_each(|(ind, name)| {
            if v == *name {
                pos.push(ind.clone());
            }
        });
    }

    let mut vec_new_tup = Vec::new();
    let mut new_names = Vec::new();
    for ind in pos.clone() {
        vec_new_tup.push(tuple[ind].clone());
        new_names.push(new_var_order[ind].clone());
    }

    println!("Reordered in operator:");
    for x in new_names.clone() {
        print!(" {} ", x);
    }
    println!();

    return vec_new_tup;
}

pub(crate) fn get_var_indices(attrs: &Vec<String>, var: String) -> Vec<usize> {
    let mut indices = Vec::with_capacity(attrs.len());
    attrs.iter().enumerate().for_each(|(i, attr)| {
        if *attr == var {
            indices.push(i);
        }
    });
    indices
}

pub(crate) fn print_vec(vec1: Vec<Intervals>) {
    print!("[");
    for v in vec1.iter() {
        print!("{} ", v);
    }
    println!("]")
}

pub(crate) fn print_rec(rec: Vec<Arg>) {
    print!("Rec [ ");
    for r in rec.clone() {
        print!("{} ", r);
    }
    println!("]");
}

