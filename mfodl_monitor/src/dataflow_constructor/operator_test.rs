#![allow(dead_code)]
#[cfg(test)]
mod test {
    use rand::{Rng, StdRng};
    use std::cmp::max;
    use std::collections::HashMap;
    use std::vec;
    use {std, TS};

    use dataflow_constructor::operators::*;

    use dataflow_constructor::partial_sequence::Intervals;
    use dataflow_constructor::partial_sequence::Intervals::{Interval, Literal};
    use parser::formula_syntax_tree::{Arg, Constant};
    use ::{timely};
    use timely::dataflow::operators::capture::extract::Extract;
    use timely::dataflow::operators::{UnorderedInput, Probe, Capture, Input, Broadcast};
    use parser::formula_syntax_tree::Constant::{Int, Str};
    use timeunits::TimeInterval;
    use dataflow_constructor::types::{default_options, Record as Record, TimeFlowValues};
    use dataflow_constructor::types::TimeFlowValues::{EOS, Timestamp};


    #[derive(Abomonation, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
    pub enum StreamData {
        D(usize, Vec<i32>),
        M(usize, bool),
    }

    const NUM_WORKERS: usize = 2;
    const NUMBER_OF_ROUNDS: usize = 1;

    fn new_order_reverse(x: Vec<usize>) -> Vec<usize> {
        let mut tmp = x.clone();
        tmp.reverse();
        tmp
    }

    fn new_order_shuffle(x: Vec<usize>) -> Vec<usize> {
        let mut t = x.clone();
        let mut rng = StdRng::new().unwrap();
        rng.shuffle(&mut t);
        t
    }

    fn old_order(x: Vec<usize>) -> Vec<usize> {
        return x
    }

    #[test]
    fn test_unary_once_interval_one_one() {
        // betas
        let data_rhs = vec![
            vec![
                StreamData::D(0, vec![1, 10]),
                StreamData::D(0, vec![3, 10]),
                StreamData::D(0, vec![4, 10]),
            ],
            vec![StreamData::M(1, true)],
            vec![StreamData::D(2, vec![2, 100])],
            vec![StreamData::D(3, vec![5, 5])],
            vec![StreamData::D(4, vec![6, 6])],
            vec![StreamData::D(5, vec![7, 7])],
        ];

        let expected = vec![
            (1, vec![(1, vec![1, 10]), (1, vec![3, 10]), (1, vec![4, 10])]),
            (3, vec![(3, vec![2, 100])]),
            (4, vec![(4, vec![5, 5])]),
            (5, vec![(5, vec![6,6])])
        ];

        let times = vec![(0,0), (1,1), (2,2), (3,3), (4,4), (5,5)];

        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        let time_interval = TimeInterval::new(TS::new(1), TS::new(1));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_unary_once_eventually(
                0,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_unary_once_eventually(
                0,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }

        let expected = vec![
            (0, vec![(0, vec![1, 10]), (0, vec![3, 10]), (0, vec![4, 10])]),
            (2, vec![(2, vec![2, 100])]),
            (3, vec![(3, vec![5, 5])]),
            (4, vec![(4, vec![6,6])]),
            (5, vec![(5, vec![7,7])])
        ];

        let time_interval = TimeInterval::new(TS::new(0), TS::new(0));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_unary_once_eventually(
                0,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_unary_once_eventually(
                0,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }

        let expected = vec![
            (5, vec![(5, vec![1, 10]), (5, vec![3, 10]), (5, vec![4, 10])]),
        ];

        let time_interval = TimeInterval::new(TS::new(5), TS::new(5));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_unary_once_eventually(
                0,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_unary_once_eventually(
                0,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }
    }

    #[test]
    fn test_unary_once_interval_one_inf() {
        // betas
        let data_rhs = vec![
            vec![
                StreamData::D(0, vec![1, 10]),
                StreamData::D(0, vec![3, 10]),
                StreamData::D(0, vec![4, 10]),
            ],
            vec![StreamData::M(1, true)],
            vec![StreamData::D(2, vec![2, 100])],
            vec![StreamData::D(3, vec![5, 5])],
            vec![StreamData::D(4, vec![6, 6])],
            vec![StreamData::D(5, vec![7, 7])],
        ];

        let expected = vec![
            (1, vec![(1, vec![1, 10]), (1, vec![3, 10]), (1, vec![4, 10])]),
            (2, vec![(2, vec![1, 10]), (2, vec![3, 10]), (2, vec![4, 10])]),
            (3, vec![(3, vec![2, 100]), (3, vec![1, 10]), (3, vec![3, 10]), (3, vec![4, 10])]),
            (4, vec![(4, vec![5, 5]), (4, vec![2, 100]), (4, vec![1, 10]), (4, vec![3, 10]), (4, vec![4, 10])]),
            (5, vec![(5, vec![6,6]), (5, vec![5, 5]), (5, vec![2, 100]), (5, vec![1, 10]), (5, vec![3, 10]), (5, vec![4, 10])])
        ];

        let times = vec![(0,0), (1,1), (2,2), (3,3), (4,4), (5,5)];

        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        let time_interval = TimeInterval::new(TS::new(1), TS::INFINITY);
        for _x in 0..NUMBER_OF_ROUNDS {
            test_unary_once_eventually(
                0,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_unary_once_eventually(
                0,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }

        let expected = vec![
            (0, vec![(0, vec![1, 10]), (0, vec![3, 10]), (0, vec![4, 10])]),
            (1, vec![(1, vec![1, 10]), (1, vec![3, 10]), (1, vec![4, 10])]),
            (2, vec![(2, vec![2, 100]), (2, vec![1, 10]), (2, vec![3, 10]), (2, vec![4, 10])]),
            (3, vec![(3, vec![5, 5]), (3, vec![2, 100]), (3, vec![1, 10]), (3, vec![3, 10]), (3, vec![4, 10])]),
            (4, vec![(4, vec![6, 6]), (4, vec![5, 5]), (4, vec![2, 100]), (4, vec![1, 10]), (4, vec![3, 10]), (4, vec![4, 10])]),
            (5, vec![(5, vec![7, 7]), (5, vec![6, 6]), (5, vec![5, 5]), (5, vec![2, 100]), (5, vec![1, 10]), (5, vec![3, 10]), (5, vec![4, 10])])
        ];

        let time_interval = TimeInterval::new(TS::new(0), TS::INFINITY);
        for _x in 0..NUMBER_OF_ROUNDS {
            test_unary_once_eventually(
                0,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_unary_once_eventually(
                0,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }

        let expected = vec![
            (4, vec![(4, vec![1, 10]), (4, vec![3, 10]), (4, vec![4, 10])]),
            (5, vec![(5, vec![1, 10]), (5, vec![3, 10]), (5, vec![4, 10])]),
        ];

        let time_interval = TimeInterval::new(TS::new(4), TS::INFINITY);
        for _x in 0..NUMBER_OF_ROUNDS {
            test_unary_once_eventually(
                0,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_unary_once_eventually(
                0,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }
    }

    #[test]
    fn test_unary_once_interval_one_one_ts_equal_tp() {
        // betas
        let data_rhs = vec![
            vec![
                StreamData::D(10, vec![1, 10]),
                StreamData::D(10, vec![3, 10]),
                StreamData::D(10, vec![4, 10]),
            ],
            vec![StreamData::M(11, true)],
            vec![StreamData::D(12, vec![2, 100])],
            vec![StreamData::D(13, vec![5, 5])],
            vec![StreamData::D(14, vec![6, 6])],
            vec![StreamData::D(15, vec![7, 7])],
        ];

        let expected = vec![
            (1, vec![(11, vec![1, 10]), (11, vec![3, 10]), (11, vec![4, 10])]),
            (3, vec![(13, vec![2, 100])]),
            (4, vec![(14, vec![5, 5])]),
            (5, vec![(15, vec![6,6])])
        ];

        let times = vec![(0,10), (1,11), (2,12), (3,13), (4,14), (5,15)];

        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        let time_interval = TimeInterval::new(TS::new(1), TS::new(1));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_unary_once_eventually(
                0,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_unary_once_eventually(
                0,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }

        let expected = vec![
            (0, vec![(10, vec![1, 10]), (10, vec![3, 10]), (10, vec![4, 10])]),
            (2, vec![(12, vec![2, 100])]),
            (3, vec![(13, vec![5, 5])]),
            (4, vec![(14, vec![6,6])]),
            (5, vec![(15, vec![7,7])])
        ];

        let time_interval = TimeInterval::new(TS::new(0), TS::new(0));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_unary_once_eventually(
                0,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_unary_once_eventually(
                0,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }

        let expected = vec![
            (5, vec![(15, vec![1, 10]), (15, vec![3, 10]), (15, vec![4, 10])]),
        ];

        let time_interval = TimeInterval::new(TS::new(5), TS::new(5));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_unary_once_eventually(
                0,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_unary_once_eventually(
                0,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }
    }

    #[test]
    fn test_unary_once_interval_one_one_different_ts_tp1() {
        // betas
        let data_rhs = vec![
            vec![
                StreamData::D(10, vec![1, 10]),
                StreamData::D(10, vec![3, 10]),
                StreamData::D(10, vec![4, 10]),
            ],
            vec![StreamData::M(11, true)],
            vec![StreamData::D(12, vec![2, 100])],
            vec![StreamData::D(13, vec![5, 5])],
            vec![StreamData::D(14, vec![6, 6])],
            vec![StreamData::D(15, vec![7, 7])],
        ];

        let expected = vec![
            (1, vec![(11, vec![1, 10]), (11, vec![3, 10]), (11, vec![4, 10])]),
            (3, vec![(13, vec![2, 100])]),
            (4, vec![(14, vec![5, 5])]),
            (5, vec![(15, vec![6,6])])
        ];

        let times = vec![(0,10), (1,11), (2,12), (3,13), (4,14), (5,15)];

        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        let time_interval = TimeInterval::new(TS::new(1), TS::new(1));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_unary_once_eventually(
                0,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_unary_once_eventually(
                0,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }

        let expected = vec![
            (0, vec![(10, vec![1, 10]), (10, vec![3, 10]), (10, vec![4, 10])]),
            (2, vec![(12, vec![2, 100])]),
            (3, vec![(13, vec![5, 5])]),
            (4, vec![(14, vec![6,6])]),
            (5, vec![(15, vec![7,7])])
        ];

        let time_interval = TimeInterval::new(TS::new(0), TS::new(0));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_unary_once_eventually(
                0,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_unary_once_eventually(
                0,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }

        let expected = vec![
            (5, vec![(15, vec![1, 10]), (15, vec![3, 10]), (15, vec![4, 10])]),
        ];

        let time_interval = TimeInterval::new(TS::new(5), TS::new(5));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_unary_once_eventually(
                0,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_unary_once_eventually(
                0,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }
    }

    #[test]
    fn test_unary_once_interval_one_inf_different_ts_tp() {
        // betas
        let data_rhs = vec![
            vec![
                StreamData::D(10, vec![1, 10]),
                StreamData::D(10, vec![3, 10]),
                StreamData::D(10, vec![4, 10]),
            ],
            vec![StreamData::M(11, true)],
            vec![StreamData::D(12, vec![2, 100])],
            vec![StreamData::D(13, vec![5, 5])],
            vec![StreamData::D(14, vec![6, 6])],
            vec![StreamData::D(15, vec![7, 7])],
        ];

        let expected = vec![
            (1, vec![(1, vec![1, 10]), (1, vec![3, 10]), (1, vec![4, 10])]),
            (2, vec![(2, vec![1, 10]), (2, vec![3, 10]), (2, vec![4, 10])]),
            (3, vec![(3, vec![2, 100]), (3, vec![1, 10]), (3, vec![3, 10]), (3, vec![4, 10])]),
            (4, vec![(4, vec![5, 5]), (4, vec![2, 100]), (4, vec![1, 10]), (4, vec![3, 10]), (4, vec![4, 10])]),
            (5, vec![(5, vec![6,6]), (5, vec![5, 5]), (5, vec![2, 100]), (5, vec![1, 10]), (5, vec![3, 10]), (5, vec![4, 10])])
        ];

        let times = vec![(0,10), (1,11), (2,12), (3,13), (4,14), (5,15)];

        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        let time_interval = TimeInterval::new(TS::new(1), TS::INFINITY);
        for _x in 0..NUMBER_OF_ROUNDS {
            test_unary_once_eventually(
                0,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_unary_once_eventually(
                0,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }

        let expected = vec![
            (5, vec![(15, vec![1, 10]), (15, vec![3, 10]), (15, vec![4, 10])]),
        ];

        let time_interval = TimeInterval::new(TS::new(5), TS::INFINITY);
        for _x in 0..NUMBER_OF_ROUNDS {
            test_unary_once_eventually(
                0,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_unary_once_eventually(
                0,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }
    }

    #[test]
    fn test_unary_once_reorder() {
        // betas
        let data_rhs = vec![
            vec![StreamData::D(0, vec![1, 1]), StreamData::D(0, vec![3, 3])],
            vec![StreamData::M(1, true)],
            vec![StreamData::D(2, vec![2, 2])],
            vec![StreamData::D(3, vec![5, 5])],
            vec![StreamData::M(4, true)],
            vec![StreamData::D(5, vec![6, 6])],
        ];

        let expected = vec![
            (0, vec![(0, vec![1, 1]), (0, vec![3, 3])]),
            (1, vec![(1, vec![1, 1]), (1, vec![3, 3])]),
            (2, vec![(2, vec![1, 1]), (2, vec![2, 2]), (2, vec![3, 3])]),
            (
                3,
                vec![
                    (3, vec![1, 1]),
                    (3, vec![2, 2]),
                    (3, vec![3, 3]),
                    (3, vec![5, 5]),
                ],
            ),
            (
                4,
                vec![
                    (4, vec![1, 1]),
                    (4, vec![2, 2]),
                    (4, vec![3, 3]),
                    (4, vec![5, 5]),
                ],
            ),
            (
                5,
                vec![
                    (5, vec![1, 1]),
                    (5, vec![2, 2]),
                    (5, vec![3, 3]),
                    (5, vec![5, 5]),
                    (5, vec![6, 6]),
                ],
            ),
        ];

        let times = vec![(0,0), (1,1), (2,2), (3,3), (4,4), (5,5)];
        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        let time_interval = TimeInterval::new(TS::new(0), TS::INFINITY);
        for _x in 0..NUMBER_OF_ROUNDS {
            test_unary_once_eventually(
                0,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_unary_once_eventually(
                0,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }

        let time_interval = TimeInterval::new(TS::new(0), TS::new(7));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_unary_once_eventually(
                0,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_unary_once_eventually(
                0,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }

        let expected = vec![
            (1, vec![(1, vec![1, 1]), (1, vec![3, 3])]),
            (2, vec![(2, vec![1, 1]), (2, vec![3, 3])]),
            (3, vec![(3, vec![1, 1]), (3, vec![2, 2]), (3, vec![3, 3])]),
            (
                4,
                vec![
                    (4, vec![1, 1]),
                    (4, vec![2, 2]),
                    (4, vec![3, 3]),
                    (4, vec![5, 5]),
                ],
            ),
            (
                5,
                vec![
                    (5, vec![1, 1]),
                    (5, vec![2, 2]),
                    (5, vec![3, 3]),
                    (5, vec![5, 5]),
                ],
            ),
        ];

        let time_interval = TimeInterval::new(TS::new(1), TS::INFINITY);
        for _x in 0..NUMBER_OF_ROUNDS {
            test_unary_once_eventually(
                0,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_unary_once_eventually(
                0,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }

        let time_interval = TimeInterval::new(TS::new(1), TS::new(7));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_unary_once_eventually(
                0,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_unary_once_eventually(
                0,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }

        let expected = vec![
            (4, vec![(4, vec![1, 1]), (4, vec![3, 3])]),
            (5, vec![(5, vec![1, 1]), (5, vec![3, 3])]),
        ];

        let time_interval = TimeInterval::new(TS::new(4), TS::INFINITY);
        for _x in 0..NUMBER_OF_ROUNDS {
            test_unary_once_eventually(
                0,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_unary_once_eventually(
                0,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }

        let time_interval = TimeInterval::new(TS::new(4), TS::new(7));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_unary_once_eventually(
                0,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_unary_once_eventually(
                0,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }
    }
    #[test]
    fn test_unary_once() {
        let data_rhs = vec![
            vec![StreamData::M(0, true)],
            vec![StreamData::M(1, true)],
            vec![StreamData::D(2, vec![1, 1])]
        ];

        let expected = vec![(2, vec![(2, vec![1, 1])])];
        let times = vec![(0,0), (1,1), (2,2)];
        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        let time_interval = TimeInterval::new(TS::new(0), TS::INFINITY);
        for _x in 0..NUMBER_OF_ROUNDS {
            test_unary_once_eventually(
                0,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );
        }
    }

    #[test]
    fn test_unary_eventually_interval_one_inf() {
        // betas
        let data_rhs = vec![
            vec![
                StreamData::D(0, vec![1, 10]),
                StreamData::D(0, vec![3, 10]),
                StreamData::D(0, vec![4, 10]),
            ],
            vec![StreamData::M(1, true)],
            vec![StreamData::D(2, vec![2, 100])],
            vec![StreamData::D(3, vec![5, 5])],
            vec![StreamData::D(4, vec![6, 6])],
            vec![StreamData::D(5, vec![7, 7])],
        ];

        let expected = vec![
            (0, vec![(0, vec![2, 100]), (0, vec![5,5]), (0, vec![6,6]), (0, vec![7,7])]),
            (1, vec![(1, vec![2, 100]), (1, vec![5,5]), (1, vec![6,6]), (1, vec![7,7])]),
            (2, vec![(2, vec![5,5]), (2, vec![6,6]), (2, vec![7,7])]),
            (3, vec![(3, vec![6,6]), (3, vec![7,7])]),
            (4, vec![(4, vec![7,7])]),
        ];

        let times = vec![(0,0), (1,1), (2,2), (3,3), (4,4), (5,5)];

        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        let time_interval = TimeInterval::new(TS::new(1), TS::INFINITY);
        for _x in 0..NUMBER_OF_ROUNDS {
            test_unary_once_eventually(
                1,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_unary_once_eventually(
                1,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }

        let expected = vec![
            (0, vec![(0, vec![1, 10]), (0, vec![2, 100]), (0, vec![3, 10]), (0, vec![4, 10]), (0, vec![5,5]), (0, vec![6,6]), (0, vec![7,7])]),
            (1, vec![(1, vec![2, 100]), (1, vec![5,5]), (1, vec![6,6]), (1, vec![7,7])]),
            (2, vec![(2, vec![2, 100]), (2, vec![5,5]), (2, vec![6,6]), (2, vec![7,7])]),
            (3, vec![(3, vec![5,5]), (3, vec![6,6]), (3, vec![7,7])]),
            (4, vec![(4, vec![6,6]), (4, vec![7,7])]),
            (5, vec![(5, vec![7,7])]),
        ];

        let time_interval = TimeInterval::new(TS::new(0), TS::INFINITY);
        for _x in 0..NUMBER_OF_ROUNDS {
            test_unary_once_eventually(
                1,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_unary_once_eventually(
                1,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }

        let expected = vec![
            (0, vec![(0, vec![6,6]), (0, vec![7,7])]),
            (1, vec![(1, vec![7,7])]),
        ];

        let time_interval = TimeInterval::new(TS::new(4), TS::INFINITY);
        for _x in 0..NUMBER_OF_ROUNDS {
            test_unary_once_eventually(
                1,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_unary_once_eventually(
                1,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }
    }

    #[test]
    fn next_test() {
        let data_lhs = vec![
            vec![StreamData::M(0,true)],
            vec![StreamData::D(10, vec![1,2])],
            vec![StreamData::M(20, true)],
            vec![StreamData::D(30, vec![2,3])],
            vec![StreamData::D(40, vec![2,3])]
        ];

        let expected = vec![
            (0, vec![(0, vec![1, 2])]),
            (2, vec![(20, vec![2, 3])]),
            (3, vec![(30, vec![2, 3])])
        ];

        let times = vec![(0,0), (1,10), (2,20), (3,30), (4,40), (5,50)];

        let lhs_attrs: Vec<String> = vec!["x".into(), "y".into()];
        let time_interval = TimeInterval::new(TS::new(0), TS::new(10));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_next_prev_reordered(
                0,
                data_lhs.clone(),
                lhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_next_prev_reordered(
                0,
                data_lhs.clone(),
                lhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }

        let expected = vec![
            (0, vec![(0, vec![1, 2])]),
            (2, vec![(20, vec![2, 3])]),
            (3, vec![(30, vec![2, 3])])
        ];

        let times = vec![(0,0), (1,10), (2,20), (3,30), (4,40), (5,50)];

        let lhs_attrs: Vec<String> = vec!["x".into(), "y".into()];
        let time_interval = TimeInterval::new(TS::new(10), TS::new(10));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_next_prev_reordered(
                0,
                data_lhs.clone(),
                lhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_next_prev_reordered(
                0,
                data_lhs.clone(),
                lhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }

        let expected = vec![];

        let times = vec![(0,0), (1,10), (2,20), (3,30), (4,40), (5,50)];

        let lhs_attrs: Vec<String> = vec!["x".into(), "y".into()];
        let time_interval = TimeInterval::new(TS::new(0), TS::new(0));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_next_prev_reordered(
                0,
                data_lhs.clone(),
                lhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_next_prev_reordered(
                0,
                data_lhs.clone(),
                lhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }

        let data_lhs = vec![
            vec![StreamData::M(0,true)],
            vec![StreamData::D(1, vec![1,2])],
            vec![StreamData::M(2, true)],
            vec![StreamData::D(3, vec![2,3])],
            vec![StreamData::D(4, vec![2,3])]
        ];

        let expected = vec![
            (0, vec![(0, vec![1, 2])]),
            (2, vec![(2, vec![2, 3])]),
            (3, vec![(3, vec![2, 3])])
        ];

        let times = vec![(0,0), (1,1), (2,2), (3,3), (4,4), (5,5)];
        let lhs_attrs: Vec<String> = vec!["x".into(), "y".into()];
        let time_interval = TimeInterval::new(TS::new(0), TS::new(40));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_next_prev_reordered(
                0,
                data_lhs.clone(),
                lhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_next_prev_reordered(
                0,
                data_lhs.clone(),
                lhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }
    }

    #[test]
    fn next_test_property() {
        let data_lhs = vec![
            vec![StreamData::M(0,true)],
            vec![StreamData::D(10, vec![1,2])],
            vec![StreamData::M(20, true)],
            vec![StreamData::D(30, vec![2,3])],
            vec![StreamData::D(40, vec![2,3])]
        ];

        let expected = vec![
            (0, vec![(0, vec![1, 2])]),
            (2, vec![(20, vec![2, 3])]),
            (3, vec![(30, vec![2, 3])])
        ];

        let times = vec![(0,0), (1,10), (2,20), (3,30), (4,40), (5,50)];

        let vecs = &[0, 1, 2, 3, 4];
        let permutations_lhs = permutations(vecs);

        let lhs_attrs: Vec<String> = vec!["x".into(), "y".into()];
        let time_interval = TimeInterval::new(TS::new(0), TS::new(10));
        for rhs in permutations_lhs.clone() {
            test_next_prev_property(
                0,
                data_lhs.clone(),
                lhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                rhs.clone(),
                time_interval.clone(),
            );
        }

        let expected = vec![
            (0, vec![(0, vec![1, 2])]),
            (2, vec![(20, vec![2, 3])]),
            (3, vec![(30, vec![2, 3])])
        ];

        let times = vec![(0,0), (1,10), (2,20), (3,30), (4,40), (5,50)];

        let lhs_attrs: Vec<String> = vec!["x".into(), "y".into()];
        let time_interval = TimeInterval::new(TS::new(10), TS::new(10));
        for rhs in permutations_lhs.clone() {
            test_next_prev_property(
                0,
                data_lhs.clone(),
                lhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                rhs.clone(),
                time_interval.clone(),
            );
        }

        let expected = vec![];

        let times = vec![(0,0), (1,10), (2,20), (3,30), (4,40), (5,50)];

        let lhs_attrs: Vec<String> = vec!["x".into(), "y".into()];
        let time_interval = TimeInterval::new(TS::new(0), TS::new(0));
        for rhs in permutations_lhs.clone() {
            test_next_prev_property(
                0,
                data_lhs.clone(),
                lhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                rhs.clone(),
                time_interval.clone(),
            );
        }

        let data_lhs = vec![
            vec![StreamData::M(0,true)],
            vec![StreamData::D(1, vec![1,2])],
            vec![StreamData::M(2, true)],
            vec![StreamData::D(3, vec![2,3])],
            vec![StreamData::D(4, vec![2,3])]
        ];

        let expected = vec![
            (0, vec![(0, vec![1, 2])]),
            (2, vec![(2, vec![2, 3])]),
            (3, vec![(3, vec![2, 3])])
        ];

        let times = vec![(0,0), (1,1), (2,2), (3,3), (4,4), (5,5)];

        let lhs_attrs: Vec<String> = vec!["x".into(), "y".into()];
        let time_interval = TimeInterval::new(TS::new(0), TS::new(40));
        for rhs in permutations_lhs.clone() {
            test_next_prev_property(
                0,
                data_lhs.clone(),
                lhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                rhs.clone(),
                time_interval.clone(),
            );
        }
    }

    #[test]
    fn next_test_property_special() {
        let data_lhs = vec![
            vec![StreamData::M(0,true)],
            vec![StreamData::D(10, vec![1,2])],
            vec![StreamData::M(20, true)],
            vec![StreamData::D(30, vec![2,3])],
            vec![StreamData::D(40, vec![2,3])]
        ];

        let expected = vec![
            (0, vec![(0, vec![1, 2])]),
            (2, vec![(20, vec![2, 3])]),
            (3, vec![(30, vec![2, 3])])
        ];

        let times = vec![(0,0), (1,10), (2,20), (3,30), (4,40)];

        let vecs = &[0, 1, 2, 3, 4];
        let permutations_lhs = permutations(vecs);

        let lhs_attrs: Vec<String> = vec!["x".into(), "y".into()];
        for rhs in permutations_lhs.clone() {
            test_next_special_case(
                0,
                data_lhs.clone(),
                lhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                rhs.clone()
            );
        }

        let data_lhs = vec![
            vec![StreamData::D(0,vec![1,2])],
            vec![StreamData::M(1, true)],
            vec![StreamData::M(2, true)],
            vec![StreamData::D(3, vec![2,3])],
            vec![StreamData::D(4, vec![2,3])]
        ];

        let expected = vec![
            (2, vec![(2, vec![2, 3])]),
            (3, vec![(3, vec![2, 3])])
        ];

        let times = vec![(0,0), (1,1), (2,2), (3,3), (4,4), (5,5)];

        let lhs_attrs: Vec<String> = vec!["x".into(), "y".into()];
        for rhs in permutations_lhs.clone() {
            test_next_special_case(
                0,
                data_lhs.clone(),
                lhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                rhs.clone()
            );
        }

        let data_lhs = vec![
            vec![StreamData::M(0,true)],
            vec![StreamData::D(1, vec![1,2])],
            vec![StreamData::M(2, true)],
            vec![StreamData::M(3, true)],
            vec![StreamData::D(4, vec![2,3])]
        ];

        let expected = vec![
            (0, vec![(0, vec![1, 2])]),
            (3, vec![(3, vec![2, 3])])
        ];

        let times = vec![(0,0), (1,1), (2,2), (3,3), (4,4), (5,5)];

        let lhs_attrs: Vec<String> = vec!["x".into(), "y".into()];
        for rhs in permutations_lhs.clone() {
            test_next_special_case(
                0,
                data_lhs.clone(),
                lhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                rhs.clone()
            );
        }
    }

    #[test]
    fn prev_test() {
        let data_lhs = vec![
            vec![StreamData::M(0,true)],
            vec![StreamData::D(10, vec![1,2])],
            vec![StreamData::M(20, true)],
            vec![StreamData::D(30, vec![2,3])],
            vec![StreamData::D(40, vec![2,3])]
        ];

        let expected = vec![
            (2, vec![(20, vec![1, 2])]),
            (4, vec![(40, vec![2, 3])])
        ];

        let times = vec![(0,0), (1,10), (2,20), (3,30), (4,40), (5,50)];

        let lhs_attrs: Vec<String> = vec!["x".into(), "y".into()];
        let time_interval = TimeInterval::new(TS::new(0), TS::new(10));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_next_prev_reordered(
                1,
                data_lhs.clone(),
                lhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_next_prev_reordered(
                1,
                data_lhs.clone(),
                lhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }

        let expected = vec![
            (2, vec![(20, vec![1, 2])]),
            (4, vec![(40, vec![2, 3])])
        ];

        let times = vec![(0,0), (1,10), (2,20), (3,30), (4,40), (5,50)];
        let lhs_attrs: Vec<String> = vec!["x".into(), "y".into()];
        let time_interval = TimeInterval::new(TS::new(10), TS::new(10));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_next_prev_reordered(
                1,
                data_lhs.clone(),
                lhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_next_prev_reordered(
                1,
                data_lhs.clone(),
                lhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }

        let expected = vec![];

        let times = vec![(0,0), (1,10), (2,20), (3,30), (4,40), (5,50)];
        let lhs_attrs: Vec<String> = vec!["x".into(), "y".into()];
        let time_interval = TimeInterval::new(TS::new(0), TS::new(0));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_next_prev_reordered(
                1,
                data_lhs.clone(),
                lhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_next_prev_reordered(
                1,
                data_lhs.clone(),
                lhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }

        let data_lhs = vec![
            vec![StreamData::M(0,true)],
            vec![StreamData::D(1, vec![1,2])],
            vec![StreamData::M(2, true)],
            vec![StreamData::D(3, vec![2,3])],
            vec![StreamData::D(4, vec![2,3])]
        ];

        let expected = vec![
            (2, vec![(2, vec![1, 2])]),
            (4, vec![(4, vec![2, 3])])
        ];

        let times = vec![(0,0), (1,1), (2,2), (3,3), (4,4), (5,5)];
        let lhs_attrs: Vec<String> = vec!["x".into(), "y".into()];
        let time_interval = TimeInterval::new(TS::new(0), TS::new(40));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_next_prev_reordered(
                1,
                data_lhs.clone(),
                lhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_next_prev_reordered(
                1,
                data_lhs.clone(),
                lhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }
    }

    #[test]
    fn prev_test_property() {
        let data_lhs = vec![
            vec![StreamData::M(0,true)],
            vec![StreamData::D(10, vec![1,2])],
            vec![StreamData::M(20, true)],
            vec![StreamData::D(30, vec![2,3])],
            vec![StreamData::D(40, vec![2,3])]
        ];

        let expected = vec![
            (2, vec![(20, vec![1, 2])]),
            (4, vec![(40, vec![2, 3])])
        ];

        let times = vec![(0,0), (1,10), (2,20), (3,30), (4,40), (5,50)];

        let vecs = &[0, 1, 2, 3, 4];
        let permutations = permutations(vecs);

        let lhs_attrs: Vec<String> = vec!["x".into(), "y".into()];
        let time_interval = TimeInterval::new(TS::new(0), TS::new(10));
        for rhs in permutations.clone() {
                test_next_prev_property(
                    1,
                    data_lhs.clone(),
                    lhs_attrs.clone(),
                    times.clone(),
                    expected.clone(),
                    rhs.clone(),
                    time_interval.clone(),
                );
        }

        let expected = vec![
            (2, vec![(20, vec![1, 2])]),
            (4, vec![(40, vec![2, 3])])
        ];

        let times = vec![(0,0), (1,10), (2,20), (3,30), (4,40), (5,50)];

        let lhs_attrs: Vec<String> = vec!["x".into(), "y".into()];
        let time_interval = TimeInterval::new(TS::new(10), TS::new(10));
        for rhs in permutations.clone() {
            test_next_prev_property(
                1,
                data_lhs.clone(),
                lhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                rhs.clone(),
                time_interval.clone(),
            );
        }

        let expected = vec![];

        let times = vec![(0,0), (1,10), (2,20), (3,30), (4,40), (5,50)];

        let lhs_attrs: Vec<String> = vec!["x".into(), "y".into()];
        let time_interval = TimeInterval::new(TS::new(0), TS::new(0));
        for rhs in permutations.clone() {
            test_next_prev_property(
                1,
                data_lhs.clone(),
                lhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                rhs.clone(),
                time_interval.clone(),
            );
        }

        let data_lhs = vec![
            vec![StreamData::M(0,true)],
            vec![StreamData::D(1, vec![1,2])],
            vec![StreamData::M(2, true)],
            vec![StreamData::D(3, vec![2,3])],
            vec![StreamData::D(4, vec![2,3])]
        ];

        let expected = vec![
            (2, vec![(2, vec![1, 2])]),
            (4, vec![(4, vec![2, 3])])
        ];

        let times = vec![(0,0), (1,1), (2,2), (3,3), (4,4), (5,5)];

        let lhs_attrs: Vec<String> = vec!["x".into(), "y".into()];
        let time_interval = TimeInterval::new(TS::new(0), TS::new(40));
        for rhs in permutations.clone() {
            test_next_prev_property(
                1,
                data_lhs.clone(),
                lhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                rhs.clone(),
                time_interval.clone(),
            );
        }
    }

    #[test]
    fn prev_test_property_special() {
        let data_lhs = vec![
            vec![StreamData::M(0,true)],
            vec![StreamData::D(10, vec![1,2])],
            vec![StreamData::M(20, true)],
            vec![StreamData::D(30, vec![2,3])],
            vec![StreamData::D(40, vec![2,3])]
        ];

        let expected = vec![
            (2, vec![(20, vec![1, 2])]),
            (4, vec![(40, vec![2, 3])])
        ];

        let times = vec![(0,0), (1,10), (2,20), (3,30), (4,40)];

        let vecs = &[0, 1, 2, 3, 4];
        let permutations = permutations(vecs);

        let lhs_attrs: Vec<String> = vec!["x".into(), "y".into()];
        for rhs in permutations.clone() {
            test_prev_special_case(
                data_lhs.clone(),
                lhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                rhs.clone(),
            );
        }

        let data_lhs = vec![
            vec![StreamData::M(0,true)],
            vec![StreamData::M(10, true)],
            vec![StreamData::M(20, true)],
            vec![StreamData::M(30, true)],
            vec![StreamData::D(40, vec![2,3])]
        ];

        let expected = vec![];

        for rhs in permutations.clone() {
            test_prev_special_case(
                data_lhs.clone(),
                lhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                rhs.clone(),
            );
        }

        let data_lhs = vec![
            vec![StreamData::M(0,true)],
            vec![StreamData::M(10, true)],
            vec![StreamData::D(20, vec![1,2])],
            vec![StreamData::M(30, true)],
            vec![StreamData::D(40, vec![2,3])]
        ];

        let expected = vec![
            (3, vec![(30, vec![1, 2])])
        ];

        for rhs in permutations.clone() {
            test_prev_special_case(
                data_lhs.clone(),
                lhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                rhs.clone(),
            );
        }

        let data_lhs = vec![
            vec![StreamData::D(0,vec![1,2])],
            vec![StreamData::M(10, true)],
            vec![StreamData::M(20, true)],
            vec![StreamData::M(30, true)],
            vec![StreamData::D(40, vec![2,3])]
        ];

        let expected = vec![
            (1, vec![(10, vec![1, 2])])
        ];

        for rhs in permutations.clone() {
            test_prev_special_case(
                data_lhs.clone(),
                lhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                rhs.clone(),
            );
        }
    }

    #[test]
    fn one_one_interval_once_different_ts_tp() {
        let data_lhs = vec![
            vec![StreamData::D(10, vec![])],
            vec![StreamData::D(11, vec![])],
            vec![StreamData::D(12, vec![])],
            vec![StreamData::D(13, vec![])],
            vec![StreamData::D(14, vec![])],
            vec![StreamData::D(15, vec![])]
        ];

        // betas
        let data_rhs = vec![
            vec![
                StreamData::D(10, vec![1, 10]),
                StreamData::D(10, vec![3, 10]),
                StreamData::D(10, vec![4, 10]),
            ],
            vec![StreamData::M(11, true)],
            vec![StreamData::D(12, vec![2, 100])],
            vec![StreamData::D(13, vec![5, 5])],
            vec![StreamData::D(14, vec![6, 6])],
            vec![StreamData::D(15, vec![7, 7])],
        ];

        let expected = vec![
            (1, vec![(11, vec![1, 10]), (11, vec![3, 10]), (11, vec![4, 10])]),
            (3, vec![(13, vec![2, 100])]),
            (4, vec![(14, vec![5, 5])]),
            (5, vec![(15, vec![6,6])])
        ];

        let times = vec![(0,10), (1,11), (2,12), (3,13), (4,14), (5,15)];

        let lhs_attrs: Vec<String> = vec![];
        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        let time_interval = TimeInterval::new(TS::new(1), TS::new(1));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_temporal_reordered(
                0,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_temporal_reordered(
                0,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }

        let expected = vec![
            (0, vec![(10, vec![1, 10]), (10, vec![3, 10]), (10, vec![4, 10])]),
            (2, vec![(12, vec![2, 100])]),
            (3, vec![(13, vec![5, 5])]),
            (4, vec![(14, vec![6,6])]),
            (5, vec![(15, vec![7,7])])
        ];

        let time_interval = TimeInterval::new(TS::new(0), TS::new(0));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_temporal_reordered(
                0,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_temporal_reordered(
                0,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }

        let expected = vec![
            (5, vec![(15, vec![1, 10]), (15, vec![3, 10]), (15, vec![4, 10])]),
        ];

        let time_interval = TimeInterval::new(TS::new(5), TS::new(5));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_temporal_reordered(
                0,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_temporal_reordered(
                0,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }
    }

    #[test]
    fn one_one_interval_eventually() {
        let data_lhs = vec![
            vec![StreamData::D(0, vec![])],
            vec![StreamData::D(1, vec![])],
            vec![StreamData::D(2, vec![])],
            vec![StreamData::D(3, vec![])],
            vec![StreamData::D(4, vec![])],
            vec![StreamData::D(5, vec![])]
        ];

        // betas
        let data_rhs = vec![
            vec![
                StreamData::D(0, vec![1, 10]),
                StreamData::D(0, vec![3, 10]),
                StreamData::D(0, vec![4, 10]),
            ],
            vec![StreamData::M(1, true)],
            vec![StreamData::D(2, vec![2, 100])],
            vec![StreamData::D(3, vec![5, 5])],
            vec![StreamData::D(4, vec![6, 6])],
            vec![StreamData::D(5, vec![7, 7])],
        ];

        let expected = vec![
            (1, vec![(1, vec![2, 100])]),
            (2, vec![(2, vec![5, 5])]),
            (3, vec![(3, vec![6, 6])]),
            (4, vec![(4, vec![7, 7])])
        ];

        let times = vec![(0,0), (1,1), (2,2), (3,3), (4,4), (5,5)];

        let lhs_attrs: Vec<String> = vec![];
        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        let time_interval = TimeInterval::new(TS::new(1), TS::new(1));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_temporal_reordered(
                1,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_temporal_reordered(
                1,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }


        let expected = vec![
            (0, vec![(0, vec![1, 10]), (0, vec![3, 10]), (0, vec![4, 10])]),
            (2, vec![(2, vec![2, 100])]),
            (3, vec![(3, vec![5, 5])]),
            (4, vec![(4, vec![6, 6])]),
            (5, vec![(5, vec![7, 7])])
        ];

        let time_interval = TimeInterval::new(TS::new(0), TS::new(0));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_temporal_reordered(
                1,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_temporal_reordered(
                1,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }

        let expected = vec![
            (0, vec![(0, vec![7, 7])])
        ];

        let time_interval = TimeInterval::new(TS::new(5), TS::new(5));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_temporal_reordered(
                1,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_temporal_reordered(
                1,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }
    }

    #[test]
    fn unary_one_one_interval_eventually() {
        // betas
        let data_rhs = vec![
            vec![
                StreamData::D(0, vec![1, 10]),
                StreamData::D(0, vec![3, 10]),
                StreamData::D(0, vec![4, 10]),
            ],
            vec![StreamData::M(1, true)],
            vec![StreamData::D(2, vec![2, 100])],
            vec![StreamData::D(3, vec![5, 5])],
            vec![StreamData::D(4, vec![6, 6])],
            vec![StreamData::D(5, vec![7, 7])],
        ];

        let expected = vec![
            (1, vec![(1, vec![2, 100])]),
            (2, vec![(2, vec![5, 5])]),
            (3, vec![(3, vec![6, 6])]),
            (4, vec![(4, vec![7, 7])])
        ];

        let times = vec![(0,0), (1,1), (2,2), (3,3), (4,4), (5,5)];

        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        let time_interval = TimeInterval::new(TS::new(1), TS::new(1));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_unary_once_eventually(
                1,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_unary_once_eventually(
                1,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }


        let expected = vec![
            (0, vec![(0, vec![1, 10]), (0, vec![3, 10]), (0, vec![4, 10])]),
            (2, vec![(2, vec![2, 100])]),
            (3, vec![(3, vec![5, 5])]),
            (4, vec![(4, vec![6, 6])]),
            (5, vec![(5, vec![7, 7])])
        ];

        let time_interval = TimeInterval::new(TS::new(0), TS::new(0));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_unary_once_eventually(
                1,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_unary_once_eventually(
                1,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }

        let expected = vec![
            (0, vec![(0, vec![7, 7])])
        ];

        let time_interval = TimeInterval::new(TS::new(5), TS::new(5));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_unary_once_eventually(
                1,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_unary_once_eventually(
                1,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }
    }

    #[test]
    fn one_one_interval_eventually_unary() {
        // betas
        let data_rhs = vec![
            vec![
                StreamData::D(0, vec![1, 10]),
                StreamData::D(0, vec![3, 10]),
                StreamData::D(0, vec![4, 10]),
            ],
            vec![StreamData::M(1, true)],
            vec![StreamData::D(2, vec![2, 100])],
            vec![StreamData::D(3, vec![5, 5])],
            vec![StreamData::D(4, vec![6, 6])],
            vec![StreamData::D(5, vec![7, 7])],
        ];

        let expected = vec![
            (1, vec![(1, vec![2, 100])]),
            (2, vec![(2, vec![5, 5])]),
            (3, vec![(3, vec![6, 6])]),
            (4, vec![(4, vec![7, 7])])
        ];

        let times = vec![(0,0), (1,1), (2,2), (3,3), (4,4), (5,5)];
        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        let time_interval = TimeInterval::new(TS::new(1), TS::new(1));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_unary_once_eventually(
                1,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_unary_once_eventually(
                1,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }


        let expected = vec![
            (0, vec![(0, vec![1, 10]), (0, vec![3, 10]), (0, vec![4, 10])]),
            (2, vec![(2, vec![2, 100])]),
            (3, vec![(3, vec![5, 5])]),
            (4, vec![(4, vec![6, 6])]),
            (5, vec![(5, vec![7, 7])])
        ];

        let time_interval = TimeInterval::new(TS::new(0), TS::new(0));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_unary_once_eventually(
                1,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_unary_once_eventually(
                1,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }

        let expected = vec![
            (0, vec![(0, vec![7, 7])])
        ];

        let time_interval = TimeInterval::new(TS::new(5), TS::new(5));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_unary_once_eventually(
                1,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_unary_once_eventually(
                1,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }
    }

    #[test]
    fn one_one_interval_eventually_different_ts_tp() {
        // betas
        let data_rhs = vec![
            vec![
                StreamData::D(10, vec![1, 10]),
                StreamData::D(10, vec![3, 10]),
                StreamData::D(10, vec![4, 10]),
            ],
            vec![StreamData::M(11, true)],
            vec![StreamData::D(12, vec![2, 100])],
            vec![StreamData::D(13, vec![5, 5])],
            vec![StreamData::D(14, vec![6, 6])],
            vec![StreamData::D(15, vec![7, 7])],
        ];

        let expected = vec![
            (1, vec![(11, vec![2, 100])]),
            (2, vec![(12, vec![5, 5])]),
            (3, vec![(13, vec![6, 6])]),
            (4, vec![(14, vec![7, 7])])
        ];

        let times = vec![(0,10), (1,11), (2,12), (3,13), (4,14), (5,15)];
        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        let time_interval = TimeInterval::new(TS::new(1), TS::new(1));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_unary_once_eventually(
                1,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_unary_once_eventually(
                1,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }


        let expected = vec![
            (0, vec![(10, vec![1, 10]), (10, vec![3, 10]), (10, vec![4, 10])]),
            (2, vec![(12, vec![2, 100])]),
            (3, vec![(13, vec![5, 5])]),
            (4, vec![(14, vec![6, 6])]),
            (5, vec![(15, vec![7, 7])])
        ];

        let time_interval = TimeInterval::new(TS::new(0), TS::new(0));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_unary_once_eventually(
                1,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_unary_once_eventually(
                1,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }

        let expected = vec![
            (0, vec![(10, vec![7, 7])])
        ];

        let time_interval = TimeInterval::new(TS::new(5), TS::new(5));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_unary_once_eventually(
                1,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_unary_once_eventually(
                1,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }
    }

    #[test]
    fn one_one_interval_eventually_unary_different_ts_tp() {
        // betas
        let data_rhs = vec![
            vec![
                StreamData::D(10, vec![1, 10]),
                StreamData::D(10, vec![3, 10]),
                StreamData::D(10, vec![4, 10]),
            ],
            vec![StreamData::M(11, true)],
            vec![StreamData::D(12, vec![2, 100])],
            vec![StreamData::D(13, vec![5, 5])],
            vec![StreamData::D(14, vec![6, 6])],
            vec![StreamData::D(15, vec![7, 7])],
        ];

        let expected = vec![
            (1, vec![(11, vec![2, 100])]),
            (2, vec![(12, vec![5, 5])]),
            (3, vec![(13, vec![6, 6])]),
            (4, vec![(14, vec![7, 7])])
        ];

        let times = vec![(0,10), (1,11), (2,12), (3,13), (4,14), (5,15)];
        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        let time_interval = TimeInterval::new(TS::new(1), TS::new(1));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_unary_once_eventually(
                1,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_unary_once_eventually(
                1,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }


        let expected = vec![
            (0, vec![(10, vec![1, 10]), (10, vec![3, 10]), (10, vec![4, 10])]),
            (2, vec![(12, vec![2, 100])]),
            (3, vec![(13, vec![5, 5])]),
            (4, vec![(14, vec![6, 6])]),
            (5, vec![(15, vec![7, 7])])
        ];

        let time_interval = TimeInterval::new(TS::new(0), TS::new(0));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_unary_once_eventually(
                1,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_unary_once_eventually(
                1,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }

        let expected = vec![
            (0, vec![(10, vec![7, 7])])
        ];

        let time_interval = TimeInterval::new(TS::new(5), TS::new(5));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_unary_once_eventually(
                1,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_unary_once_eventually(
                1,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }
    }

    #[test]
    fn eventually_unary_test() {
        let data_rhs = vec![
            vec![StreamData::M(0, true)],
            vec![StreamData::M(1, true)],
            vec![StreamData::D(2, vec![1, 1])]
        ];

        let expected = vec![
            (0, vec![(0, vec![1, 1])]),
            (1, vec![(1, vec![1, 1])]),
            (2, vec![(2, vec![1, 1])])
        ];

        let times = vec![(0,0), (1,1), (2,2)];
        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        let time_interval = TimeInterval::new(TS::new(0), TS::new(7));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_unary_once_eventually(
                1,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_unary_once_eventually(
                1,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }
    }

    #[test]
    fn reorder_eventually_unary() {
        // betas
        let data_rhs = vec![
            vec![StreamData::D(0, vec![1, 1]), StreamData::D(0, vec![3, 3])],
            vec![StreamData::M(1, true)],
            vec![StreamData::D(2, vec![2, 2])],
            vec![StreamData::D(3, vec![5, 5])],
            vec![StreamData::M(4, true)],
            vec![StreamData::D(5, vec![6, 6])],
        ];

        let expected = vec![
            (0, vec![(0, vec![1, 1]), (0, vec![2, 2]), (0, vec![3, 3]), (0, vec![5, 5]), (0, vec![6, 6])]),
            (1, vec![(1, vec![2, 2]), (1, vec![5, 5]), (1, vec![6, 6])]),
            (2, vec![(2, vec![2, 2]), (2, vec![5, 5]), (2, vec![6, 6])]),
            (3, vec![(3, vec![5, 5]), (3, vec![6, 6])]),
            (4, vec![(4, vec![6, 6])]),
            (5, vec![(5, vec![6, 6])]),
        ];

        let times = vec![(0,0), (1,1), (2,2), (3,3), (4,4), (5,5)];
        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        let time_interval = TimeInterval::new(TS::new(0), TS::new(7));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_unary_once_eventually(
                1,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_unary_once_eventually(
                1,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }

        let expected = vec![
            (0, vec![(0, vec![2, 2]), (0, vec![5, 5]), (0, vec![6, 6])]),
            (1, vec![(1, vec![2, 2]), (1, vec![5, 5]), (1, vec![6, 6])]),
            (2, vec![(2, vec![5, 5]), (2, vec![6, 6])]),
            (3, vec![(3, vec![6, 6])]),
            (4, vec![(4, vec![6, 6])]),
        ];

        let time_interval = TimeInterval::new(TS::new(1), TS::new(7));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_unary_once_eventually(
                1,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_unary_once_eventually(
                1,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }

        let expected = vec![
            (0, vec![(0, vec![6, 6])]),
            (1, vec![(1, vec![6, 6])]),
        ];

        let time_interval = TimeInterval::new(TS::new(4), TS::new(7));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_unary_once_eventually(
                1,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_unary_once_eventually(
                1,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }
    }

    #[test]
    fn property_eventually_special() {
        // betas
        let data_rhs = vec![
            vec![StreamData::D(10, vec![1, 10]), StreamData::D(10, vec![3, 10]), StreamData::D(10, vec![4, 10]), ],
            vec![StreamData::M(11, true)],
            vec![StreamData::D(12, vec![2, 100])],
            vec![StreamData::D(13, vec![5, 5])],
            vec![StreamData::D(14, vec![6, 6])],
            vec![StreamData::D(15, vec![7, 7])],
        ];

        let expected = vec![
            (0, vec![(10, vec![1,10]), (10, vec![2,100]), (10, vec![3,10]), (10, vec![4,10]), (10, vec![5,5]), (10, vec![6,6]), (10, vec![7,7])]),
            (1, vec![(11, vec![2,100]), (11, vec![5,5]), (11, vec![6,6]), (11, vec![7,7])]),
            (2, vec![(12, vec![2,100]), (12, vec![5,5]), (12, vec![6,6]), (12, vec![7,7])]),
            (3, vec![(13, vec![5,5]), (13, vec![6,6]), (13, vec![7,7])]),
            (4, vec![(14, vec![6,6]), (14, vec![7,7])]),
            (5, vec![(15, vec![7, 7])])
        ];

        let times = vec![(0,10), (1,11), (2,12), (3,13), (4,14), (5,15)];

        let vecs = &[0, 1, 2, 3, 4, 5];
        let permuts = permutations(vecs);

        let lhs_attrs: Vec<String> = vec!["x".into(), "y".into()];
        for rhs in permuts.clone() {
            test_unary_eventually_special(
                data_rhs.clone(),
                lhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                rhs.clone(),
            );
        }

        let data_rhs = vec![
            vec![StreamData::M(10, true)],
            vec![StreamData::M(11, true)],
            vec![StreamData::M(12, true)],
            vec![StreamData::M(13, true)],
            vec![StreamData::M(14, true)],
            vec![StreamData::D(15, vec![7, 7])],
        ];

        let expected = vec![
            (0, vec![(10, vec![7,7])]),
            (1, vec![(11, vec![7,7])]),
            (2, vec![(12, vec![7,7])]),
            (3, vec![(13, vec![7,7])]),
            (4, vec![(14, vec![7,7])]),
            (5, vec![(15, vec![7, 7])])
        ];

        let times = vec![(0,10), (1,11), (2,12), (3,13), (4,14), (5,15)];

        let vecs = &[0, 1, 2, 3, 4, 5];
        let permuts1 = permutations(vecs);

        let lhs_attrs: Vec<String> = vec!["x".into(), "y".into()];
        for rhs in permuts1.clone() {
            test_unary_eventually_special(
                data_rhs.clone(),
                lhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                rhs.clone(),
            );
        }

        let data_rhs = vec![
            vec![StreamData::M(10, true)],
            vec![StreamData::D(11, vec![7, 7])],
        ];

        let expected = vec![
            (0, vec![(10, vec![7,7])]),
            (1, vec![(11, vec![7,7])]),
        ];

        let times = vec![(0,10), (1,11)];

        let vecs = &[0, 1];
        let permuts1 = permutations(vecs);

        let lhs_attrs: Vec<String> = vec!["x".into(), "y".into()];
        for rhs in permuts1.clone() {
            test_unary_eventually_special(
                data_rhs.clone(),
                lhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                rhs.clone(),
            );
        }

        let data_rhs = vec![
            vec![StreamData::D(10, vec![7, 7])],
        ];

        let expected = vec![
            (0, vec![(10, vec![7,7])])
        ];

        let times = vec![(0,10)];

        let vecs = &[0];
        let permuts1 = permutations(vecs);

        let lhs_attrs: Vec<String> = vec!["x".into(), "y".into()];
        for rhs in permuts1.clone() {
            test_unary_eventually_special(
                data_rhs.clone(),
                lhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                rhs.clone(),
            );
        }

        let data_rhs = vec![
            vec![StreamData::M(10, true)],
            vec![StreamData::M(11, true)],
            vec![StreamData::M(12, true)],
            vec![StreamData::M(13, true)],
            vec![StreamData::M(14, true)],
            vec![StreamData::M(15, true)],
        ];

        let expected = vec![];

        let times = vec![(0,10), (1,11), (2,12), (3,13), (4,14), (5,15)];

        let vecs = &[0, 1, 2, 3, 4, 5];
        let permuts1 = permutations(vecs);

        let lhs_attrs: Vec<String> = vec!["x".into(), "y".into()];
        for rhs in permuts1.clone() {
            test_unary_eventually_special(
                data_rhs.clone(),
                lhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                rhs.clone(),
            );
        }

        let data_rhs = vec![
            vec![StreamData::M(10, true)],
            vec![StreamData::D(11, vec![7, 7])],
            vec![StreamData::M(12, true)],
            vec![StreamData::D(13, vec![7, 7])],
            vec![StreamData::M(14, true)],
            vec![StreamData::D(15, vec![7, 7])],
        ];

        let expected = vec![
            (0, vec![(10, vec![7,7])]),
            (1, vec![(11, vec![7,7])]),
            (2, vec![(12, vec![7,7])]),
            (3, vec![(13, vec![7,7])]),
            (4, vec![(14, vec![7,7])]),
            (5, vec![(15, vec![7, 7])])
        ];

        let times = vec![(0,10), (1,11), (2,12), (3,13), (4,14), (5,15)];

        let vecs = &[0, 1, 2, 3, 4, 5];
        let permuts1 = permutations(vecs);

        let lhs_attrs: Vec<String> = vec!["x".into(), "y".into()];
        for rhs in permuts1.clone() {
            test_unary_eventually_special(
                data_rhs.clone(),
                lhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                rhs.clone(),
            );
        }

        let data_rhs = vec![
            vec![StreamData::D(10, vec![7, 7])],
            vec![StreamData::D(11, vec![7, 7])],
            vec![StreamData::D(12, vec![7, 7])],
            vec![StreamData::D(13, vec![7, 7])],
            vec![StreamData::D(14, vec![7, 7])],
            vec![StreamData::D(15, vec![7, 7])],
        ];

        let expected = vec![
            (0, vec![(10, vec![7,7])]),
            (1, vec![(11, vec![7,7])]),
            (2, vec![(12, vec![7,7])]),
            (3, vec![(13, vec![7,7])]),
            (4, vec![(14, vec![7,7])]),
            (5, vec![(15, vec![7, 7])])
        ];

        let times = vec![(0,10), (1,11), (2,12), (3,13), (4,14), (5,15)];

        let vecs = &[0, 1, 2, 3, 4, 5];
        let permuts1 = permutations(vecs);

        let lhs_attrs: Vec<String> = vec!["x".into(), "y".into()];
        for rhs in permuts1.clone() {
            test_unary_eventually_special(
                data_rhs.clone(),
                lhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                rhs.clone(),
            );
        }
    }


    #[test]
    fn property_once_special() {
        // betas
        let data_rhs = vec![
            vec![StreamData::D(10, vec![1, 10]), StreamData::D(10, vec![3, 10]), StreamData::D(10, vec![4, 10]), ],
            vec![StreamData::M(11, true)],
            vec![StreamData::D(12, vec![2, 100])],
            vec![StreamData::D(13, vec![5, 5])],
            vec![StreamData::D(14, vec![6, 6])],
            vec![StreamData::D(15, vec![7, 7])],
        ];

        let expected = vec![
            (0, vec![(10, vec![1,10]), (10, vec![3,10]), (10, vec![4,10])]),
            (1, vec![(11, vec![1,10]), (11, vec![3,10]), (11, vec![4,10])]),
            (2, vec![(12, vec![2,100]), (12, vec![1,10]), (12, vec![3,10]), (12, vec![4,10])]),
            (3, vec![(13, vec![5,5]), (13, vec![2,100]), (13, vec![1,10]), (13, vec![3,10]), (13, vec![4,10])]),
            (4, vec![(14, vec![6,6]), (14, vec![5,5]), (14, vec![2,100]), (14, vec![1,10]), (14, vec![3,10]), (14, vec![4,10])]),
            (5, vec![(15, vec![7, 7]), (15, vec![6,6]), (15, vec![5,5]), (15, vec![2,100]), (15, vec![1,10]), (15, vec![3,10]), (15, vec![4,10])]),
        ];

        let times = vec![(0,10), (1,11), (2,12), (3,13), (4,14), (5,15)];

        let vecs = &[0, 1, 2, 3, 4, 5];
        let permuts = permutations(vecs);

        let lhs_attrs: Vec<String> = vec!["x".into(), "y".into()];
        for rhs in permuts.clone() {
            test_unary_once_special(
                data_rhs.clone(),
                lhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                rhs.clone(),
            );
        }

        let data_rhs = vec![
            vec![StreamData::D(10, vec![1, 10]), StreamData::D(10, vec![3, 10]), StreamData::D(10, vec![4, 10]), ],
            vec![StreamData::M(11, true)],
            vec![StreamData::D(12, vec![2, 100])],
            vec![StreamData::D(13, vec![5, 5])],
            vec![StreamData::D(14, vec![6, 6])],
            vec![StreamData::M(15, true)],
        ];

        let expected = vec![
            (0, vec![(10, vec![1,10]), (10, vec![3,10]), (10, vec![4,10])]),
            (1, vec![(11, vec![1,10]), (11, vec![3,10]), (11, vec![4,10])]),
            (2, vec![(12, vec![2,100]), (12, vec![1,10]), (12, vec![3,10]), (12, vec![4,10])]),
            (3, vec![(13, vec![5,5]), (13, vec![2,100]), (13, vec![1,10]), (13, vec![3,10]), (13, vec![4,10])]),
            (4, vec![(14, vec![6,6]), (14, vec![5,5]), (14, vec![2,100]), (14, vec![1,10]), (14, vec![3,10]), (14, vec![4,10])]),
            (5, vec![(15, vec![6,6]), (15, vec![5,5]), (15, vec![2,100]), (15, vec![1,10]), (15, vec![3,10]), (15, vec![4,10])]),
        ];

        let times = vec![(0,10), (1,11), (2,12), (3,13), (4,14), (5,15)];

        let vecs = &[0, 1, 2, 3, 4, 5];
        let permuts = permutations(vecs);

        let lhs_attrs: Vec<String> = vec!["x".into(), "y".into()];
        for rhs in permuts.clone() {
            test_unary_once_special(
                data_rhs.clone(),
                lhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                rhs.clone(),
            );
        }

        let data_rhs = vec![
            vec![StreamData::D(10, vec![1, 10])],
            vec![StreamData::M(11, true)],
            vec![StreamData::D(12, vec![1, 10])],
            vec![StreamData::D(13, vec![1, 10])],
            vec![StreamData::D(14, vec![1, 10])],
            vec![StreamData::M(15, true)],
        ];

        let expected = vec![
            (0, vec![(10, vec![1,10])]),
            (1, vec![(11, vec![1,10])]),
            (2, vec![(12, vec![1,10])]),
            (3, vec![(13, vec![1,10])]),
            (4, vec![(14, vec![1,10])]),
            (5, vec![(15, vec![1,10])]),
        ];

        let times = vec![(0,10), (1,11), (2,12), (3,13), (4,14), (5,15)];

        let vecs = &[0, 1, 2, 3, 4, 5];
        let permuts = permutations(vecs);

        let lhs_attrs: Vec<String> = vec!["x".into(), "y".into()];
        for rhs in permuts.clone() {
            test_unary_once_special(
                data_rhs.clone(),
                lhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                rhs.clone(),
            );
        }
    }

    // negative since tests ---------------------------------------------------------------------------------

    #[test]
    fn test_neg_since_property() {
        //alphas
        let data_lhs = vec![
            vec![StreamData::M(0, true)
            ],
            vec![StreamData::D(1, vec![1]),
                 StreamData::D(1, vec![3])
            ],
            vec![StreamData::D(2, vec![1]),
                 StreamData::D(2, vec![2]),
                 StreamData::D(2, vec![3]),
            ],
            vec![StreamData::D(3, vec![1]),
                 StreamData::D(3, vec![2])
            ],
            vec![StreamData::D(4, vec![1]),
                 StreamData::D(4, vec![2])
            ],
            vec![StreamData::D(5, vec![1])],
        ];

        // betas
        let data_rhs = vec![
            vec![
                StreamData::D(0, vec![1, 10]),
                StreamData::D(0, vec![3, 10]),
                StreamData::D(0, vec![4, 10]),
            ],
            vec![StreamData::M(1, true)],
            vec![StreamData::D(2, vec![2, 100])],
            vec![StreamData::D(3, vec![5, 5])],
            vec![StreamData::D(4, vec![6, 6])],
            vec![StreamData::D(5, vec![7, 7])],
        ];

        let expected = vec![
            (0, vec![(0, vec![1, 10]), (0, vec![3, 10]), (0, vec![4, 10])]),
            (1, vec![(1, vec![4, 10])]),
            (2, vec![(2, vec![2, 100]), (2, vec![4, 10])]),
            (3, vec![(3, vec![5, 5]), (3, vec![4, 10])]),
            (4, vec![(4, vec![5, 5]), (4, vec![6, 6]), (4, vec![4, 10])]),
            (5, vec![(5, vec![4, 10]), (5, vec![5, 5]), (5, vec![6, 6]), (5, vec![7, 7])]),
        ];

        let times = vec![(0,0), (1,1), (2,2), (3,3), (4,4), (5,5)];

        let lhs_attrs: Vec<String> = vec!["x".into()];
        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        let time_interval = TimeInterval::new(TS::new(0), TS::INFINITY);

        let vecs = &[0, 1, 2, 3, 4, 5];
        let permuts = permutations(vecs);

        for rhs in permuts.clone() {
            for lhs in permuts.clone() {
                test_negative_temporal_property(true,data_lhs.clone(), data_rhs.clone(), lhs_attrs.clone(), rhs_attrs.clone(), times.clone(), expected.clone(), rhs.clone(), lhs.clone(), time_interval);
            }
        }
    }

    #[test]
    fn test_neg_since_property1() {
        //alphas
        let data_lhs = vec![
            vec![StreamData::M(0, true)
            ],
            vec![StreamData::D(1, vec![1]),
                 StreamData::D(1, vec![3])
            ],
            vec![StreamData::D(2, vec![1]),
                 StreamData::D(2, vec![2]),
                 StreamData::D(2, vec![3]),
            ],
            vec![StreamData::D(3, vec![1]),
                 StreamData::D(3, vec![2])
            ],
            vec![StreamData::D(4, vec![1]),
                 StreamData::D(4, vec![2])
            ],
            vec![StreamData::D(5, vec![1])],
        ];

        // betas
        let data_rhs = vec![
            vec![
                StreamData::D(0, vec![1, 10]),
                StreamData::D(0, vec![3, 10]),
                StreamData::D(0, vec![4, 10]),
            ],
            vec![StreamData::M(1, true)],
            vec![StreamData::D(2, vec![2, 100])],
            vec![StreamData::D(3, vec![5, 5])],
            vec![StreamData::D(4, vec![6, 6])],
            vec![StreamData::D(5, vec![7, 7])],
        ];

        let times = vec![(0,0), (1,1), (2,2), (3,3), (4,4), (5,5)];

        let lhs_attrs: Vec<String> = vec!["x".into()];
        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        let vecs = &[0, 1, 2, 3, 4, 5];
        let permuts = permutations(vecs);

        let expected = vec![
            (1, vec![(1, vec![4, 10])]),
            (2, vec![(2, vec![4, 10])]),
            (3, vec![(3, vec![4, 10])]),
            (4, vec![(4, vec![5, 5]), (4, vec![4, 10])]),
            (5, vec![(5, vec![4, 10]), (5, vec![5, 5]), (5, vec![6, 6])]),
        ];

        let time_interval = TimeInterval::new(TS::new(1), TS::INFINITY);
        for rhs in permuts.clone() {
            for lhs in &permuts.clone() {
                test_negative_temporal_property(true, data_lhs.clone(), data_rhs.clone(), lhs_attrs.clone(), rhs_attrs.clone(), times.clone(), expected.clone(), rhs.clone(), lhs.clone(), time_interval);
            }
        }
    }

    #[test]
    fn test_neg_since_property2() {
        //alphas
        let data_lhs = vec![
            vec![StreamData::M(0, true)
            ],
            vec![StreamData::D(1, vec![1]),
                 StreamData::D(1, vec![3])
            ],
            vec![StreamData::D(2, vec![1]),
                 StreamData::D(2, vec![2]),
                 StreamData::D(2, vec![3]),
            ],
            vec![StreamData::D(3, vec![1]),
                 StreamData::D(3, vec![2])
            ],
            vec![StreamData::D(4, vec![1]),
                 StreamData::D(4, vec![2])
            ],
            vec![StreamData::D(5, vec![1])],
        ];

        // betas
        let data_rhs = vec![
            vec![
                StreamData::D(0, vec![1, 10]),
                StreamData::D(0, vec![3, 10]),
                StreamData::D(0, vec![4, 10]),
            ],
            vec![StreamData::M(1, true)],
            vec![StreamData::D(2, vec![2, 100]), StreamData::D(2, vec![4, 10])],
            vec![StreamData::D(3, vec![5, 5])],
            vec![StreamData::D(4, vec![6, 6]), StreamData::D(4, vec![1, 10])],
            vec![StreamData::D(5, vec![7, 7])],
        ];

        let times = vec![(0,0), (1,1), (2,2), (3,3), (4,4), (5,5)];

        let lhs_attrs: Vec<String> = vec!["x".into()];
        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        let vecs = &[0, 1, 2, 3, 4, 5];
        let permuts = permutations(vecs);

        let expected = vec![
            (1, vec![(1, vec![4, 10])]),
            (2, vec![(2, vec![4, 10])]),
            (3, vec![(3, vec![4, 10])]),
            (4, vec![(4, vec![5, 5]), (4, vec![4, 10])]),
            (5, vec![(5, vec![4, 10]), (5, vec![5, 5]), (5, vec![6, 6])]),
        ];

        let time_interval = TimeInterval::new(TS::new(1), TS::INFINITY);
        for rhs in permuts.clone() {
            for lhs in permuts.clone() {
                test_negative_temporal_property(true,data_lhs.clone(), data_rhs.clone(), lhs_attrs.clone(), rhs_attrs.clone(), times.clone(), expected.clone(), rhs.clone(), lhs.clone(), time_interval);
            }
        }
    }

    #[test]
    fn test_neg_since_property3() {
        //alphas
        let data_lhs = vec![
            vec![StreamData::M(0, true)
            ],
            vec![StreamData::D(1, vec![1]),
                 StreamData::D(1, vec![3])
            ],
            vec![StreamData::D(2, vec![1]),
                 StreamData::D(2, vec![2]),
                 StreamData::D(2, vec![3]),
            ],
            vec![StreamData::D(3, vec![1]),
                 StreamData::D(3, vec![2])
            ],
            vec![StreamData::D(4, vec![1]),
                 StreamData::D(4, vec![2])
            ],
            vec![StreamData::D(5, vec![1])],
        ];

        // betas
        let data_rhs = vec![
            vec![
                StreamData::D(0, vec![1, 10]),
                StreamData::D(0, vec![3, 10]),
                StreamData::D(0, vec![4, 10]),
            ],
            vec![StreamData::M(1, true)],
            vec![StreamData::D(2, vec![2, 100]), StreamData::D(2, vec![4, 10])],
            vec![StreamData::D(3, vec![5, 5])],
            vec![StreamData::D(4, vec![6, 6]), StreamData::D(4, vec![1, 10])],
            vec![StreamData::D(5, vec![7, 7])],
        ];

        let times = vec![(0,0), (1,1), (2,2), (3,3), (4,4), (5,5)];

        let lhs_attrs: Vec<String> = vec!["x".into()];
        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        let vecs = &[0, 1, 2, 3, 4, 5];
        let permuts = permutations(vecs);

        let expected = vec![
            (0, vec![(0, vec![1,10]), (0, vec![3,10]), (0, vec![4,10])]),
            (1, vec![(1, vec![4, 10])]),
            (2, vec![(2, vec![2,100]), (2, vec![4, 10])]),
            (3, vec![(3, vec![4, 10]), (3, vec![5, 5])]),
            (4, vec![(4, vec![1,10]), (4, vec![4, 10]), (4, vec![5, 5]), (4, vec![6, 6])]),
            (5, vec![(5, vec![4, 10]), (5, vec![5, 5]), (5, vec![6, 6]), (5, vec![7, 7]),]),
        ];

        let time_interval = TimeInterval::new(TS::new(0), TS::INFINITY);
        for rhs in permuts.clone() {
            for lhs in permuts.clone() {
                test_negative_temporal_property(true,data_lhs.clone(), data_rhs.clone(), lhs_attrs.clone(), rhs_attrs.clone(), times.clone(), expected.clone(), rhs.clone(), lhs.clone(), time_interval);
            }
        }
    }

    #[test]
    fn test_neg_since_property4() {
        //alphas
        let data_lhs = vec![
            vec![StreamData::M(0, true)
            ],
            vec![StreamData::D(1, vec![1]),
                 StreamData::D(1, vec![3])
            ],
            vec![StreamData::D(2, vec![1]),
                 StreamData::D(2, vec![2]),
                 StreamData::D(2, vec![3]),
            ],
            vec![StreamData::D(3, vec![1]),
                 StreamData::D(3, vec![2])
            ],
            vec![StreamData::D(4, vec![1]),
                 StreamData::D(4, vec![2])
            ],
        ];

        // betas
        let data_rhs = vec![
            vec![
                StreamData::D(0, vec![1, 10]),
                StreamData::D(0, vec![3, 10]),
                StreamData::D(0, vec![4, 10]),
            ],
            vec![StreamData::M(1, true)],
            vec![StreamData::D(2, vec![2, 100]), StreamData::D(2, vec![4, 10])],
            vec![StreamData::D(3, vec![5, 5])],
            vec![StreamData::D(4, vec![6, 6]), StreamData::D(4, vec![1, 10])],
        ];

        let times = vec![(0,0), (1,1), (2,2), (3,3), (4,4)];

        let lhs_attrs: Vec<String> = vec!["x".into()];
        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        let vecs = &[0, 1, 2, 3, 4];
        let permuts = permutations(vecs);

        let expected = vec![
            (1, vec![(1, vec![4, 10])]),
            (2, vec![(2, vec![4, 10])]),
            (3, vec![(3, vec![4, 10])]),
            (4, vec![(4, vec![4, 10]), (4, vec![5, 5])])
        ];

        let time_interval = TimeInterval::new(TS::new(1), TS::new(2));
        for rhs in permuts.clone() {
            for lhs in permuts.clone() {
                test_negative_temporal_property(true,data_lhs.clone(), data_rhs.clone(), lhs_attrs.clone(), rhs_attrs.clone(), times.clone(), expected.clone(), rhs.clone(), lhs.clone(), time_interval);
            }
        }
    }

    // negative until tests
    #[test]
    fn test_neg_until_property_test() {
        //alphas
        let data_lhs = vec![
            vec![StreamData::M(0, true)
            ],
            vec![StreamData::D(1, vec![1]),
                 StreamData::D(1, vec![3])
            ],
            vec![StreamData::D(2, vec![1]),
                 StreamData::D(2, vec![2]),
                 StreamData::D(2, vec![3]),
            ],
            vec![StreamData::D(3, vec![1]),
                 StreamData::D(3, vec![2])
            ],
            vec![StreamData::D(4, vec![1]),
                 StreamData::D(4, vec![2])
            ]
        ];

        // betas
        let data_rhs = vec![
            vec![
                StreamData::D(0, vec![1, 10]),
                StreamData::D(0, vec![3, 10]),
                StreamData::D(0, vec![4, 10]),
            ],
            vec![StreamData::M(1, true)],
            vec![StreamData::D(2, vec![2, 100])],
            vec![StreamData::D(3, vec![5, 5])],
            vec![StreamData::D(4, vec![6, 6])]
        ];

        let expected = vec![
            (0, vec![(0, vec![6, 6])]),
        ];

        let times = vec![(0,0), (1,1), (2,2), (3,3), (4,4)];

        let lhs_attrs: Vec<String> = vec!["x".into()];
        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        let time_interval = TimeInterval::new(TS::new(4), TS::new(5));

        let vecs = &[0, 1, 2, 3, 4];
        let permuts = permutations(vecs);

        for rhs in permuts.clone() {
            for lhs in permuts.clone() {
                test_negative_temporal_property(false,data_lhs.clone(), data_rhs.clone(), lhs_attrs.clone(), rhs_attrs.clone(), times.clone(), expected.clone(), rhs.clone(), lhs.clone(), time_interval);
            }
        }
    }

    #[test]
    fn test_neg_until_random() {
        //alphas
        let data_lhs = vec![
            vec![StreamData::D(0, vec![24])
            ],
            vec![StreamData::D(1, vec![24])
            ],
            vec![StreamData::D(2, vec![24])
            ],
        ];

        // betas
        let data_rhs = vec![
            vec![StreamData::M(0, true)],
            vec![StreamData::D(1, vec![1, 1])],
            vec![StreamData::D(2, vec![1, 2]),
                 StreamData::D(2, vec![1, 24]),
                 StreamData::D(2, vec![1, 23]),
            ],
        ];

        let expected = vec![
            (0, vec![(0, vec![1,1])]),
            (1, vec![(1, vec![1,2]), (1, vec![1, 23])])
        ];

        let times = vec![(0,0), (1,1), (2,2), (3,3)];

        let lhs_attrs: Vec<String> = vec!["y".into()];
        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        let time_interval = TimeInterval::new(TS::new(1), TS::new(1));

        let vecs = &[0, 1, 2];
        let permuts = permutations(vecs);

        for rhs in permuts.clone() {
            for lhs in permuts.clone() {
                test_negative_temporal_property(false,data_lhs.clone(), data_rhs.clone(), lhs_attrs.clone(), rhs_attrs.clone(), times.clone(), expected.clone(), rhs.clone(), lhs.clone(), time_interval);
            }
        }
    }

    #[test]
    fn test_neg_until_property() {
        //alphas
        let data_lhs = vec![
            vec![StreamData::M(0, true)
            ],
            vec![StreamData::D(1, vec![1]),
                 StreamData::D(1, vec![3])
            ],
            vec![StreamData::D(2, vec![1]),
                 StreamData::D(2, vec![2]),
                 StreamData::D(2, vec![3]),
            ],
            vec![StreamData::D(3, vec![1]),
                 StreamData::D(3, vec![2])
            ],
            vec![StreamData::D(4, vec![1]),
                 StreamData::D(4, vec![2])
            ],
            vec![StreamData::D(5, vec![1])],
        ];

        // betas
        let data_rhs = vec![
            vec![
                StreamData::D(0, vec![1, 10]),
                StreamData::D(0, vec![3, 10]),
                StreamData::D(0, vec![4, 10]),
            ],
            vec![StreamData::M(1, true)],
            vec![StreamData::D(2, vec![2, 100])],
            vec![StreamData::D(3, vec![5, 5])],
            vec![StreamData::D(4, vec![6, 6])],
            vec![StreamData::D(5, vec![7, 7])],
        ];

        let expected = vec![
            (0, vec![(0, vec![6, 6]), (0, vec![7, 7])]),
            (1, vec![(1, vec![7, 7])]),
        ];

        let times = vec![(0,0), (1,1), (2,2), (3,3), (4,4), (5,5)];

        let lhs_attrs: Vec<String> = vec!["x".into()];
        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        let time_interval = TimeInterval::new(TS::new(4), TS::new(5));

        let vecs = &[0, 1, 2, 3, 4, 5];
        let permuts = permutations(vecs);

        for rhs in permuts.clone() {
            for lhs in permuts.clone() {
                test_negative_temporal_property(false,data_lhs.clone(), data_rhs.clone(), lhs_attrs.clone(), rhs_attrs.clone(), times.clone(), expected.clone(), rhs.clone(), lhs.clone(), time_interval);
            }
        }
    }

    #[test]
    fn test_neg_until_property1() {
        //alphas
        let data_lhs = vec![
            vec![StreamData::M(0, true)
            ],
            vec![StreamData::D(1, vec![1]),
                 StreamData::D(1, vec![3])
            ],
            vec![StreamData::D(2, vec![1]),
                 StreamData::D(2, vec![2]),
                 StreamData::D(2, vec![3]),
            ],
            vec![StreamData::D(3, vec![1]),
                 StreamData::D(3, vec![2])
            ],
            vec![StreamData::D(4, vec![1]),
                 StreamData::D(4, vec![2])
            ],
            vec![StreamData::D(5, vec![1])],
        ];

        // betas
        let data_rhs = vec![
            vec![
                StreamData::D(0, vec![1, 10]),
                StreamData::D(0, vec![3, 10]),
                StreamData::D(0, vec![4, 10]),
            ],
            vec![StreamData::M(1, true)],
            vec![StreamData::D(2, vec![2, 100])],
            vec![StreamData::D(3, vec![5, 5])],
            vec![StreamData::D(4, vec![6, 6])],
            vec![StreamData::D(5, vec![7, 7])],
        ];

        let times = vec![(0,0), (1,1), (2,2), (3,3), (4,4), (5,5)];

        let lhs_attrs: Vec<String> = vec!["x".into()];
        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        let vecs = &[0, 1, 2, 3, 4, 5];
        let permuts = permutations(vecs);

        let expected = vec![
            (0, vec![(0, vec![2, 100])]),
            (1, vec![(1, vec![2, 100]), (1, vec![5, 5])]),
            (2, vec![(2, vec![5, 5]), (2, vec![6, 6])]),
            (3, vec![(3, vec![6, 6]), (3, vec![7, 7])]),
            (4, vec![(4, vec![7, 7])]),
        ];

        let time_interval = TimeInterval::new(TS::new(1), TS::new(2));
        for rhs in permuts.clone() {
            for lhs in &permuts.clone() {
                test_negative_temporal_property(false, data_lhs.clone(), data_rhs.clone(), lhs_attrs.clone(), rhs_attrs.clone(), times.clone(), expected.clone(), rhs.clone(), lhs.clone(), time_interval);
            }
        }
    }

    #[test]
    fn test_neg_until_property2() {
        //alphas
        let data_lhs = vec![
            vec![StreamData::M(0, true)
            ],
            vec![StreamData::D(1, vec![1]),
                 StreamData::D(1, vec![3])
            ],
            vec![StreamData::D(2, vec![1]),
                 StreamData::D(2, vec![2]),
                 StreamData::D(2, vec![3]),
            ],
            vec![StreamData::D(3, vec![1]),
                 StreamData::D(3, vec![2])
            ],
            vec![StreamData::D(4, vec![1]),
                 StreamData::D(4, vec![2])
            ],
        ];

        // betas
        let data_rhs = vec![
            vec![
                StreamData::D(0, vec![1, 10]),
                StreamData::D(0, vec![3, 10]),
                StreamData::D(0, vec![4, 10]),
            ],
            vec![StreamData::M(1, true)],
            vec![StreamData::D(2, vec![2, 100]), StreamData::D(2, vec![4, 10])],
            vec![StreamData::D(3, vec![5, 5])],
            vec![StreamData::D(4, vec![6, 6]), StreamData::D(4, vec![1, 10])],
        ];

        let times = vec![(0,0), (1,1), (2,2), (3,3), (4,4)];

        let lhs_attrs: Vec<String> = vec!["x".into()];
        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        let vecs = &[0, 1, 2, 3, 4];
        let permuts = permutations(vecs);

        let expected = vec![
            (0, vec![(0, vec![2, 100]), (0, vec![4, 10]), (0, vec![5, 5]), (0, vec![6, 6])]),
            (1, vec![(1, vec![2, 100]), (1, vec![4, 10]), (1, vec![5, 5]), (1, vec![6, 6])]),
            (2, vec![(2, vec![5, 5]), (2, vec![6, 6])]),
            (3, vec![(3, vec![6, 6])]),
        ];

        let time_interval = TimeInterval::new(TS::new(1), TS::INFINITY);
        for rhs in permuts.clone() {
            for lhs in permuts.clone() {
                test_negative_temporal_property(false,data_lhs.clone(), data_rhs.clone(), lhs_attrs.clone(), rhs_attrs.clone(), times.clone(), expected.clone(), rhs.clone(), lhs.clone(), time_interval);
            }
        }
    }

    #[test]
    fn test_neg_until_property3() {
        //alphas
        let data_lhs = vec![
            vec![StreamData::M(0, true)
            ],
            vec![StreamData::D(1, vec![1]),
                 StreamData::D(1, vec![3])
            ],
            vec![StreamData::D(2, vec![1]),
                 StreamData::D(2, vec![2]),
                 StreamData::D(2, vec![3]),
            ],
            vec![StreamData::D(3, vec![1]),
                 StreamData::D(3, vec![2])
            ],
            vec![StreamData::D(4, vec![1]),
                 StreamData::D(4, vec![2])
            ],
            vec![StreamData::D(5, vec![1])],
        ];

        // betas
        let data_rhs = vec![
            vec![
                StreamData::D(0, vec![1, 10]),
                StreamData::D(0, vec![3, 10]),
                StreamData::D(0, vec![4, 10]),
            ],
            vec![StreamData::M(1, true)],
            vec![StreamData::D(2, vec![2, 100]), StreamData::D(2, vec![4, 10])],
            vec![StreamData::D(3, vec![5, 5])],
            vec![StreamData::D(4, vec![6, 6]), StreamData::D(4, vec![1, 10])],
            vec![StreamData::D(5, vec![7, 7])],
        ];

        let times = vec![(0,0), (1,1), (2,2), (3,3), (4,4), (5,5)];

        let lhs_attrs: Vec<String> = vec!["x".into()];
        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        let vecs = &[0, 1, 2, 3, 4, 5];
        let permuts = permutations(vecs);

        let expected = vec![
            (0, vec![(0, vec![1, 10]), (0, vec![2, 100]), (0, vec![3, 10]), (0, vec![4, 10]), (0, vec![5, 5]), (0, vec![6, 6]), (0, vec![7, 7])]),
            (1, vec![(1, vec![2, 100]), (1, vec![4, 10]), (1, vec![5, 5]), (1, vec![6, 6]), (1, vec![7, 7])]),
            (2, vec![(2, vec![2, 100]), (2, vec![4, 10]), (2, vec![5, 5]), (2, vec![6, 6]), (2, vec![7, 7])]),
            (3, vec![(3, vec![5, 5]), (3, vec![6, 6]), (3, vec![7, 7])]),
            (4, vec![(4, vec![1, 10]), (4, vec![6, 6]), (4, vec![7, 7])]),
            (5, vec![(5, vec![7, 7])]),
        ];

        let time_interval = TimeInterval::new(TS::new(0), TS::INFINITY);
        for rhs in permuts.clone() {
            for lhs in permuts.clone() {
                test_negative_temporal_property(false,data_lhs.clone(), data_rhs.clone(), lhs_attrs.clone(), rhs_attrs.clone(), times.clone(), expected.clone(), rhs.clone(), lhs.clone(), time_interval);
            }
        }
    }

    // -----------------------------------------------
    #[test]
    fn reorder_once_unary() {
        // betas
        let data_rhs = vec![
            vec![StreamData::D(0, vec![1, 1]), StreamData::D(0, vec![3, 3])],
            vec![StreamData::M(1, true)],
            vec![StreamData::D(2, vec![2, 2])],
            vec![StreamData::D(3, vec![5, 5])],
            vec![StreamData::M(4, true)],
            vec![StreamData::D(5, vec![6, 6])],
        ];

        let expected = vec![
            (0, vec![(0, vec![1, 1]), (0, vec![3, 3])]),
            (1, vec![(1, vec![1, 1]), (1, vec![3, 3])]),
            (2, vec![(2, vec![1, 1]), (2, vec![2, 2]), (2, vec![3, 3])]),
            (
                3,
                vec![
                    (3, vec![1, 1]),
                    (3, vec![2, 2]),
                    (3, vec![3, 3]),
                    (3, vec![5, 5]),
                ],
            ),
            (
                4,
                vec![
                    (4, vec![1, 1]),
                    (4, vec![2, 2]),
                    (4, vec![3, 3]),
                    (4, vec![5, 5]),
                ],
            ),
            (
                5,
                vec![
                    (5, vec![1, 1]),
                    (5, vec![2, 2]),
                    (5, vec![3, 3]),
                    (5, vec![5, 5]),
                    (5, vec![6, 6]),
                ],
            ),
        ];

        let times = vec![(0,0), (1,1), (2,2), (3,3), (4,4), (5,5)];
        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        let time_interval = TimeInterval::new(TS::new(0), TS::new(7));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_unary_once_eventually(
                0,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_unary_once_eventually(
                0,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }

        let expected = vec![
            (1, vec![(1, vec![1, 1]), (1, vec![3, 3])]),
            (2, vec![(2, vec![1, 1]), (2, vec![3, 3])]),
            (3, vec![(3, vec![1, 1]), (3, vec![2, 2]), (3, vec![3, 3])]),
            (
                4,
                vec![
                    (4, vec![1, 1]),
                    (4, vec![2, 2]),
                    (4, vec![3, 3]),
                    (4, vec![5, 5]),
                ],
            ),
            (
                5,
                vec![
                    (5, vec![1, 1]),
                    (5, vec![2, 2]),
                    (5, vec![3, 3]),
                    (5, vec![5, 5]),
                ],
            ),
        ];

        let time_interval = TimeInterval::new(TS::new(1), TS::new(7));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_unary_once_eventually(
                0,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_unary_once_eventually(
                0,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }

        let expected = vec![
            (4, vec![(4, vec![1, 1]), (4, vec![3, 3])]),
            (5, vec![(5, vec![1, 1]), (5, vec![3, 3])]),
        ];

        let time_interval = TimeInterval::new(TS::new(4), TS::new(7));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_unary_once_eventually(
                0,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_unary_once_eventually(
                0,
                data_rhs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }
    }

    #[test]
    fn reorder_once_since() {
        //alphas
        let data_lhs = vec![
            vec![StreamData::D(0, vec![])],
            vec![StreamData::D(1, vec![])],
            vec![StreamData::D(2, vec![])],
            vec![StreamData::D(3, vec![])],
            vec![StreamData::D(4, vec![])],
            vec![StreamData::D(5, vec![])],
        ];

        // betas
        let data_rhs = vec![
            vec![StreamData::D(0, vec![1, 1]), StreamData::D(0, vec![3, 3])],
            vec![StreamData::M(1, true)],
            vec![StreamData::D(2, vec![2, 2])],
            vec![StreamData::D(3, vec![5, 5])],
            vec![StreamData::M(4, true)],
            vec![StreamData::D(5, vec![6, 6])],
        ];

        let expected = vec![
            (0, vec![(0, vec![1, 1]), (0, vec![3, 3])]),
            (1, vec![(1, vec![1, 1]), (1, vec![3, 3])]),
            (2, vec![(2, vec![1, 1]), (2, vec![2, 2]), (2, vec![3, 3])]),
            (
                3,
                vec![
                    (3, vec![1, 1]),
                    (3, vec![2, 2]),
                    (3, vec![3, 3]),
                    (3, vec![5, 5]),
                ],
            ),
            (
                4,
                vec![
                    (4, vec![1, 1]),
                    (4, vec![2, 2]),
                    (4, vec![3, 3]),
                    (4, vec![5, 5]),
                ],
            ),
            (
                5,
                vec![
                    (5, vec![1, 1]),
                    (5, vec![2, 2]),
                    (5, vec![3, 3]),
                    (5, vec![5, 5]),
                    (5, vec![6, 6]),
                ],
            ),
        ];

        let times = vec![(0,0), (1,1), (2,2), (3,3), (4,4), (5,5)];

        let lhs_attrs: Vec<String> = vec![];
        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        let time_interval = TimeInterval::new(TS::new(0), TS::new(7));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_temporal_reordered(
                0,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_temporal_reordered(
                0,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }

        let expected = vec![
            (1, vec![(1, vec![1, 1]), (1, vec![3, 3])]),
            (2, vec![(2, vec![1, 1]), (2, vec![3, 3])]),
            (3, vec![(3, vec![1, 1]), (3, vec![2, 2]), (3, vec![3, 3])]),
            (
                4,
                vec![
                    (4, vec![1, 1]),
                    (4, vec![2, 2]),
                    (4, vec![3, 3]),
                    (4, vec![5, 5]),
                ],
            ),
            (
                5,
                vec![
                    (5, vec![1, 1]),
                    (5, vec![2, 2]),
                    (5, vec![3, 3]),
                    (5, vec![5, 5]),
                ],
            ),
        ];

        let time_interval = TimeInterval::new(TS::new(1), TS::new(7));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_temporal_reordered(
                0,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );

            test_temporal_reordered(
                0,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }

        let expected = vec![
            (4, vec![(4, vec![1, 1]), (4, vec![3, 3])]),
            (5, vec![(5, vec![1, 1]), (5, vec![3, 3])]),
        ];

        let time_interval = TimeInterval::new(TS::new(4), TS::new(7));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_temporal_reordered(
                0,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_temporal_reordered(
                0,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }
    }

    #[test]
    fn mult_worker_since() {
        //alphas
        let data_lhs = vec![
            vec![
                StreamData::D(0, vec![1]),
                StreamData::D(0, vec![10]),
                StreamData::D(0, vec![100]),
                StreamData::D(0, vec![1000]),
            ],
            vec![
                StreamData::D(1, vec![1]),
                StreamData::D(1, vec![10]),
                StreamData::D(1, vec![100]),
                StreamData::D(1, vec![1000]),
            ],
            vec![
                StreamData::D(2, vec![1]),
                StreamData::D(2, vec![10]),
                StreamData::D(2, vec![100]),
                StreamData::D(2, vec![1000]),
            ],
            vec![
                StreamData::D(3, vec![1]),
                StreamData::D(3, vec![10]),
                StreamData::D(3, vec![100]),
                StreamData::D(3, vec![1000]),
            ],
            vec![
                StreamData::D(4, vec![1]),
                StreamData::D(4, vec![10]),
                StreamData::D(4, vec![100]),
                StreamData::D(4, vec![1000]),
            ],
            vec![
                StreamData::D(5, vec![1]),
                StreamData::D(5, vec![10]),
                StreamData::D(5, vec![100]),
                StreamData::D(5, vec![1000]),
            ],
        ];

        // betas
        let data_rhs = vec![
            vec![StreamData::D(0, vec![1, 1]), StreamData::D(0, vec![10, 10])],
            vec![StreamData::M(1, true)],
            vec![StreamData::D(2, vec![100, 100])],
            vec![StreamData::D(3, vec![1000, 1000])],
            vec![StreamData::D(4, vec![666, 666])],
            vec![StreamData::D(5, vec![777, 777])],
        ];

        let expected = vec![
            (0, vec![(0, vec![1, 1]), (0, vec![10, 10])]),
            (1, vec![(1, vec![1, 1]), (1, vec![10, 10])]),
            (
                2,
                vec![(2, vec![1, 1]), (2, vec![10, 10]), (2, vec![100, 100])],
            ),
            (
                3,
                vec![
                    (3, vec![1, 1]),
                    (3, vec![10, 10]),
                    (3, vec![100, 100]),
                    (3, vec![1000, 1000]),
                ],
            ),
            (
                4,
                vec![
                    (4, vec![1, 1]),
                    (4, vec![10, 10]),
                    (4, vec![100, 100]),
                    (4, vec![666, 666]),
                    (4, vec![1000, 1000]),
                ],
            ),
            (
                5,
                vec![
                    (5, vec![1, 1]),
                    (5, vec![10, 10]),
                    (5, vec![100, 100]),
                    (5, vec![777, 777]),
                    (5, vec![1000, 1000]),
                ],
            ),
        ];

        let times = vec![(0,0), (1,1), (2,2), (3,3), (4,4), (5,5)];

        let lhs_attrs: Vec<String> = vec!["x".into()];
        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        let time_interval = TimeInterval::new(TS::new(0), TS::INFINITY);
        for _x in 0..NUMBER_OF_ROUNDS {
            test_temporal_reordered(
                0,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_temporal_reordered(
                0,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }
    }

    #[test]
    fn reorder_since() {
        /*
        alpha: P(x:int)
        beta:  Q(x:int, y:int)

        P(a) SINCE[0, *] Q(a,b)
        @0 (time point 0): (1,10) (3,10) (4,10)
        @1 (time point 1): (1,10) (3,10)
        @2 (time point 2): (1,10) (2,100) (3,10)
        @3 (time point 3): (1,10) (2,100) (5,5)
        @4 (time point 4): (1,10) (2,100) (6,6)
        @5 (time point 5): (1,10) (7,7)

        P(a) SINCE[1, *] Q(a,b)

        @1 (time point 1): (1,10) (3,10)
        @2 (time point 2): (1,10) (3,10)
        @3 (time point 3): (1,10) (2,100)
        @4 (time point 4): (1,10) (2,100)
        @5 (time point 5): (1,10)


        P(a) SINCE[2, 3] Q(a,b)
        @2 (time point 2): (1,10) (3,10)
        @3 (time point 3): (1,10)
        @4 (time point 4): (2,100)
         */

        //alphas
        let data_lhs = vec![
            vec![StreamData::M(0, true)],
            vec![StreamData::D(1, vec![1]), StreamData::D(1, vec![3])],
            vec![
                StreamData::D(2, vec![1]),
                StreamData::D(2, vec![2]),
                StreamData::D(2, vec![3]),
            ],
            vec![StreamData::D(3, vec![1]), StreamData::D(3, vec![2])],
            vec![StreamData::D(4, vec![1]), StreamData::D(4, vec![2])],
            vec![StreamData::D(5, vec![1])],
        ];

        // betas
        let data_rhs = vec![
            vec![
                StreamData::D(0, vec![1, 10]),
                StreamData::D(0, vec![3, 10]),
                StreamData::D(0, vec![4, 10]),
            ],
            vec![StreamData::M(1, true)],
            vec![StreamData::D(2, vec![2, 100])],
            vec![StreamData::D(3, vec![5, 5])],
            vec![StreamData::D(4, vec![6, 6])],
            vec![StreamData::D(5, vec![7, 7])],
        ];

        let expected = vec![
            (
                0,
                vec![(0, vec![1, 10]), (0, vec![3, 10]), (0, vec![4, 10])],
            ),
            (1, vec![(1, vec![1, 10]), (1, vec![3, 10])]),
            (
                2,
                vec![(2, vec![1, 10]), (2, vec![2, 100]), (2, vec![3, 10])],
            ),
            (
                3,
                vec![(3, vec![1, 10]), (3, vec![2, 100]), (3, vec![5, 5])],
            ),
            (
                4,
                vec![(4, vec![1, 10]), (4, vec![2, 100]), (4, vec![6, 6])],
            ),
            (5, vec![(5, vec![1, 10]), (5, vec![7, 7])]),
        ];

        let times = vec![(0,0), (1,1), (2,2), (3,3), (4,4), (5,5)];

        let lhs_attrs: Vec<String> = vec!["x".into()];
        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        let time_interval = TimeInterval::new(TS::new(0), TS::INFINITY);
        for _x in 0..NUMBER_OF_ROUNDS {
            test_temporal_reordered(
                0,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_temporal_reordered(
                0,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }

        let expected = vec![
            (1, vec![(1, vec![1, 10]), (1, vec![3, 10])]),
            (2, vec![(2, vec![1, 10]), (2, vec![3, 10])]),
            (3, vec![(3, vec![1, 10]), (3, vec![2, 100])]),
            (4, vec![(4, vec![1, 10]), (4, vec![2, 100])]),
            (5, vec![(5, vec![1, 10])]),
        ];

        let time_interval = TimeInterval::new(TS::new(1), TS::INFINITY);
        for _x in 0..NUMBER_OF_ROUNDS {
            test_temporal_reordered(
                0,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );

            test_temporal_reordered(
                0,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }

        let expected = vec![
            (2, vec![(2, vec![1, 10]), (2, vec![3, 10])]),
            (3, vec![(3, vec![1, 10])]),
            (4, vec![(4, vec![2, 100])]),
        ];

        let time_interval = TimeInterval::new(TS::new(2), TS::new(3));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_temporal_reordered(
                0,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_temporal_reordered(
                0,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }
    }

    #[test]
    fn reorder_since_1() {
        /*
        alpha: P(x:int)
        beta:  Q(x:int, y:int)

        P(a) SINCE[1, 2] Q(a,b)
        @1 (time point 1): (1,10) (3,10)
        @2 (time point 2): (1,10) (3,10)
        @3 (time point 3): (2,100)
        @4 (time point 4): (2,100)


        P(a) SINCE[2, 4] Q(a,b)
        @2 (time point 2): (1,10) (3,10)
        @3 (time point 3): (1,10)
        @4 (time point 4): (1,10) (2,100)

        P(a) SINCE[6, 7] Q(a,b)
         */

        //alphas
        let data_lhs = vec![
            vec![StreamData::M(0, true)],
            vec![StreamData::D(1, vec![1]), StreamData::D(1, vec![3])],
            vec![
                StreamData::D(2, vec![1]),
                StreamData::D(2, vec![2]),
                StreamData::D(2, vec![3]),
            ],
            vec![StreamData::D(3, vec![1]), StreamData::D(3, vec![2])],
            vec![StreamData::D(4, vec![1]), StreamData::D(4, vec![2])],
            vec![StreamData::D(5, vec![1])],
        ];

        // betas
        let data_rhs = vec![
            vec![
                StreamData::D(0, vec![1, 10]),
                StreamData::D(0, vec![3, 10]),
                StreamData::D(0, vec![4, 10]),
            ],
            vec![StreamData::M(1, true)],
            vec![StreamData::D(2, vec![2, 100])],
            vec![StreamData::D(3, vec![5, 5])],
            vec![StreamData::D(4, vec![6, 6])],
            vec![StreamData::D(5, vec![7, 7])],
        ];

        let times = vec![(0,0), (1,1), (2,2), (3,3), (4,4), (5,5)];

        let lhs_attrs: Vec<String> = vec!["x".into()];
        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        let time_interval = TimeInterval::new(TS::new(1), TS::new(2));

        // P(a) SINCE[1, 2] Q(a,b)
        let expected = vec![
            (1, vec![(1, vec![1, 10]), (1, vec![3, 10])]),
            (2, vec![(2, vec![1, 10]), (2, vec![3, 10])]),
            (3, vec![(3, vec![2, 100])]),
            (4, vec![(4, vec![2, 100])]),
        ];

        for _x in 0..NUMBER_OF_ROUNDS {
            test_temporal_reordered(
                0,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
            test_temporal_reordered(
                0,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );
        }

        // P(a) SINCE[2, 4] Q(a,b)
        let expected = vec![
            (2, vec![(2, vec![1, 10]), (2, vec![3, 10])]),
            (3, vec![(3, vec![1, 10])]),
            (4, vec![(4, vec![1, 10]), (4, vec![2, 100])]),
        ];

        let time_interval = TimeInterval::new(TS::new(2), TS::new(4));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_temporal_reordered(
                0,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
            test_temporal_reordered(
                0,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );
        }

        // P(a) SINCE[6, 7] Q(a,b)
        let expected = vec![];

        let time_interval = TimeInterval::new(TS::new(6), TS::new(7));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_temporal_reordered(
                0,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
            test_temporal_reordered(
                0,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );
        }
    }

    #[test]
    fn reorder_since_2() {
        /*
        alpha: P(x:int)
        beta:  Q(x:int, y:int)

        P(a) SINCE[0, 1] Q(a,b)
        @0 (time point 0): (1,10) (3,10) (4,10)
        @1 (time point 1): (1,10) (3,10)
        @2 (time point 2): (2,100)
        @3 (time point 3): (2,100) (5,5)
        @4 (time point 4): (6,6)
        @5 (time point 5): (7,7)


        P(a) SINCE[4, 4] Q(a,b)
        @4 (time point 4): (1,10)


        P(a) SINCE[0, 0] Q(a,b)
        @0 (time point 0): (1,10) (3,10) (4,10)
        @2 (time point 2): (2,100)
        @3 (time point 3): (5,5)
        @4 (time point 4): (6,6)
        @5 (time point 5): (7,7)
         */

        //alphas
        let data_lhs = vec![
            vec![StreamData::M(0, true)],
            vec![StreamData::D(1, vec![1]), StreamData::D(1, vec![3])],
            vec![
                StreamData::D(2, vec![1]),
                StreamData::D(2, vec![2]),
                StreamData::D(2, vec![3]),
            ],
            vec![StreamData::D(3, vec![1]), StreamData::D(3, vec![2])],
            vec![StreamData::D(4, vec![1]), StreamData::D(4, vec![2])],
            vec![StreamData::D(5, vec![1])],
        ];

        // betas
        let data_rhs = vec![
            vec![
                StreamData::D(0, vec![1, 10]),
                StreamData::D(0, vec![3, 10]),
                StreamData::D(0, vec![4, 10]),
            ],
            vec![StreamData::M(1, true)],
            vec![StreamData::D(2, vec![2, 100])],
            vec![StreamData::D(3, vec![5, 5])],
            vec![StreamData::D(4, vec![6, 6])],
            vec![StreamData::D(5, vec![7, 7])],
        ];

        let times = vec![(0,0), (1,1), (2,2), (3,3), (4,4), (5,5)];

        let lhs_attrs: Vec<String> = vec!["x".into()];
        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        // P(a) SINCE[0, 1] Q(a,b)
        let expected = vec![
            (
                0,
                vec![(0, vec![1, 10]), (0, vec![3, 10]), (0, vec![4, 10])],
            ),
            (1, vec![(1, vec![1, 10]), (1, vec![3, 10])]),
            (2, vec![(2, vec![2, 100])]),
            (3, vec![(3, vec![2, 100]), (3, vec![5, 5])]),
            (4, vec![(4, vec![6, 6])]),
            (5, vec![(5, vec![7, 7])]),
        ];

        let time_interval = TimeInterval::new(TS::new(0), TS::new(1));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_temporal_reordered(
                0,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_temporal_reordered(
                0,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }

        // P(a) SINCE[4, 4] Q(a,b)
        let expected = vec![(4, vec![(4, vec![1, 10])])];

        let time_interval = TimeInterval::new(TS::new(4), TS::new(4));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_temporal_reordered(
                0,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_temporal_reordered(
                0,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }
        // P(a) SINCE[0, 0] Q(a,b)
        let expected = vec![
            (
                0,
                vec![(0, vec![1, 10]), (0, vec![3, 10]), (0, vec![4, 10])],
            ),
            (2, vec![(2, vec![2, 100])]),
            (3, vec![(3, vec![5, 5])]),
            (4, vec![(4, vec![6, 6])]),
            (5, vec![(5, vec![7, 7])]),
        ];

        let time_interval = TimeInterval::new(TS::new(0), TS::new(0));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_temporal_reordered(
                0,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_temporal_reordered(
                0,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }
    }

    #[test]
    fn reorder_since_3() {
        /*
        @0
        P(1)
        P(2)
        @1
        P(1)
        @2
        Q(1,2)
        @3
        P(1)
        @4
        P(1)
        @5
        P(1)
        @6
        P(1)
        Q(2,2)
        @7
        P(1)
        P(2)

        P(a) SINCE[0, 3] Q(a,b)

        @2 (time point 2): (1,2)
        @3 (time point 3): (1,2)
        @4 (time point 4): (1,2)
        @5 (time point 5): (1,2)
        @6 (time point 6): (2,2)
        @7 (time point 7): (2,2)
         */

        //alphas
        let data_lhs = vec![
            vec![StreamData::D(0, vec![1]), StreamData::D(0, vec![2])],
            vec![StreamData::D(1, vec![1])],
            vec![StreamData::M(2, true)],
            vec![StreamData::D(3, vec![1])],
            vec![StreamData::D(4, vec![1])],
            vec![StreamData::D(5, vec![1])],
            vec![StreamData::D(6, vec![1])],
            vec![StreamData::D(7, vec![1]), StreamData::D(7, vec![2])],
        ];

        // betas
        let data_rhs = vec![
            vec![StreamData::M(0, true)],
            vec![StreamData::M(1, true)],
            vec![StreamData::D(2, vec![1, 2])],
            vec![StreamData::M(3, true)],
            vec![StreamData::M(4, true)],
            vec![StreamData::M(5, true)],
            vec![StreamData::D(6, vec![2, 2])],
            vec![StreamData::M(7, true)],
        ];

        let times = vec![(0,0), (1,1), (2,2), (3,3), (4,4), (5,5), (6,6), (7,7)];

        let lhs_attrs: Vec<String> = vec!["x".into()];
        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        // P(a) SINCE[0, 3] Q(a,b)
        let expected = vec![
            (2, vec![(2, vec![1, 2])]),
            (3, vec![(3, vec![1, 2])]),
            (4, vec![(4, vec![1, 2])]),
            (5, vec![(5, vec![1, 2])]),
            (6, vec![(6, vec![2, 2])]),
            (7, vec![(7, vec![2, 2])]),
        ];

        let time_interval = TimeInterval::new(TS::new(0), TS::new(3));

        for _x in 0..NUMBER_OF_ROUNDS {
            test_temporal_reordered(
                0,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_temporal_reordered(
                0,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }
    }

    #[test]
    fn reorder_since_4() {
        /*
        @0
        P(1)
        P(2)

        @1
        Q(1,10)

        @2
        P(1)
        Q(2, 10)

        @3
        P(2)


        P(a) SINCE[0, *] Q(a,b)
        @1 (time point 1): (1,10)
        @2 (time point 2): (1,10) (2,10)
        @3 (time point 3): (2,10)
         */

        //alphas
        let data_lhs = vec![
            vec![StreamData::D(0, vec![1]), StreamData::D(0, vec![2])],
            vec![StreamData::M(1, true)],
            vec![StreamData::D(2, vec![1])],
            vec![StreamData::D(3, vec![2])],
        ];

        // betas
        let data_rhs = vec![
            vec![StreamData::M(0, true)],
            vec![StreamData::D(1, vec![1, 10])],
            vec![StreamData::D(2, vec![2, 10])],
        ];

        let times = vec![(0,0), (1,1), (2,2), (3,3)];

        let lhs_attrs: Vec<String> = vec!["x".into()];
        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        let expected = vec![
            (1, vec![(1, vec![1, 10])]),
            (2, vec![(2, vec![1, 10]), (2, vec![2, 10])]),
            (3, vec![(3, vec![2, 10])]),
        ];

        let time_interval = TimeInterval::new(TS::new(0), TS::INFINITY);
        for _x in 0..NUMBER_OF_ROUNDS {
            test_temporal_reordered(
                0,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_temporal_reordered(
                0,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }
    }

    #[test]
    fn since_test_1() {
        /*
        @0 (time point 0): (1,1) (1,100) (2,100) (3,100)
        @1 (time point 1): (1,1) (1,100) (2,100)
        @2 (time point 2): (1,1) (1,100)
        @3 (time point 3): (1,1) (1,100)
        @4 (time point 4): (1,1) (1,100)
        @5 (time point 5): (1,1) (1,100)
         */

        let expected = vec![
            (
                0,
                vec![
                    (0, vec![1, 1]),
                    (0, vec![1, 100]),
                    (0, vec![2, 100]),
                    (0, vec![3, 100]),
                ],
            ),
            (
                1,
                vec![(1, vec![1, 1]), (1, vec![1, 100]), (1, vec![2, 100])],
            ),
            (2, vec![(2, vec![1, 1]), (2, vec![1, 100])]),
            (3, vec![(3, vec![1, 1]), (3, vec![1, 100])]),
            (4, vec![(4, vec![1, 1]), (4, vec![1, 100])]),
            (5, vec![(5, vec![1, 1]), (5, vec![1, 100])]),
        ];

        let data_lhs = vec![
            vec![StreamData::M(0, true)],
            vec![StreamData::D(1, vec![1]), StreamData::D(1, vec![2])],
            vec![StreamData::D(2, vec![1])],
            vec![StreamData::D(3, vec![1])],
            vec![StreamData::D(4, vec![1])],
            vec![StreamData::D(5, vec![1])],
        ];

        // betas
        let data_rhs = vec![
            vec![
                StreamData::D(0, vec![1, 1]),
                StreamData::D(0, vec![1, 100]),
                StreamData::D(0, vec![2, 100]),
                StreamData::D(0, vec![3, 100]),
            ],
            vec![StreamData::M(1, true)],
            vec![StreamData::M(2, true)],
            vec![StreamData::M(3, true)],
            vec![StreamData::M(4, true)],
            vec![StreamData::M(5, true)],
        ];

        let times = vec![(0,0), (1,1), (2,2), (3,3), (4,4), (5,5)];

        let lhs_attrs: Vec<String> = vec!["x".into()];
        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        let time_interval = TimeInterval::new(TS::new(0), TS::INFINITY);
        for _x in 0..NUMBER_OF_ROUNDS {
            test_temporal_reordered(
                0,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_temporal_reordered(
                0,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }
    }

    #[test]
    fn since_test_2() {
        /*
        @1 (time point 1): (1,1) (1,100) (2,100)
        @2 (time point 2): (1,1) (1,100)
        @3 (time point 3): (1,1) (1,100)
         */

        let expected = vec![
            (1, vec![(1, vec![1, 1]), (1, vec![1, 100]), (1, vec![2, 100])], ),
            (2, vec![(2, vec![1, 1]), (2, vec![1, 100])]),
            (3, vec![(3, vec![1, 1]), (3, vec![1, 100])]),
        ];

        let data_lhs = vec![
            vec![StreamData::M(0, true)],
            vec![StreamData::D(1, vec![1]), StreamData::D(1, vec![2])],
            vec![StreamData::D(2, vec![1])],
            vec![StreamData::D(3, vec![1])],
            vec![StreamData::D(4, vec![1])],
            vec![StreamData::D(5, vec![1])],
        ];

        // betas
        let data_rhs = vec![
            vec![
                StreamData::D(0, vec![1, 1]),
                StreamData::D(0, vec![1, 100]),
                StreamData::D(0, vec![2, 100]),
                StreamData::D(0, vec![3, 100]),
            ],
            vec![StreamData::M(1, true)],
            vec![StreamData::M(2, true)],
            vec![StreamData::M(3, true)],
            vec![StreamData::M(4, true)],
            vec![StreamData::M(5, true)],
        ];

        let times = vec![(0,0), (1,1), (2,2), (3,3), (4,4), (5,5)];

        let lhs_attrs: Vec<String> = vec!["x".into()];
        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        let time_interval = TimeInterval::new(TS::new(1), TS::new(3));
        for _x in 0..NUMBER_OF_ROUNDS {
            test_temporal_reordered(
                0,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_temporal_reordered(
                0,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }
    }

    #[test]
    fn test_until_1() {
        //alphas
        let data_lhs = vec![
            vec![StreamData::D(0, vec![1])],
            vec![StreamData::D(1, vec![1])],
            vec![StreamData::D(2, vec![1])],
            vec![StreamData::D(3, vec![1])],
            vec![StreamData::D(4, vec![1])],
        ];

        // betas
        let data_rhs = vec![
            vec![StreamData::D(0, vec![])],
            vec![StreamData::D(1, vec![])],
            vec![StreamData::D(2, vec![])],
            vec![StreamData::D(3, vec![])],
            vec![StreamData::D(4, vec![])],
            vec![StreamData::D(5, vec![1, 10])],
        ];

        let expected = vec![
            (0, vec![(0, vec![1, 10])]),
            (1, vec![(1, vec![1, 10])]),
            (2, vec![(2, vec![1, 10])]),
        ];

        let times = vec![(0,0), (1,1), (2,2), (3,3), (4,4), (5,5)];

        let lhs_attrs: Vec<String> = vec!["x".into()];
        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        let time_interval = TimeInterval::new(TS::new(3), TS::new(10));

        for _i in 0..NUMBER_OF_ROUNDS {
            test_temporal_reordered(
                1,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                old_order,
                time_interval.clone(),
            );

            test_temporal_reordered(
                1,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }
    }

    #[test]
    fn test_until_2() {
        /*
        Data set: {
        @0
        P(1)

        @1
        P(1)

        @2
        P(1)

        @3
        P(1)
        Q(1,20)

        @4
        P(1)

        @5
        Q(1,10)
    }

        alpha: P(x:int)
        beta:  Q(x:int, y:int)

        P(a) UNTIL[3, 10] Q(a,b)

        @0 (time point 0): (1,10) (1,20)
        @1 (time point 1): (1,10)
        @2 (time point 2): (1,10)
         */

        //alphas
        let data_lhs = vec![
            vec![StreamData::D(0, vec![1])],
            vec![StreamData::D(1, vec![1])],
            vec![StreamData::D(2, vec![1])],
            vec![StreamData::D(3, vec![1])],
            vec![StreamData::D(4, vec![1])],
        ];

        // betas
        let data_rhs = vec![
            vec![StreamData::D(0, vec![])],
            vec![StreamData::D(1, vec![])],
            vec![StreamData::D(2, vec![])],
            vec![StreamData::D(3, vec![1, 20])],
            vec![StreamData::D(4, vec![])],
            vec![StreamData::D(5, vec![1, 10])],
        ];

        let expected = vec![
            (0, vec![(0, vec![1, 10]), (0, vec![1, 20])]),
            (1, vec![(1, vec![1, 10])]),
            (2, vec![(2, vec![1, 10])]),
        ];

        let times = vec![(0,0), (1,1), (2,2), (3,3), (4,4), (5,5)];

        let lhs_attrs: Vec<String> = vec!["x".into()];
        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        //let time_interval = TimeInterval::new(TS::new(0), TS::new(0));
        let time_interval = TimeInterval::new(TS::new(3), TS::new(8));

        for _i in 0..NUMBER_OF_ROUNDS {
            test_temporal_reordered(
                1,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );

            test_temporal_reordered(
                1,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }
    }

    #[test]
    fn test_until_3() {
        let expected = vec![
            (0, vec![(0, vec![1, 20])]),
            (1, vec![(1, vec![1, 10]), (1, vec![1, 20])]),
            (2, vec![(2, vec![1, 10]), (2, vec![1, 20])]),
            (3, vec![(3, vec![1, 10]), (3, vec![1, 20])]),
            (4, vec![(4, vec![1, 10])]),
            (5, vec![(5, vec![1, 10])]),
        ];

        //alphas
        let data_lhs = vec![
            vec![StreamData::D(0, vec![1])],
            vec![StreamData::D(1, vec![1])],
            vec![StreamData::D(2, vec![1])],
            vec![StreamData::D(3, vec![1])],
            vec![StreamData::D(4, vec![1])],
            vec![StreamData::M(5, true)],
        ];

        // betas
        let data_rhs = vec![
            vec![StreamData::M(0, true)],
            vec![StreamData::M(1, true)],
            vec![StreamData::M(2, true)],
            vec![StreamData::D(3, vec![1, 20])],
            vec![StreamData::M(4, true)],
            vec![StreamData::D(5, vec![1, 10])]
        ];

        let times = vec![(0,0), (1,1), (2,2), (3,3), (4,4), (5,5)];

        let lhs_attrs: Vec<String> = vec!["x".into()];
        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];
        let time_interval = TimeInterval::new(TS::new(0), TS::new(4));

        for _i in 0..NUMBER_OF_ROUNDS {
            test_temporal_reordered(
                1,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_shuffle,
                time_interval.clone(),
            );
            test_temporal_reordered(
                1,
                data_lhs.clone(),
                data_rhs.clone(),
                lhs_attrs.clone(),
                rhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                new_order_reverse,
                time_interval.clone(),
            );
        }
    }

    #[test]
    fn mem_since_test() {
        // tp == ts
        let _tp_to_ts: HashMap<usize, usize> =
            HashMap::from([(0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)]);
        // 2 [0,1]; 0 + 0 <= 1 left holds; 1 <= 0 + 1 right holds
        assert!(mem_since(
            0,
            1,
            TimeInterval::new(TS::FINITE(0), TS::FINITE(1))
        ));
        // 2 [0,1] -> [1, 2]; 0 + 0 <= 2 left holds; 2 <= 0 + 1 is violated
        assert!(!mem_since(
            0,
            2,
            TimeInterval::new(TS::FINITE(0), TS::FINITE(1))
        ));

        // 4 [1,2]; 0 + 1 <= 4 left holds;  4 <= 0 + 2 right holds
        assert!(!mem_since(
            0,
            4,
            TimeInterval::new(TS::FINITE(1), TS::FINITE(2))
        ));
        // 4 [1,2]; 4 + 1 <= 4 doesn't left bound; 4 <= 4 + 2 right holds
        assert!(!mem_since(
            4,
            4,
            TimeInterval::new(TS::FINITE(1), TS::FINITE(2))
        ));
        // 4 [1,2]; 3 + 1 <= 4 doesn't left bound; 4 <= 3 + 2 right holds
        assert!(mem_since(
            3,
            4,
            TimeInterval::new(TS::FINITE(1), TS::FINITE(2))
        ));

        // 4 [0,3]; 2 + 0 <= 4 left holds; 4 <= 2 + 3 right holds
        assert!(mem_since(
            2,
            4,
            TimeInterval::new(TS::FINITE(0), TS::FINITE(3))
        ));
        // 4 [0,3]; 1 + 0 <= 4 left holds; 4 <= 1 + 3 right holds
        assert!(mem_since(
            1,
            4,
            TimeInterval::new(TS::FINITE(0), TS::FINITE(3))
        ));
    }


    #[test]
    fn mem_since_0_diff_interval() {
        let _tp_to_ts : HashMap<usize, usize> = HashMap::from([(0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)]);
        assert!(mem_since(0, 0, TimeInterval::new(TS::FINITE(0), TS::FINITE(0))));
        assert!(!mem_since(0, 1, TimeInterval::new(TS::FINITE(0), TS::FINITE(0))));
        assert!(mem_since(1, 1, TimeInterval::new(TS::FINITE(0), TS::FINITE(0))));
        assert!(!mem_since(2, 3, TimeInterval::new(TS::FINITE(0), TS::FINITE(0))));
        assert!(!mem_since(4, 5, TimeInterval::new(TS::FINITE(0), TS::FINITE(0))));
        assert!(mem_since(5, 5, TimeInterval::new(TS::FINITE(0), TS::FINITE(0))));


        assert!(mem_since(1, 2, TimeInterval::new(TS::FINITE(1), TS::FINITE(1))));
        assert!(!mem_since(2, 2, TimeInterval::new(TS::FINITE(1), TS::FINITE(1))));
        assert!(!mem_since(1, 3, TimeInterval::new(TS::FINITE(1), TS::FINITE(1))));
        assert!(mem_since(5, 6, TimeInterval::new(TS::FINITE(1), TS::FINITE(1))));
        assert!(!mem_since(6, 6, TimeInterval::new(TS::FINITE(1), TS::FINITE(1))));
    }

    #[test]
    fn mem_until_inf_interval() {
        let _tp_to_ts : HashMap<usize, usize> = HashMap::from([(0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)]);
        assert!(mem_until(0, 0, TimeInterval::new(TS::FINITE(0), TS::INFINITY)));
        assert!(mem_until(1, 0, TimeInterval::new(TS::FINITE(0), TS::INFINITY)));
        assert!(mem_until(1, 1, TimeInterval::new(TS::FINITE(0), TS::INFINITY)));
        assert!(mem_until(3, 2, TimeInterval::new(TS::FINITE(0), TS::INFINITY)));
        assert!(mem_until(5, 4, TimeInterval::new(TS::FINITE(0), TS::INFINITY)));
        assert!(mem_until(5, 5, TimeInterval::new(TS::FINITE(0), TS::INFINITY)));


        assert!(!mem_until(1, 2, TimeInterval::new(TS::FINITE(1), TS::INFINITY)));
        assert!(!mem_until(2, 2, TimeInterval::new(TS::FINITE(1), TS::INFINITY)));
        assert!(mem_until(3, 1, TimeInterval::new(TS::FINITE(1), TS::INFINITY)));
        assert!(mem_until(6, 5, TimeInterval::new(TS::FINITE(1), TS::INFINITY)));
        assert!(!mem_until(6, 6, TimeInterval::new(TS::FINITE(1), TS::INFINITY)));
    }

    #[test]
    fn mem_until_0_diff_interval() {
        assert!(mem_until(10, 10, TimeInterval::new(TS::FINITE(0), TS::FINITE(0))));
        assert!(!mem_until(10, 9, TimeInterval::new(TS::FINITE(0), TS::FINITE(0))));
        assert!(mem_until(9, 9, TimeInterval::new(TS::FINITE(0), TS::FINITE(0))));
        assert!(!mem_until(9, 8, TimeInterval::new(TS::FINITE(0), TS::FINITE(0))));
        assert!(mem_until(5, 5, TimeInterval::new(TS::FINITE(0), TS::FINITE(0))));
        assert!(!mem_until(5, 4, TimeInterval::new(TS::FINITE(0), TS::FINITE(0))));

        assert!(!mem_until(10, 10, TimeInterval::new(TS::FINITE(1), TS::FINITE(1))));
        assert!(mem_until(10, 9, TimeInterval::new(TS::FINITE(1), TS::FINITE(1))));
        assert!(!mem_until(9, 9, TimeInterval::new(TS::FINITE(1), TS::FINITE(1))));
        assert!(mem_until(9, 8, TimeInterval::new(TS::FINITE(1), TS::FINITE(1))));
        assert!(!mem_until(4, 4, TimeInterval::new(TS::FINITE(1), TS::FINITE(1))));
        assert!(mem_until(4, 3, TimeInterval::new(TS::FINITE(1), TS::FINITE(1))));
    }

    #[test]
    fn mem_since_test_inf_at_0() {
        let _tp_to_ts : HashMap<usize, usize> = HashMap::from([(0, 2), (1, 4), (2, 6), (3, 8), (4, 10), (5, 12)]);
        assert!(mem_since(2, 2, TimeInterval::new(TS::FINITE(0), TS::INFINITY)));
        assert!(mem_since(2, 4, TimeInterval::new(TS::FINITE(0), TS::INFINITY)));
        assert!(mem_since(2, 6, TimeInterval::new(TS::FINITE(0), TS::INFINITY)));


        assert!(!mem_since(2, 2, TimeInterval::new(TS::FINITE(1), TS::INFINITY)));
        assert!(mem_since(2, 4, TimeInterval::new(TS::FINITE(1), TS::INFINITY)));
        assert!(mem_since(2, 6, TimeInterval::new(TS::FINITE(1), TS::INFINITY)));


        assert!(!mem_since(2, 2, TimeInterval::new(TS::FINITE(2), TS::INFINITY)));
        assert!(mem_since(2, 4, TimeInterval::new(TS::FINITE(2), TS::INFINITY)));
        assert!(mem_since(2, 6, TimeInterval::new(TS::FINITE(2), TS::INFINITY)));
        assert!(mem_since(2, 8, TimeInterval::new(TS::FINITE(2), TS::INFINITY)));


        assert!(!mem_since(2, 2, TimeInterval::new(TS::FINITE(3), TS::INFINITY)));
        assert!(!mem_since(2, 4, TimeInterval::new(TS::FINITE(3), TS::INFINITY)));
        assert!(mem_since(2, 6, TimeInterval::new(TS::FINITE(3), TS::INFINITY)));
        assert!(mem_since(2, 8, TimeInterval::new(TS::FINITE(3), TS::INFINITY)));


        assert!(!mem_since(2, 2, TimeInterval::new(TS::FINITE(4), TS::INFINITY)));
        assert!(!mem_since(2, 4, TimeInterval::new(TS::FINITE(4), TS::INFINITY)));
        assert!(mem_since(2, 6, TimeInterval::new(TS::FINITE(4), TS::INFINITY)));
        assert!(mem_since(2, 8, TimeInterval::new(TS::FINITE(4), TS::INFINITY)));
        assert!(mem_since(2, 10, TimeInterval::new(TS::FINITE(4), TS::INFINITY)));


        assert!(!mem_since(2, 2, TimeInterval::new(TS::FINITE(5), TS::INFINITY)));
        assert!(!mem_since(2, 4, TimeInterval::new(TS::FINITE(5), TS::INFINITY)));
        assert!(!mem_since(2, 6, TimeInterval::new(TS::FINITE(5), TS::INFINITY)));
        assert!(mem_since(2, 8, TimeInterval::new(TS::FINITE(5), TS::INFINITY)));
        assert!(mem_since(2, 10, TimeInterval::new(TS::FINITE(5), TS::INFINITY)));
    }

    #[test]
    fn mem_since_test_inf_at_x() {
        let _tp_to_ts : HashMap<usize, usize> = HashMap::from([(0, 2), (1, 4), (2, 6), (3, 8), (4, 10), (5, 12)]);
        assert!(!mem_since(4, 2, TimeInterval::new(TS::FINITE(0), TS::INFINITY)));
        assert!(mem_since(4, 4, TimeInterval::new(TS::FINITE(0), TS::INFINITY)));
        assert!(mem_since(4, 6, TimeInterval::new(TS::FINITE(0), TS::INFINITY)));


        assert!(!mem_since(4, 2, TimeInterval::new(TS::FINITE(1), TS::INFINITY)));
        assert!(!mem_since(4, 4, TimeInterval::new(TS::FINITE(1), TS::INFINITY)));
        assert!(mem_since(4, 6, TimeInterval::new(TS::FINITE(1), TS::INFINITY)));
        assert!(mem_since(4, 8, TimeInterval::new(TS::FINITE(1), TS::INFINITY)));


        assert!(!mem_since(6, 2, TimeInterval::new(TS::FINITE(2), TS::INFINITY)));
        assert!(!mem_since(6, 4, TimeInterval::new(TS::FINITE(2), TS::INFINITY)));
        assert!(!mem_since(6, 6, TimeInterval::new(TS::FINITE(2), TS::INFINITY)));
        assert!(mem_since(6, 8, TimeInterval::new(TS::FINITE(2), TS::INFINITY)));


        assert!(!mem_since(6, 6, TimeInterval::new(TS::FINITE(3), TS::INFINITY)));
        assert!(!mem_since(6, 8, TimeInterval::new(TS::FINITE(3), TS::INFINITY)));
        assert!(mem_since(6, 10, TimeInterval::new(TS::FINITE(3), TS::INFINITY)));
        assert!(mem_since(6, 12, TimeInterval::new(TS::FINITE(3), TS::INFINITY)));


        assert!(!mem_since(8, 8, TimeInterval::new(TS::FINITE(4), TS::INFINITY)));
        assert!(!mem_since(8, 10, TimeInterval::new(TS::FINITE(4), TS::INFINITY)));
        assert!(mem_since(8, 12, TimeInterval::new(TS::FINITE(4), TS::INFINITY)));
    }

    #[test]
    fn mem_until_test_inf_at_x() {
        let _tp_to_ts : HashMap<usize, usize> = HashMap::from([(0, 2), (1, 4), (2, 6), (3, 8), (4, 10), (5, 12)]);
        assert!(mem_until(4, 2, TimeInterval::new(TS::FINITE(0), TS::INFINITY)));
        assert!(mem_until(4, 4, TimeInterval::new(TS::FINITE(0), TS::INFINITY)));
        assert!(!mem_until(4, 6, TimeInterval::new(TS::FINITE(0), TS::INFINITY)));


        assert!(mem_until(4, 2, TimeInterval::new(TS::FINITE(1), TS::INFINITY)));
        assert!(!mem_until(4, 4, TimeInterval::new(TS::FINITE(1), TS::INFINITY)));
        assert!(mem_until(4, 3, TimeInterval::new(TS::FINITE(1), TS::INFINITY)));
        assert!(mem_until(8, 4, TimeInterval::new(TS::FINITE(1), TS::INFINITY)));


        assert!(mem_until(6, 2, TimeInterval::new(TS::FINITE(2), TS::INFINITY)));
        assert!(mem_until(6, 4, TimeInterval::new(TS::FINITE(2), TS::INFINITY)));
        assert!(!mem_until(6, 6, TimeInterval::new(TS::FINITE(2), TS::INFINITY)));
        assert!(!mem_until(6, 8, TimeInterval::new(TS::FINITE(2), TS::INFINITY)));


        assert!(mem_until(6, 3, TimeInterval::new(TS::FINITE(3), TS::INFINITY)));
        assert!(mem_until(8, 5, TimeInterval::new(TS::FINITE(3), TS::INFINITY)));
        assert!(mem_until(10, 6, TimeInterval::new(TS::FINITE(3), TS::INFINITY)));
        assert!(mem_until(12, 8, TimeInterval::new(TS::FINITE(3), TS::INFINITY)));


        assert!(!mem_until(8, 8, TimeInterval::new(TS::FINITE(4), TS::INFINITY)));
        assert!(!mem_until(8, 10, TimeInterval::new(TS::FINITE(4), TS::INFINITY)));
        assert!(mem_until(12, 8, TimeInterval::new(TS::FINITE(4), TS::INFINITY)));
    }

    #[test]
    fn mem_since_test_eq_ts() {
        let _tp_to_ts : HashMap<usize, usize> = HashMap::from([(0, 2), (1, 4), (2, 4), (3, 8), (4, 10), (5, 10)]);
        assert!(mem_since(2, 4, TimeInterval::new(TS::FINITE(0), TS::INFINITY)));
        assert!(mem_since(2, 4, TimeInterval::new(TS::FINITE(1), TS::INFINITY)));
        assert!(mem_since(2, 4, TimeInterval::new(TS::FINITE(0), TS::INFINITY)));
        assert!(mem_since(2, 4, TimeInterval::new(TS::FINITE(1), TS::INFINITY)));


        assert!(!mem_since(4, 4, TimeInterval::new(TS::FINITE(1), TS::INFINITY)));
        assert!(mem_since(2, 10, TimeInterval::new(TS::FINITE(2), TS::INFINITY)));
        assert!(mem_since(4, 10, TimeInterval::new(TS::FINITE(3), TS::INFINITY)));


        assert!(mem_since(2, 10, TimeInterval::new(TS::FINITE(2), TS::INFINITY)));
        assert!(mem_since(4, 10, TimeInterval::new(TS::FINITE(3), TS::INFINITY)));


        assert!(mem_since(2, 4, TimeInterval::new(TS::FINITE(2), TS::FINITE(3))));
        assert!(mem_since(2, 4, TimeInterval::new(TS::FINITE(2), TS::FINITE(3))));
        assert!(!mem_since(2, 8, TimeInterval::new(TS::FINITE(2), TS::FINITE(3))));


        assert!(mem_since(4, 8, TimeInterval::new(TS::FINITE(3), TS::FINITE(4))));
        assert!(!mem_since(4, 10, TimeInterval::new(TS::FINITE(3), TS::FINITE(4))));
    }

    #[test]
    fn mem_until_test_eq_ts() {
        let _tp_to_ts : HashMap<usize, usize> = HashMap::from([(0, 2), (1, 4), (2, 4), (3, 8), (4, 10), (5, 10)]);
        assert!(mem_until(4, 4, TimeInterval::new(TS::FINITE(0), TS::INFINITY)));
        assert!(mem_until(4, 3, TimeInterval::new(TS::FINITE(1), TS::INFINITY)));
        assert!(mem_until(4, 4, TimeInterval::new(TS::FINITE(0), TS::INFINITY)));
        assert!(mem_until(4, 3, TimeInterval::new(TS::FINITE(1), TS::INFINITY)));


        assert!(mem_until(5, 4, TimeInterval::new(TS::FINITE(1), TS::INFINITY)));
        assert!(!mem_until(11, 10, TimeInterval::new(TS::FINITE(2), TS::INFINITY)));
        assert!(mem_until(12, 10, TimeInterval::new(TS::FINITE(2), TS::INFINITY)));
        assert!(mem_until(13, 10, TimeInterval::new(TS::FINITE(3), TS::INFINITY)));


        assert!(mem_until(12, 10, TimeInterval::new(TS::FINITE(2), TS::INFINITY)));
        assert!(mem_until(4, 1, TimeInterval::new(TS::FINITE(3), TS::INFINITY)));
        assert!(!mem_until(4, 2, TimeInterval::new(TS::FINITE(3), TS::INFINITY)));

        assert!(mem_until(4, 2, TimeInterval::new(TS::FINITE(2), TS::INFINITY)));
        assert!(mem_until(4, 2, TimeInterval::new(TS::FINITE(2), TS::INFINITY)));
        assert!(!mem_until(9, 8, TimeInterval::new(TS::FINITE(2), TS::INFINITY)));

        assert!(mem_until(11, 8, TimeInterval::new(TS::FINITE(3), TS::INFINITY)));
        assert!(!mem_until(10, 10, TimeInterval::new(TS::FINITE(3), TS::INFINITY)));
    }

    #[test]
    fn mem_since_until() {
        // tp == ts
        let _tp_to_ts: HashMap<usize, usize> =
            HashMap::from([(0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)]);
        assert!(mem_until(
            4,
            3,
            TimeInterval::new(TS::FINITE(0), TS::FINITE(1))
        ));
        assert!(mem_until(
            4,
            4,
            TimeInterval::new(TS::FINITE(0), TS::FINITE(1))
        ));

        assert!(!mem_until(
            4,
            4,
            TimeInterval::new(TS::FINITE(1), TS::FINITE(2))
        ));
        assert!(mem_until(
            4,
            3,
            TimeInterval::new(TS::FINITE(1), TS::FINITE(2))
        ));
        assert!(mem_until(
            4,
            2,
            TimeInterval::new(TS::FINITE(1), TS::FINITE(2))
        ));
        assert!(!mem_until(
            4,
            1,
            TimeInterval::new(TS::FINITE(1), TS::FINITE(2))
        ));

        assert!(mem_until(
            4,
            4,
            TimeInterval::new(TS::FINITE(0), TS::FINITE(3))
        ));
        assert!(mem_until(
            4,
            3,
            TimeInterval::new(TS::FINITE(0), TS::FINITE(3))
        ));
        assert!(mem_until(
            4,
            2,
            TimeInterval::new(TS::FINITE(0), TS::FINITE(3))
        ));
        assert!(mem_until(
            4,
            1,
            TimeInterval::new(TS::FINITE(0), TS::FINITE(3))
        ));
        assert!(!mem_until(
            4,
            0,
            TimeInterval::new(TS::FINITE(0), TS::FINITE(3))
        ));
    }

    #[test]
    fn mem_until_inf_test() {
        let _tp_to_ts : HashMap<usize, usize> = HashMap::from([(0, 2), (1, 4), (2, 4), (3, 8), (4, 10), (5, 10)]);
        assert!(mem_until(10, 10, TimeInterval::new(TS::FINITE(0), TS::FINITE(8))));
        assert!(mem_until(10, 10, TimeInterval::new(TS::FINITE(0), TS::FINITE(8))));
        assert!(mem_until(10, 8, TimeInterval::new(TS::FINITE(0), TS::FINITE(8))));
        assert!(mem_until(10, 4, TimeInterval::new(TS::FINITE(0), TS::FINITE(8))));
        assert!(mem_until(10, 4, TimeInterval::new(TS::FINITE(0), TS::FINITE(8))));
        assert!(mem_until(10, 2, TimeInterval::new(TS::FINITE(0), TS::FINITE(8))));

        assert!(!mem_until(10, 10, TimeInterval::new(TS::FINITE(1), TS::FINITE(8))));
        assert!(!mem_until(10, 10, TimeInterval::new(TS::FINITE(1), TS::FINITE(8))));
        assert!(mem_until(10, 8, TimeInterval::new(TS::FINITE(1), TS::FINITE(8))));
        assert!(mem_until(10, 4, TimeInterval::new(TS::FINITE(1), TS::FINITE(8))));
        assert!(mem_until(10, 4, TimeInterval::new(TS::FINITE(1), TS::FINITE(8))));
        assert!(mem_until(10, 2, TimeInterval::new(TS::FINITE(1), TS::FINITE(8))));

        assert!(!mem_until(10, 10, TimeInterval::new(TS::FINITE(2), TS::FINITE(8))));
        assert!(!mem_until(10, 10, TimeInterval::new(TS::FINITE(2), TS::FINITE(8))));
        assert!(mem_until(10, 8, TimeInterval::new(TS::FINITE(2), TS::FINITE(8))));
        assert!(mem_until(10, 4, TimeInterval::new(TS::FINITE(2), TS::FINITE(8))));
        assert!(mem_until(10, 4, TimeInterval::new(TS::FINITE(2), TS::FINITE(8))));
        assert!(mem_until(10, 2, TimeInterval::new(TS::FINITE(2), TS::FINITE(8))));

        assert!(!mem_until(10, 10, TimeInterval::new(TS::FINITE(3), TS::FINITE(8))));
        assert!(!mem_until(10, 10, TimeInterval::new(TS::FINITE(3), TS::FINITE(8))));
        assert!(!mem_until(10, 8, TimeInterval::new(TS::FINITE(3), TS::FINITE(8))));
        assert!(mem_until(10, 4, TimeInterval::new(TS::FINITE(3), TS::FINITE(8))));
        assert!(mem_until(10, 4, TimeInterval::new(TS::FINITE(3), TS::FINITE(8))));
        assert!(mem_until(10, 2, TimeInterval::new(TS::FINITE(3), TS::FINITE(8))));
    }

    #[test]
    fn mem_until_test_eq_ts_2() {
        let _tp_to_ts : HashMap<usize, usize> = HashMap::from([(0, 2), (1, 4), (2, 4), (3, 8), (4, 10), (5, 10)]);
        assert!(!mem_until(10, 10, TimeInterval::new(TS::FINITE(4), TS::FINITE(6))));
        assert!(!mem_until(10, 10, TimeInterval::new(TS::FINITE(4), TS::FINITE(6))));
        assert!(!mem_until(10, 8, TimeInterval::new(TS::FINITE(4), TS::FINITE(6))));
        assert!(mem_until(10, 4, TimeInterval::new(TS::FINITE(4), TS::FINITE(6))));
        assert!(mem_until(10, 4, TimeInterval::new(TS::FINITE(4), TS::FINITE(6))));
        assert!(!mem_until(10, 2, TimeInterval::new(TS::FINITE(4), TS::FINITE(6))));

        assert!(!mem_until(10, 10, TimeInterval::new(TS::FINITE(8), TS::FINITE(8))));
        assert!(!mem_until(10, 10, TimeInterval::new(TS::FINITE(8), TS::FINITE(8))));
        assert!(!mem_until(10, 8, TimeInterval::new(TS::FINITE(8), TS::FINITE(8))));
        assert!(!mem_until(10, 4, TimeInterval::new(TS::FINITE(8), TS::FINITE(8))));
        assert!(!mem_until(10, 4, TimeInterval::new(TS::FINITE(8), TS::FINITE(8))));
        assert!(mem_until(10, 2, TimeInterval::new(TS::FINITE(8), TS::FINITE(8))));

        assert!(mem_until(10, 10, TimeInterval::new(TS::FINITE(0), TS::FINITE(3))));
        assert!(mem_until(10, 10, TimeInterval::new(TS::FINITE(0), TS::FINITE(3))));
        assert!(mem_until(10, 8, TimeInterval::new(TS::FINITE(0), TS::FINITE(3))));
        assert!(!mem_until(10, 4, TimeInterval::new(TS::FINITE(0), TS::FINITE(3))));
        assert!(!mem_until(10, 4, TimeInterval::new(TS::FINITE(0), TS::FINITE(3))));
        assert!(!mem_until(10, 2, TimeInterval::new(TS::FINITE(0), TS::FINITE(3))));

        assert!(mem_until(10, 10, TimeInterval::new(TS::FINITE(0), TS::FINITE(7))));
        assert!(mem_until(10, 10, TimeInterval::new(TS::FINITE(0), TS::FINITE(7))));
        assert!(mem_until(10, 8, TimeInterval::new(TS::FINITE(0), TS::FINITE(7))));
        assert!(mem_until(10, 4, TimeInterval::new(TS::FINITE(0), TS::FINITE(7))));
        assert!(mem_until(10, 4, TimeInterval::new(TS::FINITE(0), TS::FINITE(7))));
        assert!(!mem_until(10, 2, TimeInterval::new(TS::FINITE(0), TS::FINITE(7))));
    }

    #[test]
    fn get_common_variables_test() {
        let vs1 = vec!["a".into(), "b".into(), "c".into()];
        let vs2 = vec!["c".into(), "b".into(), "w".into(), "a".into()];

        let actual = get_common_variables(vs1, vs2);

        let expected = (vec!["a".into(), "b".into(), "c".into()], vec![3, 1, 0]);

        assert_eq!(expected, actual);

        let vs1 = vec!["a".into()];
        let vs2 = vec!["c".into(), "b".into(), "w".into(), "a".into()];

        let actual = get_common_variables(vs1, vs2);

        let expected = (vec!["a".into()], vec![3]);

        assert_eq!(expected, actual);
    }

    #[test]
    fn split_keys_values_test() {
        let vs1 = vec!["a".into(), "b".into()];
        let _data1 = vec![
            Arg::Cst(Str("a".parse().unwrap())),
            Arg::Cst(Str("b".parse().unwrap())),
        ];
        let vs2 = vec!["c".into(), "b".into(), "w".into(), "a".into()];
        let data2 = vec![
            (Str("c".parse().unwrap())),
            (Str("b".parse().unwrap())),
            (Str("w".parse().unwrap())),
            (Str("a".parse().unwrap())),
        ];

        let (_actual, order) = get_common_variables(vs1, vs2);

        let split_key_res = split_keys_values(data2, order);

        assert_eq!(
            vec![
                (Str("a".parse().unwrap())),
                (Str("b".parse().unwrap()))
            ],
            split_key_res
        );

        let vs1 = vec!["w".into(), "c".into()];
        let _data1 = vec![
            (Str("a".parse().unwrap())),
            (Str("b".parse().unwrap())),
        ];
        let vs2 = vec!["c".into(), "b".into(), "w".into(), "a".into()];
        let data2 = vec![
            (Str("c".parse().unwrap())),
            (Str("b".parse().unwrap())),
            (Str("w".parse().unwrap())),
            (Str("a".parse().unwrap())),
        ];

        let (_actual, order) = get_common_variables(vs1, vs2);

        let split_key_res = split_keys_values(data2, order);

        assert_eq!(
            vec![
                (Str("w".parse().unwrap())),
                (Str("c".parse().unwrap()))
            ],
            split_key_res
        );
    }

    #[test]
    fn index_data_structure_test() {
        let alpha_1_keys = vec!["a".into(), "b".into(), "c".into()];
        let alpha_2_keys = vec!["t".into(), "d".into()];
        let alpha1 = vec![(Int(1)), (Str("b".into())), (Int(2))];
        let alpha2 = vec![(Str("test1".into())), (Str("test".into()))];

        let beta_1_keys = vec!["b".into(), "c".into(), "w".into(), "t".into(), "a".into()];
        let beta_2_keys = vec!["d".into(), "t".into()];
        let beta1 = vec![
            (Str("b".into())),
            (Int(2)),
            (Str("w".into())),
            (Str("t".into())),
            (Int(1)),
        ];
        let beta2 = vec![(Str("test".into())), (Str("test1".into()))];

        let (_common_1, order_1) = get_common_variables(alpha_1_keys.clone(), beta_1_keys.clone());
        let (_common_2, order_2) = get_common_variables(alpha_2_keys.clone(), beta_2_keys.clone());

        // check that beta and alpha hash to the same entry
        let mut index1: HashMap<Vec<Constant>, i32> = HashMap::new();
        // case 1 alpha then beta
        index1.insert(alpha1.clone(), 10);
        let key1 = split_keys_values(beta1, order_1);
        assert!(index1.contains_key(&*key1));
        assert_eq!(*index1.entry(key1.clone()).or_default(), 10);

        // check that beta and alpha hash to the same entry
        let mut index2: HashMap<Vec<Constant>, i32> = HashMap::new();
        // case 2 beta then alpha
        let key2 = split_keys_values(beta2, order_2);
        index2.insert(key2.clone(), 100);
        assert!(index2.contains_key(&*alpha2));
        assert_eq!(*index2.entry(alpha2.clone()).or_default(), 100);
    }

    #[test]
    fn find_common_vars() {
        let vs1 = vec!["x".into(), "y".into(), "s".into()];
        let vs2 = vec!["z".into(), "x".into(), "w".into(), "s".into()];

        let actual = find_common_bound_variables(vs1, vs2);

        let expected = (
            vec!["x".into(), "s".into(), "y".into(), "z".into(), "w".into()],
            vec![0, 2],
            vec![1, 3],
        );

        assert_eq!(expected, actual);
    }

    #[test]
    fn split_keys_test() {
        let tuple = vec![1, 2, 3];
        let key_indices = vec![1, 0];

        let expected = (vec![2, 1], vec![3]);
        let actual = split_keys(tuple, &key_indices);

        assert_eq!(expected, actual);
    }

    #[test]
    fn join_test() {
        let data_lhs = vec![
            vec![(0, vec![0]), (0, vec![1]), (0, vec![2])],
            vec![(1, vec![3]), (1, vec![4]), (1, vec![5])],
            vec![(2, vec![6]), (2, vec![7]), (2, vec![8])],
        ];

        let data_rhs = vec![
            vec![
                (0, vec![0, 10]),
                (0, vec![1, 100]),
                (0, vec![4, 10]),
                (0, vec![2, 10]),
            ],
            vec![(1, vec![1, 11]), (1, vec![7, 11]), (1, vec![5, 11])],
            vec![(2, vec![0, 12]), (2, vec![7, 12]), (2, vec![4, 12])],
        ];

        let expected = vec![
            (
                0,
                vec![(0, vec![0, 10]), (0, vec![1, 100]), (0, vec![2, 10])],
            ),
            (1, vec![(1, vec![5, 11])]),
            (2, vec![(2, vec![7, 12])]),
        ];

        let times = vec![(1), (2), (3)];

        let lhs_attrs = vec!["x".into()];
        let rhs_attrs = vec!["x".into(), "y".into()];

        test_join(
            0,
            data_lhs.clone(),
            data_rhs.clone(),
            lhs_attrs.clone(),
            rhs_attrs.clone(),
            times.clone(),
            expected.clone(),
        );
    }
    #[test]
    fn join_test1() {
        let data_lhs = vec![
            vec![(0, vec!["0".to_string()]), (0, vec!["1".to_string()]), (0, vec!["2".to_string()])],
            vec![(1, vec!["3".to_string()]), (1, vec!["4".to_string()]), (1, vec!["5".to_string()])],
            vec![(2, vec!["6".to_string()]), (2, vec!["7".to_string()]), (2, vec!["8".to_string()])],
        ];

        let data_rhs = vec![
            vec![
                (0, vec!["0".to_string(), "10".to_string()]),
                (0, vec!["1".to_string(), "100".to_string()]),
                (0, vec!["4".to_string(), "10".to_string()]),
                (0, vec!["2".to_string(), "10".to_string()]),
            ],
            vec![(1, vec!["1".to_string(), "11".to_string()]), (1, vec!["7".to_string(), "11".to_string()]), (1, vec!["5".to_string(), "11".to_string()])],
            vec![(2, vec!["0".to_string(), "12".to_string()]), (2, vec!["7".to_string(), "12".to_string()]), (2, vec!["4".to_string(), "12".to_string()])],
        ];

        let expected = vec![
            (
                0,
                vec![(0, vec!["0".to_string(), "10".to_string()]), (0, vec!["1".to_string(), "100".to_string()]), (0, vec!["2".to_string(), "10".to_string()])],
            ),
            (1, vec![(1, vec!["5".to_string(), "11".to_string()])]),
            (2, vec![(2, vec!["7".to_string(), "12".to_string()])]),
        ];

        let times = vec![(1), (2), (3)];

        let lhs_attrs = vec!["x".into()];
        let rhs_attrs = vec!["x".into(), "y".into()];

        test_join1(
            0,
            data_lhs.clone(),
            data_rhs.clone(),
            lhs_attrs.clone(),
            rhs_attrs.clone(),
            times.clone(),
            expected.clone(),
        );
    }

    #[test]
    fn sub_test() {
        let bot = "Ol";
        let key = vec![Arg::Cst(Str(bot.to_string()))];
        let key1 = vec![Arg::Cst(Str(bot.clone().to_string()))];
        //let key1 = vec![Arg::Str(format!("Oly{}01", "ion"))];

        let mut tmp = HashMap::new();
        tmp.insert(key, true);

        if let Some(t) = tmp.get(&key1) {
            assert!(t);
        }
        else {
            assert!(false);
        }

    }

    #[test]
    fn join_test2() {
        let data_lhs = vec![
            vec![(0, vec!["Olyion01".to_string()]), (0, vec!["ABC ^%$%^$%&".to_string()]), (0, vec!["Aethelflæd".to_string()])],
            vec![(1, vec!["3".to_string()]), (1, vec!["4".to_string()]), (1, vec!["5".to_string()])],
            vec![(2, vec!["6".to_string()]), (2, vec!["Plaça de Maig".to_string()]), (2, vec!["8".to_string()])],
        ];

        let data_rhs = vec![
            vec![
                (0, vec![format!("Olyion01")]),
                (0, vec!["ABC ^%$%^$%&".to_string()]),
                (0, vec!["4".to_string()]),
                (0, vec!["Aethelflæd".to_string()]),
            ],
            vec![(1, vec!["1".to_string()]), (1, vec!["7".to_string()]), (1, vec!["5".to_string()])],
            vec![(2, vec!["0".to_string()]), (2, vec!["Plaça de Maig".to_string()]), (2, vec!["4".to_string()])],
        ];

        let expected = vec![
            (
                0,
                vec![(0, vec!["Olyion01".to_string()]), (0, vec!["ABC ^%$%^$%&".to_string()]), (0, vec!["Aethelflæd".to_string()])],
            ),
            (1, vec![(1, vec!["5".to_string()])]),
            (2, vec![(2, vec!["Plaça de Maig".to_string()])]),
        ];

        let times = vec![(1), (2), (3)];

        let lhs_attrs = vec!["x".into()];
        let rhs_attrs = vec!["x".into()];

        test_join1(
            0,
            data_lhs.clone(),
            data_rhs.clone(),
            lhs_attrs.clone(),
            rhs_attrs.clone(),
            times.clone(),
            expected.clone(),
        );
    }

    #[test]
    fn union_test() {
        let data_lhs = vec![
            vec![(0, vec![0, 20]), (0, vec![1, 20]), (0, vec![2, 20])],
            vec![(1, vec![3, 20]), (1, vec![4, 20]), (1, vec![5, 20])],
            vec![(2, vec![6, 20]), (2, vec![7, 20]), (2, vec![8, 20])],
        ];

        let data_rhs = vec![
            vec![(0, vec![10, 0]), (0, vec![10, 4]), (0, vec![10, 2])],
            vec![(1, vec![11, 1]), (1, vec![11, 7]), (1, vec![11, 5])],
            vec![(2, vec![12, 0]), (2, vec![12, 7]), (2, vec![12, 4])],
        ];

        let expected = vec![
            (
                0,
                vec![
                    (0, vec![0, 10]),
                    (0, vec![0, 20]),
                    (0, vec![1, 20]),
                    (0, vec![2, 10]),
                    (0, vec![2, 20]),
                    (0, vec![4, 10]),
                ],
            ),
            (
                1,
                vec![
                    (1, vec![1, 11]),
                    (1, vec![3, 20]),
                    (1, vec![4, 20]),
                    (1, vec![5, 11]),
                    (1, vec![5, 20]),
                    (1, vec![7, 11]),
                ],
            ),
            (
                2,
                vec![
                    (2, vec![0, 12]),
                    (2, vec![4, 12]),
                    (2, vec![6, 20]),
                    (2, vec![7, 12]),
                    (2, vec![7, 20]),
                    (2, vec![8, 20]),
                ],
            ),
        ];

        let times = vec![(1), (2), (3)];

        let lhs_attrs = vec!["x".into(), "y".into()];
        let rhs_attrs = vec!["y".into(), "x".into()];

        test_join(
            1,
            data_lhs.clone(),
            data_rhs.clone(),
            lhs_attrs.clone(),
            rhs_attrs.clone(),
            times.clone(),
            expected.clone(),
        );
    }

    #[test]
    fn anti_join_test() {
        let data_lhs = vec![
            vec![(0, vec![0]), (0, vec![1]), (0, vec![2])],
            vec![(1, vec![3]), (1, vec![4]), (1, vec![5])],
            vec![(2, vec![6]), (2, vec![7]), (2, vec![8])],
        ];
        let data_rhs = vec![
            vec![(0, vec![0]), (0, vec![3]), (0, vec![6])],
            vec![(1, vec![1]), (1, vec![4]), (1, vec![7])],
            vec![(2, vec![2]), (2, vec![5]), (2, vec![8])],
        ];

        let times = vec![(1), (2), (3), (4)];

        let lhs_attrs = vec!["x".into()];
        let rhs_attrs = vec!["x".into()];

        let expected = vec![
            (0, vec![(0, vec![1]), (0, vec![2])]),
            (1, vec![(1, vec![3]), (1, vec![5])]),
            (2, vec![(2, vec![6]), (2, vec![7])]),
        ];

        test_join(
            2,
            data_lhs.clone(),
            data_rhs.clone(),
            lhs_attrs.clone(),
            rhs_attrs.clone(),
            times.clone(),
            expected.clone(),
        );
    }

    #[test]
    fn get_wanted_indices_test() {
        let column_attrs = vec!["a".into(), "b".into(), "c".into()];
        let unwanted_column_attrs = vec!["c".into()];

        let (indices, new_attrs) = get_wanted_indices(&column_attrs, &unwanted_column_attrs);

        let expected_indices = vec![0, 1];

        assert_eq!(vec!["a".to_string(), "b".to_string()], new_attrs);
        assert_eq!(expected_indices, indices);
    }

    #[test]
    fn projection_test() {
        let data = vec![
            vec![(0, vec![0, 100]), (0, vec![1, 101]), (0, vec![2, 102])],
            vec![(1, vec![3, 113]), (1, vec![4, 114]), (1, vec![5, 115])],
            vec![(2, vec![6, 126]), (2, vec![7, 127]), (2, vec![8, 128])],
        ];

        let times = vec![(1, 1), (2, 2), (3, 3)];

        let attrs = vec!["x".into(), "y".into()];

        let mut unwanted_attrs = vec!["y".into()];

        let mut expected = vec![
            (0, vec![vec![0], vec![1], vec![2]]),
            (1, vec![vec![3], vec![4], vec![5]]),
            (2, vec![vec![6], vec![7], vec![8]]),
        ];

        test_projection(
            data.clone(),
            attrs.clone(),
            unwanted_attrs.clone(),
            times.clone(),
            expected.clone(),
        );

        unwanted_attrs = vec!["x".into()];

        expected = vec![
            (0, vec![vec![100], vec![101], vec![102]]),
            (1, vec![vec![113], vec![114], vec![115]]),
            (2, vec![vec![126], vec![127], vec![128]]),
        ];

        test_projection(
            data.clone(),
            attrs.clone(),
            unwanted_attrs.clone(),
            times.clone(),
            expected.clone(),
        );
    }

    #[test]
    fn property_test_since() {
        let data_lhs = vec![
            vec![StreamData::D(0, vec![1])],
            vec![StreamData::D(1, vec![1])],
            vec![StreamData::D(2, vec![1])],
            vec![StreamData::D(3, vec![1])],
            vec![StreamData::D(4, vec![1])],
            vec![StreamData::D(5, vec![1])],
        ];

        let data_rhs = vec![
            vec![StreamData::D(0, vec![1, 1])],
            vec![StreamData::M(1, true)],
            vec![StreamData::M(2, true)],
            vec![StreamData::M(3, true)],
            vec![StreamData::M(4, true)],
            vec![StreamData::M(5, true)],
        ];

        let data_expected = vec![
            (0, vec![(0, vec![1, 1])]),
            (1, vec![(1, vec![1, 1])]),
            (2, vec![(2, vec![1, 1])]),
            (3, vec![(3, vec![1, 1])]),
            (4, vec![(4, vec![1, 1])]),
            (5, vec![(5, vec![1, 1])]),
        ];

        let times = vec![(0,0), (1,1), (2,2), (3,3), (4,4), (5,5)];

        let lhs_attrs: Vec<String> = vec!["x".into()];
        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        let vecs = &[0, 1, 2, 3, 4, 5];
        let permutations_lhs = permutations(vecs);

        let permutations_rhs = vec![
            vec![0, 1, 2, 3, 4, 5],
            vec![1, 0, 2, 3, 4, 5],
            vec![1, 2, 0, 3, 4, 5],
            vec![1, 2, 3, 0, 4, 5],
            vec![1, 2, 3, 4, 0, 5],
            vec![1, 2, 3, 4, 5, 0],
        ];

        let mut c = 0;

        let time_interval = TimeInterval::new(TS::new(0), TS::INFINITY);
        for rhs in permutations_rhs.clone() {
            for lhs in permutations_lhs.clone() {
                c += 1;
                test_since_property(
                    data_lhs.clone(),
                    data_rhs.clone(),
                    lhs_attrs.clone(),
                    rhs_attrs.clone(),
                    times.clone(),
                    data_expected.clone(),
                    rhs.clone(),
                    lhs.clone(),
                    time_interval.clone(),
                );
            }
        }

        println!("number of iterations: {}", c.clone());
    }

    #[test]
    fn property_test_since_1() {
        let data_lhs = vec![
            vec![StreamData::D(0, vec![1])],
            vec![StreamData::D(1, vec![1])],
            vec![StreamData::D(2, vec![1])],
            vec![StreamData::D(3, vec![1])],
            vec![StreamData::D(4, vec![1])],
            vec![StreamData::D(5, vec![1])],
        ];

        let data_rhs = vec![
            vec![StreamData::M(0, true)],
            vec![StreamData::M(1, true)],
            vec![StreamData::M(2, true)],
            vec![StreamData::D(3, vec![1, 2])],
            vec![StreamData::M(4, true)],
            vec![StreamData::M(5, true)],
        ];

        let data_expected = vec![
            (3, vec![(3, vec![1, 2])]),
            (4, vec![(4, vec![1, 2])]),
            (5, vec![(5, vec![1, 2])]),
        ];

        let times = vec![(0,0), (1,1), (2,2), (3,3), (4,4), (5,5)];

        let lhs_attrs: Vec<String> = vec!["x".into()];
        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        let vecs = &[0, 1, 2, 3, 4, 5];
        let permutations_lhs = permutations(vecs);

        // zero and three all permutations
        let permutations_rhs = vec![
            vec![0, 1, 2, 3, 4, 5],
            vec![1, 0, 2, 3, 4, 5],
            vec![1, 2, 0, 3, 4, 5],
            vec![1, 2, 3, 0, 4, 5],
            vec![1, 2, 3, 4, 0, 5],
            vec![1, 2, 3, 4, 5, 0],
        ];

        let mut c = 0;

        let time_interval = TimeInterval::new(TS::new(0), TS::INFINITY);
        for rhs in permutations_rhs.clone() {
            for lhs in permutations_lhs.clone() {
                c += 1;
                test_since_property(
                    data_lhs.clone(),
                    data_rhs.clone(),
                    lhs_attrs.clone(),
                    rhs_attrs.clone(),
                    times.clone(),
                    data_expected.clone(),
                    rhs.clone(),
                    lhs.clone(),
                    time_interval.clone(),
                );
            }
        }

        println!("number of iterations: {}", c.clone());
    }

    #[test]
    fn property_test_since_2() {
        let data_lhs = vec![
            vec![StreamData::D(0, vec![1])],
            vec![StreamData::D(1, vec![1])],
            vec![StreamData::D(2, vec![1])],
            vec![StreamData::D(3, vec![1])],
            vec![StreamData::D(4, vec![1])],
            vec![StreamData::D(5, vec![1])],
        ];

        let data_rhs = vec![
            vec![StreamData::D(0, vec![1, 1])],
            vec![StreamData::M(1, true)],
            vec![StreamData::M(2, true)],
            vec![StreamData::D(3, vec![1, 2])],
            vec![StreamData::M(4, true)],
            vec![StreamData::M(5, true)],
        ];

        let data_expected = vec![
            (0, vec![(0, vec![1, 1])]),
            (1, vec![(1, vec![1, 1])]),
            (2, vec![(2, vec![1, 1])]),
            (3, vec![(3, vec![1, 1]), (3, vec![1, 2])]),
            (4, vec![(4, vec![1, 1]), (4, vec![1, 2])]),
            (5, vec![(5, vec![1, 1]), (5, vec![1, 2])]),
        ];

        let times = vec![(0,0), (1,1), (2,2), (3,3), (4,4), (5,5)];

        let lhs_attrs: Vec<String> = vec!["x".into()];
        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        let vecs = &[0, 1, 2, 3, 4, 5];
        let permutations_lhs = permutations(vecs);

        let mut c = 0;
        let time_interval = TimeInterval::new(TS::new(0), TS::INFINITY);
        for rhs in &permutations_lhs[0..100] {
            for lhs in permutations_lhs.clone() {
                c += 1;
                test_since_property(
                    data_lhs.clone(),
                    data_rhs.clone(),
                    lhs_attrs.clone(),
                    rhs_attrs.clone(),
                    times.clone(),
                    data_expected.clone(),
                    rhs.clone(),
                    lhs.clone(),
                    time_interval.clone(),
                );
            }
        }

        println!("number of iterations: {}", c.clone());
    }

    #[test]
    fn property_test_until() {
        let data_lhs = vec![
            vec![StreamData::D(0, vec![1])],
            vec![StreamData::D(1, vec![1])],
            vec![StreamData::D(2, vec![1])],
            vec![StreamData::D(3, vec![1])],
            vec![StreamData::D(4, vec![1])],
            vec![StreamData::D(5, vec![1])],
        ];

        let data_rhs = vec![
            vec![StreamData::M(0, true)],
            vec![StreamData::M(1, true)],
            vec![StreamData::M(2, true)],
            vec![StreamData::M(3, true)],
            vec![StreamData::M(4, true)],
            vec![StreamData::D(5, vec![1, 1])],
        ];

        let data_expected = vec![
            (0, vec![vec![1, 1]]),
            (1, vec![vec![1, 1]]),
            (2, vec![vec![1, 1]]),
            (3, vec![vec![1, 1]]),
            (4, vec![vec![1, 1]]),
            (5, vec![vec![1, 1]]),
        ];

        let times = vec![(0,0), (1,1), (2,2), (3,3), (4,4), (5,5)];

        let lhs_attrs: Vec<String> = vec!["x".into()];
        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        let vecs = &[0, 1, 2, 3, 4, 5];
        let permutations_lhs = permutations(vecs);

        let permutations_rhs = vec![
            vec![0, 1, 2, 3, 4, 5],
            vec![1, 0, 2, 3, 4, 5],
            vec![1, 2, 0, 3, 4, 5],
            vec![1, 2, 3, 0, 4, 5],
            vec![1, 2, 3, 4, 0, 5],
            vec![1, 2, 3, 4, 5, 0],
        ];

        let mut c = 0;

        let time_interval = TimeInterval::new(TS::new(0), TS::new(10));
        for rhs in permutations_rhs.clone() {
            for lhs in permutations_lhs.clone() {
                c += 1;
                test_until_property(
                    data_lhs.clone(),
                    data_rhs.clone(),
                    lhs_attrs.clone(),
                    rhs_attrs.clone(),
                    times.clone(),
                    data_expected.clone(),
                    rhs.clone(),
                    lhs.clone(),
                    time_interval.clone(),
                );
            }
        }

        println!("number of iterations: {}", c.clone());
    }

    #[test]
    fn property_test_until_debug() {
        let data_lhs = vec![
            vec![StreamData::D(0, vec![1])],
            vec![StreamData::D(1, vec![1])],
            vec![StreamData::D(2, vec![1])],
            vec![StreamData::D(3, vec![1])],
            vec![StreamData::D(4, vec![1])],
            vec![StreamData::D(5, vec![1])],
        ];

        let data_rhs = vec![
            vec![StreamData::M(0, true)],
            vec![StreamData::M(1, true)],
            vec![StreamData::D(2, vec![1, 2])],
            vec![StreamData::M(3, true)],
            vec![StreamData::M(4, true)],
            vec![StreamData::D(5, vec![1, 1])]
        ];

        let data_expected = vec![
            (0, vec![vec![1, 1], vec![1, 2]]),
            (1, vec![vec![1, 1], vec![1, 2]]),
            (2, vec![vec![1, 1], vec![1, 2]]),
            (3, vec![vec![1, 1]]),
            (4, vec![vec![1, 1]]),
            (5, vec![vec![1, 1]]),
        ];

        let times = vec![(0,0), (1,1), (2,2), (3,3), (4,4), (5,5)];

        let lhs_attrs: Vec<String> = vec!["x".into()];
        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        let vecs = &[0, 1, 2, 3, 4, 5];
        let _permutations_lhs = permutations(vecs);

        let mut c = 0;

        let time_interval = TimeInterval::new(TS::new(0), TS::new(10));
        for _x in 0..500 {
            for rhs in vec![vec![0, 1, 2, 3, 4, 5]] {
                for lhs in vec![vec![3, 1, 2, 4, 5, 0]] {
                    c += 1;
                    test_until_property(
                        data_lhs.clone(),
                        data_rhs.clone(),
                        lhs_attrs.clone(),
                        rhs_attrs.clone(),
                        times.clone(),
                        data_expected.clone(),
                        rhs.clone(),
                        lhs.clone(),
                        time_interval.clone(),
                    );
                }
            }
        }

        println!("number of iterations: {}", c.clone());
    }

    #[test]
    fn property_test_until_1() {
        let data_lhs = vec![
            vec![StreamData::D(0, vec![1])],
            vec![StreamData::D(1, vec![1])],
            vec![StreamData::D(2, vec![1])],
            vec![StreamData::D(3, vec![1])],
            vec![StreamData::D(4, vec![1])],
            vec![StreamData::D(5, vec![1])],
        ];

        let data_rhs = vec![
            vec![StreamData::M(0, true)],
            vec![StreamData::M(1, true)],
            vec![StreamData::D(2, vec![1, 2])],
            vec![StreamData::M(3, true)],
            vec![StreamData::M(4, true)],
            vec![StreamData::D(5, vec![1, 1])],
        ];

        let data_expected = vec![
            (0, vec![vec![1, 1], vec![1, 2]]),
            (1, vec![vec![1, 1], vec![1, 2]]),
            (2, vec![vec![1, 1], vec![1, 2]]),
            (3, vec![vec![1, 1]]),
            (4, vec![vec![1, 1]]),
            (5, vec![vec![1, 1]]),
        ];

        let times = vec![(0,0), (1,1), (2,2), (3,3), (4,4), (5,5)];

        let lhs_attrs: Vec<String> = vec!["x".into()];
        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        let vecs = &[0, 1, 2, 3, 4, 5];
        let permutations_lhs = permutations(vecs);

        let mut c = 0;

        let time_interval = TimeInterval::new(TS::new(0), TS::new(10));
        for rhs in &permutations_lhs[0..100] {
            for lhs in permutations_lhs.clone() {
                c += 1;

                test_until_property(
                    data_lhs.clone(),
                    data_rhs.clone(),
                    lhs_attrs.clone(),
                    rhs_attrs.clone(),
                    times.clone(),
                    data_expected.clone(),
                    rhs.clone(),
                    lhs.clone(),
                    time_interval.clone(),
                );
            }
        }

        println!("number of iterations: {}", c.clone());
    }

    #[test]
    fn property_test_until_2() {
        let data_lhs = vec![
            vec![StreamData::D(2, vec![1])],
            vec![StreamData::D(4, vec![1])],
            vec![StreamData::D(6, vec![1])],
            vec![StreamData::D(8, vec![1])],
            vec![StreamData::D(10, vec![1])],
            vec![StreamData::D(12, vec![1])],
        ];

        let data_rhs = vec![
            vec![StreamData::M(2, true)],
            vec![StreamData::M(4, true)],
            vec![StreamData::M(6, true)],
            vec![StreamData::M(8, true)],
            vec![StreamData::M(10, true)],
            vec![StreamData::D(12, vec![1, 1])],
        ];

        let data_expected = vec![
            (0, vec![vec![1, 1]]),
            (1, vec![vec![1, 1]]),
            (2, vec![vec![1, 1]]),
            (3, vec![vec![1, 1]]),
            (4, vec![vec![1, 1]]),
            (5, vec![vec![1, 1]]),
        ];

        let times = vec![(0,2), (1,4), (2,6), (3,8), (4,10), (5,12)];

        let lhs_attrs: Vec<String> = vec!["x".into()];
        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        let vecs = &[0, 1, 2, 3, 4, 5];
        let permutations_lhs = permutations(vecs);

        let permutations_rhs = vec![
            vec![0, 1, 2, 3, 4, 5],
            vec![1, 0, 2, 3, 4, 5],
            vec![1, 2, 0, 3, 4, 5],
            vec![1, 2, 3, 0, 4, 5],
            vec![1, 2, 3, 4, 0, 5],
            vec![1, 2, 3, 4, 5, 0],
        ];

        let mut c = 0;

        let time_interval = TimeInterval::new(TS::new(0), TS::new(12));
        for rhs in permutations_rhs.clone() {
            for lhs in permutations_lhs.clone() {
                c += 1;
                test_until_property(
                    data_lhs.clone(),
                    data_rhs.clone(),
                    lhs_attrs.clone(),
                    rhs_attrs.clone(),
                    times.clone(),
                    data_expected.clone(),
                    rhs.clone(),
                    lhs.clone(),
                    time_interval.clone(),
                );
            }
        }

        println!("number of iterations: {}", c.clone());
    }

    #[test]
    fn property_test_until_3() {
        let data_lhs = vec![
            vec![StreamData::D(2, vec![1])],
            vec![StreamData::D(4, vec![1])],
            vec![StreamData::D(6, vec![1])],
            vec![StreamData::D(8, vec![1])],
            vec![StreamData::D(10, vec![1])],
            vec![StreamData::D(12, vec![1])],
        ];

        let data_rhs = vec![
            vec![StreamData::M(2, true)],
            vec![StreamData::M(4, true)],
            vec![StreamData::M(6, true)],
            vec![StreamData::M(8, true)],
            vec![StreamData::M(10, true)],
            vec![StreamData::D(12, vec![1, 1])],
        ];

        let data_expected = vec![
            (2, vec![vec![1, 1]]),
            (3, vec![vec![1, 1]]),
            (4, vec![vec![1, 1]]),
            (5, vec![vec![1, 1]]),
        ];

        let times = vec![(0,2), (1,4), (2,6), (3,8), (4,10), (5,12)];

        let lhs_attrs: Vec<String> = vec!["x".into()];
        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        let vecs = &[0, 1, 2, 3, 4, 5];
        let permutations_lhs = permutations(vecs);

        let permutations_rhs = vec![
            vec![0, 1, 2, 3, 4, 5],
            vec![1, 0, 2, 3, 4, 5],
            vec![1, 2, 0, 3, 4, 5],
            vec![1, 2, 3, 0, 4, 5],
            vec![1, 2, 3, 4, 0, 5],
            vec![1, 2, 3, 4, 5, 0],
        ];

        let mut c = 0;

        let time_interval = TimeInterval::new(TS::new(0), TS::new(6));
        for rhs in permutations_rhs.clone() {
            for lhs in permutations_lhs.clone() {
                c += 1;
                test_until_property(
                    data_lhs.clone(),
                    data_rhs.clone(),
                    lhs_attrs.clone(),
                    rhs_attrs.clone(),
                    times.clone(),
                    data_expected.clone(),
                    rhs.clone(),
                    lhs.clone(),
                    time_interval.clone(),
                );
            }
        }

        println!("number of iterations: {}", c.clone());
    }

    #[test]
    fn property_test_until_4() {
        let data_lhs = vec![
            vec![StreamData::D(2, vec![1])],
            vec![StreamData::D(4, vec![1])],
            vec![StreamData::D(6, vec![1])],
            vec![StreamData::D(8, vec![1])],
            vec![StreamData::D(10, vec![1])],
            vec![StreamData::D(12, vec![1])],
        ];

        let data_rhs = vec![
            vec![StreamData::M(2, true)],
            vec![StreamData::M(4, true)],
            vec![StreamData::M(6, true)],
            vec![StreamData::M(8, true)],
            vec![StreamData::M(10, true)],
            vec![StreamData::D(12, vec![1, 1])],
        ];

        let data_expected = vec![
            (1, vec![vec![1, 1]]),
            (2, vec![vec![1, 1]]),
            (3, vec![vec![1, 1]]),
            (4, vec![vec![1, 1]]),
            (5, vec![vec![1, 1]]),
        ];

        let times = vec![(0,2), (1,4), (2,6), (3,8), (4,10), (5,12)];

        let lhs_attrs: Vec<String> = vec!["x".into()];
        let rhs_attrs: Vec<String> = vec!["x".into(), "y".into()];

        let vecs = &[0, 1, 2, 3, 4, 5];
        let permutations_lhs = permutations(vecs);

        let permutations_rhs = vec![
            vec![0, 1, 2, 3, 4, 5],
            vec![1, 0, 2, 3, 4, 5],
            vec![1, 2, 0, 3, 4, 5],
            vec![1, 2, 3, 0, 4, 5],
            vec![1, 2, 3, 4, 0, 5],
            vec![1, 2, 3, 4, 5, 0],
        ];

        let mut c = 0;

        let time_interval = TimeInterval::new(TS::new(0), TS::new(8));
        for rhs in permutations_rhs.clone() {
            for lhs in permutations_lhs.clone() {
                c += 1;
                test_until_property(
                    data_lhs.clone(),
                    data_rhs.clone(),
                    lhs_attrs.clone(),
                    rhs_attrs.clone(),
                    times.clone(),
                    data_expected.clone(),
                    rhs.clone(),
                    lhs.clone(),
                    time_interval.clone(),
                );
            }
        }

        println!("number of iterations: {}", c.clone());
    }

    #[test]
    fn test_equality() {
        let schema = vec!["a".to_string(), "b".to_string()];
        let target = "b".to_string();
        let new_var = "c".to_string();

        let data = vec![
            vec![(0, vec![0, 0]), (0, vec![2, 2])],
            vec![(1, vec![1, 1]), (1, vec![3, 3])],
            vec![(2, vec![5, 5])]
        ];

        let exp = vec![
            (0, vec![(0, vec![0, 0, 0]), (0, vec![2, 2, 2])]),
            (1, vec![(1, vec![1, 1, 1]), (1, vec![3, 3, 3])]),
            (2, vec![(2, vec![5, 5, 5])]),
        ];

        let times = vec![(0,1),(1,2),(2,3)];

        test_equal(data, schema, target, new_var, times,exp);
    }

    #[test]
    fn test_equality1() {
        let schema = vec!["a".to_string()];
        let target = "a".to_string();
        let new_var = "c".to_string();

        let data = vec![
            vec![(0, vec![0]), (0, vec![2])],
            vec![(1, vec![1]), (1, vec![3])],
            vec![(2, vec![5])]
        ];

        let exp = vec![
            (0, vec![(0, vec![0, 0]), (0, vec![2, 2])]),
            (1, vec![(1, vec![1, 1]), (1, vec![3, 3])]),
            (2, vec![(2, vec![5, 5])]),
        ];

        let times = vec![(0,1),(1,2),(2,3)];

        test_equal(data, schema, target, new_var, times,exp);
    }


    #[test]
    fn test_equality2() {
        let schema = vec!["a".to_string(), "b".to_string()];
        let target = "a".to_string();
        let new_var = "c".to_string();

        let data = vec![
            vec![(0, vec![0, 1]), (0, vec![2, 3])],
            vec![(1, vec![1, 2]), (1, vec![3, 4])],
            vec![(2, vec![5, 6])]
        ];

        let exp = vec![
            (0, vec![(0, vec![0, 1, 0]), (0, vec![2, 3, 2])]),
            (1, vec![(1, vec![1, 2, 1]), (1, vec![3, 4, 3])]),
            (2, vec![(2, vec![5, 6, 5])]),
        ];

        let times = vec![(0,1),(1,2),(2,3)];

        test_equal(data, schema, target, new_var, times,exp);
    }


    // test functions ------------------------------------------------------------------------------
    fn permutations(v: &[usize]) -> Vec<Vec<usize>> {
        if v.len() == 0 {
            vec![]
        } else if v.len() == 1 {
            vec![v.to_vec()]
        } else {
            permutations(&v[1..])
                .iter()
                .map(|p| {
                    let mut perms = vec![];
                    for i in 0..p.len() + 1 {
                        let mut perm = p.clone();
                        perm.insert(i, v[0]);
                        perms.push(perm);
                    }
                    perms
                })
                .flatten()
                .collect()
        }
    }

    fn test_since_property(
        data_lhs: Vec<Vec<StreamData>>,
        data_rhs: Vec<Vec<StreamData>>,
        lhs_attrs: Vec<String>,
        rhs_attrs: Vec<String>,
        times: Vec<(usize, usize)>,
        expected: Vec<(usize, Vec<(usize, Vec<i32>)>)>,
        new_order_lhs: Vec<usize>,
        new_order_rhs: Vec<usize>,
        time_interval: TimeInterval,
    ) {
        let (send, recv) = std::sync::mpsc::channel();
        let send = std::sync::Arc::new(std::sync::Mutex::new(send));
        timely::execute(timely::Config::process(NUM_WORKERS), move |worker| {
            let send = send.lock().unwrap().clone();

            let ((mut lhs_input, cap_lhs), (mut rhs_input, cap_rhs), (mut time_input, cap_time), _probe) = worker
                .dataflow::<usize, _, _>(|scope| {
                    let (time_input, time_stream) = scope.new_unordered_input::<TimeFlowValues>();
                    let (lhs_input, lhs_stream) = scope.new_unordered_input::<Record>();
                    let (rhs_input, rhs_stream) = scope.new_unordered_input::<Record>();

                    let (_attr, output) = lhs_stream
                        .distribute(&mut 0, true, &lhs_attrs, &rhs_attrs).1
                        .since(&mut 0, rhs_stream.distribute(&mut 0, false, &lhs_attrs, &rhs_attrs).1,
                            time_stream.broadcast(), &lhs_attrs, &rhs_attrs, time_interval, false).1
                        .exhaust(&mut 0, time_stream.broadcast(), vec![], default_options());

                    let probe = output.probe();
                    output.capture_into(send);
                    (lhs_input, rhs_input, time_input, probe)
                });

            // give a list of pairs tp -> ts
            let max_length = max(max(data_rhs.len(), data_lhs.len()), times.len());
            for round in 0usize..max_length {
                if worker.index() == 0 {
                    if round < times.len() {
                        time_input.session(cap_time.delayed(&times[round].0)).give(Timestamp(times[round].1));
                    }

                    if round < data_lhs.len() {
                        // let get new index and original time point
                        let new_index = new_order_lhs[round];
                        let org_tp = times[new_index].0;

                        let prod = org_tp.clone();
                        let tmp_cap = cap_lhs.delayed(&prod);

                        data_lhs.get(new_index).unwrap().iter().for_each(|data| {
                            match data.clone() {
                                StreamData::D(_ts, data) => {
                                    let event = Record::Data(
                                        true,
                                        data.iter().cloned().map(|arg| Int(arg)).collect(),
                                    );
                                    lhs_input.session(tmp_cap.clone()).give(event);
                                }
                                StreamData::M(_ts, bool) => {
                                    let event = Record::MetaData(bool.clone(), bool);
                                    lhs_input.session(tmp_cap.clone()).give(event);
                                }
                            };
                        });
                    }

                    if round < data_rhs.len() {
                        let new_index = new_order_rhs[round];
                        let org_tp = times[new_index].0;

                        let prod = org_tp.clone();
                        let tmp_cap = cap_rhs.delayed(&prod);

                        data_rhs.get(new_index).unwrap().iter().for_each(|data| {
                            match data.clone() {
                                StreamData::D(_ts, data) => {
                                    let event = Record::Data(
                                        true,
                                        data.iter().cloned().map(|arg| Int(arg)).collect(),
                                    );
                                    rhs_input.session(tmp_cap.clone()).give(event);
                                }
                                StreamData::M(_ts, bool) => {
                                    let event = Record::MetaData(bool.clone(), bool);
                                    rhs_input.session(tmp_cap.clone()).give(event);
                                }
                            };
                        });
                    }
                }
                worker.step();
            }

            let prod = max_length.clone();
            let tmp_cap = cap_lhs.delayed(&prod);
            lhs_input
                .session(tmp_cap)
                .give(Record::MetaData(false, false));

            let prod = max_length.clone();
            let tmp_cap1 = cap_rhs.delayed(&prod);
            rhs_input
                .session(tmp_cap1)
                .give(Record::MetaData(false, false));
            worker.step();
        }).unwrap();

        let mut actual_res: Vec<_> = recv
            .extract()
            .iter()
            .cloned()
            .map(|(time, tuples)| (time, tuples))
            .collect();

        //Vector of (timepoint, Vector of (timestamp, data))
        let mut expected_res = expected
            .into_iter()
            .map(|(timepoint, tuples)| {
                // process tuples
                let mut tmp_vec_in = vec![];
                for (_ts, tup) in tuples.iter() {
                    let mut tmp_vec = vec![];
                    for t in tup {
                        tmp_vec.push(Int(*t))
                    }
                    tmp_vec_in.push(Record::Data(true, tmp_vec));
                }
                (timepoint, tmp_vec_in)
            })
            .collect();

        sort_operator_result(&mut actual_res);
        sort_operator_result(&mut expected_res);
        assert_eq!(expected_res, actual_res);
    }

    fn test_negative_temporal_property(
        mode: bool,
        data_lhs: Vec<Vec<StreamData>>,
        data_rhs: Vec<Vec<StreamData>>,
        lhs_attrs: Vec<String>,
        rhs_attrs: Vec<String>,
        times: Vec<(usize, usize)>,
        expected: Vec<(usize, Vec<(usize, Vec<i32>)>)>,
        new_order_lhs: Vec<usize>,
        new_order_rhs: Vec<usize>,
        time_interval: TimeInterval,
    ) {
        let lhs = new_order_lhs.clone();
        let rhs = new_order_rhs.clone();

        let (send, recv) = std::sync::mpsc::channel();
        let send = std::sync::Arc::new(std::sync::Mutex::new(send));
        timely::execute(timely::Config::process(NUM_WORKERS), move |worker| {
            let send = send.lock().unwrap().clone();

            let ((mut lhs_input, cap_lhs), (mut rhs_input, cap_rhs), (mut time_input, cap_time), _probe) = worker
                .dataflow::<usize, _, _>(|scope| {
                    let (time_input, time_stream) = scope.new_unordered_input::<TimeFlowValues>();
                    let (lhs_input, lhs_stream) = scope.new_unordered_input::<Record>();
                    let (rhs_input, rhs_stream) = scope.new_unordered_input::<Record>();

                    let (_attr, output) = if mode {
                        lhs_stream.distribute(&mut 0, true, &lhs_attrs, &rhs_attrs).1.
                            neg_since(&mut 0, rhs_stream.distribute(&mut 0, false, &lhs_attrs, &rhs_attrs).1,
                                      time_stream.broadcast(), &lhs_attrs, &rhs_attrs, time_interval, false).1
                            .exhaust(&mut 0, time_stream.broadcast(), vec![], default_options())
                    } else {
                        lhs_stream.distribute(&mut 0, true, &lhs_attrs, &rhs_attrs).1.
                            neg_until(&mut 0, rhs_stream.distribute(&mut 0, false, &lhs_attrs, &rhs_attrs).1,
                                      time_stream.broadcast(), &lhs_attrs, &rhs_attrs, time_interval, false).1
                            .exhaust(&mut 0, time_stream.broadcast(), vec![], default_options())
                    };

                    let probe = output.probe();
                    output.capture_into(send);
                    (lhs_input, rhs_input, time_input, probe)
                });

            // give a list of pairs tp -> ts
            let max_length = max(max(data_rhs.len(), data_lhs.len()), times.len());
            for round in 0usize..max_length {
                if worker.index() == 0 {
                    if round < times.len() {
                        time_input.session(cap_time.delayed(&times[round].0)).give(Timestamp(times[round].1));
                    }

                    if round < data_lhs.len() {
                        // let get new index and original time point
                        let new_index = new_order_lhs[round];
                        let org_tp = times[new_index].0;

                        let prod = org_tp.clone();
                        let tmp_cap = cap_lhs.delayed(&prod);

                        data_lhs.get(new_index).unwrap().iter().for_each(|data| {
                            match data.clone() {
                                StreamData::D(_ts, data) => {
                                    let event = Record::Data(
                                        true,
                                        data.iter().cloned().map(|arg| Int(arg)).collect(),
                                    );
                                    lhs_input.session(tmp_cap.clone()).give(event);
                                }
                                StreamData::M(_ts, bool) => {
                                    let event = Record::MetaData(bool.clone(), bool);
                                    lhs_input.session(tmp_cap.clone()).give(event);
                                }
                            };
                        });
                    }

                    if round < data_rhs.len() {
                        let new_index = new_order_rhs[round];
                        let org_tp = times[new_index].0;

                        let prod = org_tp.clone();
                        let tmp_cap = cap_rhs.delayed(&prod);

                        data_rhs.get(new_index).unwrap().iter().for_each(|data| {
                            match data.clone() {
                                StreamData::D(_ts, data) => {
                                    let event = Record::Data(
                                        true,
                                        data.iter().cloned().map(|arg| Int(arg)).collect(),
                                    );
                                    rhs_input.session(tmp_cap.clone()).give(event);
                                }
                                StreamData::M(_ts, bool) => {
                                    let event = Record::MetaData(bool.clone(), bool);
                                    rhs_input.session(tmp_cap.clone()).give(event);
                                }
                            };
                        });
                    }
                }
                worker.step();
            }

            let prod = max_length.clone();
            let tmp_cap = cap_lhs.delayed(&prod);
            lhs_input
                .session(tmp_cap)
                .give(Record::MetaData(false, false));

            let prod = max_length.clone();
            let tmp_cap1 = cap_rhs.delayed(&prod);
            rhs_input
                .session(tmp_cap1)
                .give(Record::MetaData(false, false));
            worker.step();
        }).unwrap();

        let mut actual_res: Vec<_> = recv
            .extract()
            .iter()
            .cloned()
            .map(|(time, tuples)| (time, tuples))
            .collect();

        //Vector of (timepoint, Vector of (timestamp, data))
        let mut expected_res = expected
            .into_iter()
            .map(|(timepoint, tuples)| {
                // process tuples
                let mut tmp_vec_in = vec![];
                for (_ts, tup) in tuples.iter() {
                    let mut tmp_vec = vec![];
                    for t in tup {
                        tmp_vec.push(Int(*t))
                    }
                    tmp_vec_in.push(Record::Data(true, tmp_vec));
                }
                (timepoint, tmp_vec_in)
            })
            .collect();

        sort_operator_result(&mut actual_res);
        sort_operator_result(&mut expected_res);

        if expected_res != actual_res {
            println!("lhs: {:?}", lhs);
            println!("rhs: {:?}", rhs);
        }
        assert_eq!(expected_res, actual_res);
    }

    fn test_until_property(
        data_lhs: Vec<Vec<StreamData>>,
        data_rhs: Vec<Vec<StreamData>>,
        lhs_attrs: Vec<String>,
        rhs_attrs: Vec<String>,
        times: Vec<(usize, usize)>,
        expected: Vec<(usize, Vec<Vec<i32>>)>,
        new_order_lhs: Vec<usize>,
        new_order_rhs: Vec<usize>,
        time_interval: TimeInterval,
    ) {
        let (send, recv) = std::sync::mpsc::channel();
        let send = std::sync::Arc::new(std::sync::Mutex::new(send));
        timely::execute(timely::Config::process(NUM_WORKERS), move |worker| {
            let send = send.lock().unwrap().clone();

            let ((mut lhs_input, cap_lhs), (mut rhs_input, cap_rhs), (mut time_input, cap_time), _probe) = worker
                .dataflow::<usize, _, _>(|scope| {
                    let (time_input, time_stream) = scope.new_unordered_input::<TimeFlowValues>();
                    let (lhs_input, lhs_stream) = scope.new_unordered_input::<Record>();
                    let (rhs_input, rhs_stream) = scope.new_unordered_input::<Record>();

                    let (_attr, output) = lhs_stream
                        .distribute(&mut 0, true, &lhs_attrs, &rhs_attrs).1
                        .until(&mut 0, rhs_stream.distribute(&mut 0, false, &lhs_attrs, &rhs_attrs).1,
                            time_stream.broadcast(), &lhs_attrs, &rhs_attrs, time_interval, false).1
                        .exhaust(&mut 0, time_stream.broadcast(), vec![], default_options());

                    let probe = output.probe();
                    output.capture_into(send);
                    (lhs_input, rhs_input, time_input, probe)
                });

            // give a list of pairs tp -> ts
            let max_length = max(max(data_rhs.len(), data_lhs.len()), times.len());
            for round in 0usize..max_length {
                if worker.index() == 0 {
                    if round < times.len() {
                        let new_index = new_order_lhs[round];
                        let org_tp = times[new_index].0;
                        let org_ts = times[new_index].1;

                        time_input.session(cap_time.delayed(&org_tp)).give(Timestamp(org_ts));
                    }

                    if round < data_lhs.len() {
                        // let get new index and original time point
                        let new_index = new_order_lhs[round];
                        let org_tp = times[new_index].0;

                        let prod = org_tp.clone();
                        let tmp_cap = cap_lhs.delayed(&prod);

                        data_lhs.get(new_index).unwrap().iter().for_each(|data| {
                            match data.clone() {
                                StreamData::D(_ts, data) => {
                                    let event = Record::Data(
                                        true,
                                        data.iter().cloned().map(|arg| (Int(arg))).collect(),
                                    );
                                    lhs_input.session(tmp_cap.clone()).give(event);
                                }
                                StreamData::M(_ts, bool) => {
                                    let event = Record::MetaData(bool.clone(), bool);
                                    lhs_input.session(tmp_cap.clone()).give(event);
                                }
                            };
                        });
                    }

                    if round < data_rhs.len() {
                        let new_index = new_order_rhs[round];
                        let org_tp = times[new_index].0;

                        let prod = org_tp.clone();
                        let tmp_cap = cap_rhs.delayed(&prod);

                        data_rhs.get(new_index).unwrap().iter().for_each(|data| {
                            match data.clone() {
                                StreamData::D(_ts, data) => {
                                    let event = Record::Data(
                                        true,
                                        data.iter().cloned().map(|arg| (Int(arg))).collect(),
                                    );
                                    rhs_input.session(tmp_cap.clone()).give(event);
                                }
                                StreamData::M(_ts, bool) => {
                                    let event = Record::MetaData(bool.clone(), bool);
                                    rhs_input.session(tmp_cap.clone()).give(event);
                                }
                            };
                        });
                    }
                }
                worker.step();
            }

            let prod = max_length.clone();
            let tmp_cap = cap_lhs.delayed(&prod);
            lhs_input
                .session(tmp_cap)
                .give(Record::MetaData(false, false));

            let prod = max_length.clone();
            let tmp_cap1 = cap_rhs.delayed(&prod);
            rhs_input
                .session(tmp_cap1)
                .give(Record::MetaData(false, false));
            worker.step();
        }).unwrap();

        let mut actual_res: Vec<_> = recv
            .extract()
            .iter()
            .cloned()
            .map(|(time, tuples)| (time, tuples))
            .collect();

        //Vector of (timepoint, Vector of (timestamp, data))
        let mut expected_res = expected
            .into_iter()
            .map(|(timepoint, tuples)| {
                // process tuples
                let mut tmp_vec_in = vec![];
                for tup in tuples.iter() {
                    let mut tmp_vec = vec![];
                    for t in tup {
                        tmp_vec.push(Int(*t))
                    }

                    tmp_vec_in.push(Record::Data(true, tmp_vec));
                }
                (timepoint, tmp_vec_in)
            })
            .collect();

        sort_operator_result(&mut actual_res);
        sort_operator_result(&mut expected_res);
        assert_eq!(expected_res, actual_res);
    }

    fn test_projection(
        data: Vec<Vec<(usize, Vec<i32>)>>,
        attrs: Vec<String>,
        unwanted_attrs: Vec<String>,
        times: Vec<(usize, usize)>,
        expected: Vec<(usize, Vec<Vec<i32>>)>,
    ) {
        let (send, recv) = std::sync::mpsc::channel();
        let send = std::sync::Arc::new(std::sync::Mutex::new(send));

        timely::execute(timely::Config::process(NUM_WORKERS), move |worker| {
            let send = send.lock().unwrap().clone();

            let (mut input, probe) = worker.dataflow::<usize, _, _>(|scope| {
                let (input, stream) = scope.new_input::<Record>();
                let (_attrs, output) = stream.projection(&mut 0, &attrs, &unwanted_attrs);

                let probe = output.probe();
                output.capture_into(send);

                (input, probe)
            });

            for round in 0usize..data.len() {
                if worker.index() == 0 {
                    data.get(round).unwrap().iter().for_each(|tuple| {
                        let (_ts, tup) = tuple.clone();
                        let event = Record::Data(
                            true,
                            tup.iter().cloned().map(|arg| (Int(arg))).collect(),
                        );
                        input.send(event);
                    });
                }

                let new_time = times[round].0;
                input.advance_to(new_time);

                worker.step_while(|| probe.less_than(input.time()));
            }
        })
            .unwrap();

        let mut actual_res: Vec<_> = recv.extract().iter().cloned().map(|(time, tuples)| (time, tuples)).collect();

        let mut expected_res = expected.into_iter().map(|(timepoint, tuples)| {
                // process tuples
                let mut tmp_vec_in = vec![];
                for tup in tuples.iter() {
                    let mut tmp_vec = vec![];
                    for t in tup {
                        tmp_vec.push(Int(*t))
                    }
                    tmp_vec_in.push(Record::Data(true, tmp_vec));
                }
                (timepoint, tmp_vec_in)
            }).collect();

        sort_operator_result(&mut actual_res);
        sort_operator_result(&mut expected_res);

        // Printing results:
        println!("\nExpected:");
        print_operator_result(&expected_res);
        println!("\nActual:");
        print_operator_result(&actual_res);

        assert_eq!(expected_res, actual_res);
    }

    fn test_temporal_reordered(
        join_var: i32,
        data_lhs: Vec<Vec<StreamData>>,
        data_rhs: Vec<Vec<StreamData>>,
        lhs_attrs: Vec<String>,
        rhs_attrs: Vec<String>,
        times: Vec<(usize, usize)>,
        expected: Vec<(usize, Vec<(usize, Vec<i32>)>)>,
        new_order: fn(Vec<usize>) -> Vec<usize>,
        time_interval: TimeInterval,
    ) {
        let (send, recv) = std::sync::mpsc::channel();
        let send = std::sync::Arc::new(std::sync::Mutex::new(send));
        timely::execute(timely::Config::process(NUM_WORKERS), move |worker| {
            let send = send.lock().unwrap().clone();

            let ((mut lhs_input, cap_lhs), (mut rhs_input, cap_rhs), (mut time_input, cap_time), _probe) = worker
                .dataflow::<usize, _, _>(|scope| {
                    let (time_input, time_stream) = scope.new_unordered_input::<TimeFlowValues>();
                    let (lhs_input, lhs_stream) = scope.new_unordered_input::<Record>();
                    let (rhs_input, rhs_stream) = scope.new_unordered_input::<Record>();

                    let (_attr, output) = match join_var {
                        0 => {
                            lhs_stream
                                .distribute(&mut 0, true, &lhs_attrs, &rhs_attrs)
                                .1
                                .since(
                                    &mut 0,
                                    rhs_stream.distribute(&mut 0, false, &lhs_attrs, &rhs_attrs).1,
                                    time_stream.broadcast(),
                                    &lhs_attrs,
                                    &rhs_attrs,
                                    time_interval,
                                    false
                                )
                                .1
                                .exhaust(&mut 0, time_stream.broadcast(),vec![], default_options())
                        }
                        1 => lhs_stream
                            .distribute(&mut 0, true, &lhs_attrs, &rhs_attrs).1.until(
                                &mut 0,
                                rhs_stream.distribute(&mut 0, false, &lhs_attrs, &rhs_attrs).1,
                                time_stream.broadcast(),
                                &lhs_attrs,
                                &rhs_attrs,
                                time_interval,
                                false
                            ).1
                            .exhaust(&mut 0, time_stream.broadcast(),vec![], default_options()),
                        _ => panic!(),
                    };

                    let probe = output.probe();
                    output.capture_into(send);
                    (lhs_input, rhs_input, time_input, probe)
                });

            // New order
            let mut lhs_inds = Vec::new();
            for i in 0..data_lhs.len() {
                lhs_inds.push(i);
            }
            let new_order_lhs = new_order(lhs_inds.clone());

            let mut rhs_inds = Vec::new();
            for i in 0..data_rhs.len() {
                rhs_inds.push(i);
            }
            let new_order_rhs = new_order(rhs_inds.clone());

            // give a list of pairs tp -> ts
            let max_length = max(data_rhs.len(), data_lhs.len());
            for round in 0usize..max_length {
                if worker.index() == 0 {
                    if round < times.len() {
                        let org_tp = times[round].0;
                        let org_ts = times[round].1;
                        time_input.session(cap_time.delayed(&org_tp)).give(Timestamp(org_ts));
                    }

                    if round < data_lhs.len() {
                        // let get new index and original time point
                        let new_index = new_order_lhs[round];
                        let org_tp = times[new_index].0;

                        let prod = org_tp.clone();
                        let tmp_cap = cap_lhs.delayed(&prod);

                        data_lhs.get(new_index).unwrap().iter().for_each(|data| {
                            match data.clone() {
                                StreamData::D(_ts, data) => {
                                    let event = Record::Data(
                                        true,
                                        data.iter().cloned().map(|arg| (Int(arg))).collect(),
                                    );
                                    lhs_input.session(tmp_cap.clone()).give(event);
                                }
                                StreamData::M(_ts, bool) => {
                                    let event = Record::MetaData(bool.clone(), bool);
                                    lhs_input.session(tmp_cap.clone()).give(event);
                                }
                            };
                        });
                    }

                    if round < data_rhs.len() {
                        let new_index = new_order_rhs[round];
                        let org_tp = times[new_index].0;

                        let prod = org_tp.clone();
                        let tmp_cap = cap_rhs.delayed(&prod);

                        data_rhs.get(new_index).unwrap().iter().for_each(|data| {
                            match data.clone() {
                                StreamData::D(_ts, data) => {
                                    let event = Record::Data(
                                        true,
                                        data.iter().cloned().map(|arg| (Int(arg))).collect(),
                                    );
                                    rhs_input.session(tmp_cap.clone()).give(event);
                                }
                                StreamData::M(_ts, bool) => {
                                    let event = Record::MetaData(bool.clone(), bool);
                                    rhs_input.session(tmp_cap.clone()).give(event);
                                }
                            };
                        });
                    }
                }
                worker.step();
            }

            // if until send end-of-stream
                let prod = max_length.clone();
                let tmp_cap = cap_lhs.delayed(&prod);
                lhs_input
                    .session(tmp_cap)
                    .give(Record::MetaData(false, false));

                let prod = max_length.clone();
                let tmp_cap1 = cap_rhs.delayed(&prod);
                rhs_input
                    .session(tmp_cap1)
                    .give(Record::MetaData(false, false));

        })
            .unwrap();

        let mut actual_res: Vec<_> = recv
            .extract()
            .iter()
            .cloned()
            .map(|(time, tuples)| (time, tuples))
            .collect();

        //Vector of (timepoint, Vector of (timestamp, data))
        let mut expected_res = expected
            .into_iter()
            .map(|(timepoint, tuples)| {
                // process tuples
                let mut tmp_vec_in = vec![];
                for (_ts, tup) in tuples.iter() {
                    let mut tmp_vec = vec![];
                    for t in tup {
                        tmp_vec.push(Int(*t))
                    }

                    tmp_vec_in.push(Record::Data(true, tmp_vec));
                }
                (timepoint, tmp_vec_in)
            }).collect();

        sort_operator_result(&mut actual_res);
        sort_operator_result(&mut expected_res);

        // Printing results:
        println!("\nExpected:");
        print_operator_result(&expected_res);
        println!("\nActual:");
        print_operator_result(&actual_res);

        assert_eq!(expected_res, actual_res);
    }

    fn test_join(
        join_var: i32,
        data_lhs: Vec<Vec<(usize, Vec<i32>)>>,
        data_rhs: Vec<Vec<(usize, Vec<i32>)>>,
        lhs_attrs: Vec<String>,
        rhs_attrs: Vec<String>,
        times: Vec<usize>,
        expected: Vec<(usize, Vec<(usize, Vec<i32>)>)>,
    ) {
        let (send, recv) = std::sync::mpsc::channel();
        let send = std::sync::Arc::new(std::sync::Mutex::new(send));
        timely::execute(timely::Config::process(NUM_WORKERS), move |worker| {
            let send = send.lock().unwrap().clone();

            let (mut lhs_input, mut rhs_input, probe) = worker.dataflow::<usize, _, _>(|scope| {
                let (lhs_input, lhs_stream) = scope.new_input::<Record>();
                let (rhs_input, rhs_stream) = scope.new_input::<Record>();

                let (_attr, output) = match join_var {
                    0 => lhs_stream.join(&mut 0, &rhs_stream, &lhs_attrs, &rhs_attrs),
                    1 => lhs_stream.union(&mut 0, &rhs_stream, &lhs_attrs, &rhs_attrs),
                    2 => lhs_stream.anti_join(&mut 0, &rhs_stream, &lhs_attrs, &rhs_attrs),
                    _ => panic!(),
                };

                let probe = output.probe();
                output.capture_into(send);
                (lhs_input, rhs_input, probe)
            });

            let max_length = max(data_rhs.len(), data_lhs.len());
            for round in 0usize..max_length {
                if worker.index() == 0 {
                    if round < data_lhs.len() {
                        data_lhs.get(round).unwrap().iter().for_each(|(_t, data)| {
                            let event = Record::Data(
                                true,
                                data.iter().cloned().map(|arg| (Int(arg))).collect(),
                            );
                            lhs_input.send(event);
                        });
                    }

                    if round < data_rhs.len() {
                        data_rhs.get(round).unwrap().iter().for_each(|(_t, data)| {
                            let event = Record::Data(
                                true,
                                data.iter().cloned().map(|arg| (Int(arg))).collect(),
                            );
                            rhs_input.send(event);
                        });
                    }
                }

                let new_time = times[round];
                if round < data_rhs.len() {
                    lhs_input.advance_to(new_time);
                }

                if round < data_rhs.len() {
                    rhs_input.advance_to(new_time);
                }

                if data_rhs.len() <= data_lhs.len() {
                    worker.step_while(|| probe.less_than(rhs_input.time()));
                } else {
                    worker.step_while(|| probe.less_than(lhs_input.time()));
                }
            }
        }).unwrap();

        let mut actual_res: Vec<_> = recv
            .extract()
            .iter()
            .cloned()
            .map(|(time, tuples)| (time, tuples))
            .collect();

        //Vector of (timepoint, Vector of (timestamp, data))
        let mut expected_res = expected
            .into_iter()
            .map(|(timepoint, tuples)| {
                // process tuples
                let mut tmp_vec_in = vec![];
                for (_ts, tup) in tuples.iter() {
                    let mut tmp_vec = vec![];
                    for t in tup {
                        tmp_vec.push(Int(*t))
                    }

                    tmp_vec_in.push(Record::Data(true, tmp_vec));
                }
                (timepoint, tmp_vec_in)
            })
            .collect();

        sort_operator_result(&mut actual_res);
        sort_operator_result(&mut expected_res);

        // Printing results:
        println!("\nExpected:");
        print_operator_result(&expected_res);
        println!("\nActual:");
        print_operator_result(&actual_res);

        assert_eq!(expected_res, actual_res);
    }

    fn test_join1(
        join_var: i32,
        data_lhs: Vec<Vec<(usize, Vec<String>)>>,
        data_rhs: Vec<Vec<(usize, Vec<String>)>>,
        lhs_attrs: Vec<String>,
        rhs_attrs: Vec<String>,
        times: Vec<usize>,
        expected: Vec<(usize, Vec<(usize, Vec<String>)>)>,
    ) {
        let (send, recv) = std::sync::mpsc::channel();
        let send = std::sync::Arc::new(std::sync::Mutex::new(send));
        timely::execute(timely::Config::process(NUM_WORKERS), move |worker| {
            let send = send.lock().unwrap().clone();

            let (mut lhs_input, mut rhs_input, probe) = worker.dataflow::<usize, _, _>(|scope| {
                let (lhs_input, lhs_stream) = scope.new_input::<Record>();
                let (rhs_input, rhs_stream) = scope.new_input::<Record>();

                let (_attr, output) = match join_var {
                    0 => lhs_stream.join(&mut 0, &rhs_stream, &lhs_attrs, &rhs_attrs),
                    1 => lhs_stream.union(&mut 0, &rhs_stream, &lhs_attrs, &rhs_attrs),
                    2 => lhs_stream.anti_join(&mut 0, &rhs_stream, &lhs_attrs, &rhs_attrs),
                    _ => panic!(),
                };

                let probe = output.probe();
                output.capture_into(send);
                (lhs_input, rhs_input, probe)
            });

            let max_length = max(data_rhs.len(), data_lhs.len());
            for round in 0usize..max_length {
                if worker.index() == 0 {
                    if round < data_lhs.len() {
                        data_lhs.get(round).unwrap().iter().for_each(|(_t, data)| {
                            let event = Record::Data(
                                true,
                                data.iter().cloned().map(|arg| (Str(arg))).collect(),
                            );
                            lhs_input.send(event);
                        });
                    }

                    if round < data_rhs.len() {
                        data_rhs.get(round).unwrap().iter().for_each(|(_t, data)| {
                            let event = Record::Data(
                                true,
                                data.iter().cloned().map(|arg| (Str(arg))).collect(),
                            );
                            rhs_input.send(event);
                        });
                    }
                }

                let new_time = times[round];
                if round < data_rhs.len() {
                    lhs_input.advance_to(new_time);
                }

                if round < data_rhs.len() {
                    rhs_input.advance_to(new_time);
                }

                if data_rhs.len() <= data_lhs.len() {
                    worker.step_while(|| probe.less_than(rhs_input.time()));
                } else {
                    worker.step_while(|| probe.less_than(lhs_input.time()));
                }
            }
        })
            .unwrap();

        let mut actual_res: Vec<_> = recv
            .extract()
            .iter()
            .cloned()
            .map(|(time, tuples)| (time, tuples))
            .collect();

        //Vector of (timepoint, Vector of (timestamp, data))
        let mut expected_res = expected
            .into_iter()
            .map(|(timepoint, tuples)| {
                // process tuples
                let mut tmp_vec_in = vec![];
                for (_ts, tup) in tuples.iter() {
                    let mut tmp_vec = vec![];
                    for t in tup {
                        tmp_vec.push(Str(t.clone()))
                    }

                    tmp_vec_in.push(Record::Data(true, tmp_vec));
                }
                (timepoint, tmp_vec_in)
            })
            .collect();

        sort_operator_result(&mut actual_res);
        sort_operator_result(&mut expected_res);

        // Printing results:
        println!("\nExpected:");
        print_operator_result(&expected_res);
        println!("\nActual:");
        print_operator_result(&actual_res);

        assert_eq!(expected_res, actual_res);
    }

    fn test_equal(
        data: Vec<Vec<(usize, Vec<i32>)>>,
        attrs: Vec<String>,
        target: String,
        new_attr: String,
        times : Vec<(usize, usize)>,
        expected: Vec<(usize, Vec<(usize, Vec<i32>)>)>,
    ) {
        let (send, recv) = std::sync::mpsc::channel();
        let send = std::sync::Arc::new(std::sync::Mutex::new(send));
        timely::execute(timely::Config::process(1), move |worker| {
            let send = send.lock().unwrap().clone();

            let (mut input, _probe) = worker.dataflow::<usize, _, _>(|scope| {
                let (input, stream) = scope.new_input::<Record>();

                let (_attr, output) = stream.equality(&mut 0, &attrs, &target, &new_attr);

                let probe = output.probe();
                output.capture_into(send);
                (input, probe)
            });

            //let max_length = max(data_rhs.len(), data_lhs.len());
            for (round, tp) in times.clone() {
                if worker.index() == 0 {
                    data.get(round).unwrap().iter().for_each(|(_, data)| {
                        let event = Record::Data(
                            true,
                            data.iter().cloned().map(|arg| (Int(arg))).collect(),
                        );
                        input.send(event);
                    });
                }

                input.advance_to(tp);

                worker.step();
            }
        }).unwrap();

        let mut actual_res: Vec<_> = recv
            .extract()
            .iter()
            .cloned()
            .map(|(time, tuples)| (time, tuples))
            .collect();

        //Vector of (timepoint, Vector of (timestamp, data))
        let mut expected_res = expected
            .into_iter()
            .map(|(timepoint, tuples)| {
                // process tuples
                let mut tmp_vec_in = vec![];
                for (_ts, tup) in tuples.iter() {
                    let mut tmp_vec = vec![];
                    for t in tup {
                        tmp_vec.push(Int(*t))
                    }

                    tmp_vec_in.push(Record::Data(true, tmp_vec));
                }
                (timepoint, tmp_vec_in)
            })
            .collect();

        sort_operator_result(&mut actual_res);
        sort_operator_result(&mut expected_res);

        // Printing results:
        println!("\nExpected:");
        print_operator_result(&expected_res);
        println!("\nActual:");
        print_operator_result(&actual_res);

        assert_eq!(expected_res, actual_res);
    }

    fn test_next_prev_reordered(
        join_var: i32,
        data_lhs: Vec<Vec<StreamData>>,
        lhs_attrs: Vec<String>,
        times: Vec<(usize, usize)>,
        expected: Vec<(usize, Vec<(usize, Vec<i32>)>)>,
        new_order: fn(Vec<usize>) -> Vec<usize>,
        time_interval: TimeInterval,
    ) {
        let (send, recv) = std::sync::mpsc::channel();
        let send = std::sync::Arc::new(std::sync::Mutex::new(send));
        timely::execute(timely::Config::process(NUM_WORKERS), move |worker| {
            let send = send.lock().unwrap().clone();

            let ((mut lhs_input, cap_lhs), (mut time_input, cap_time), _probe) = worker
                .dataflow::<usize, _, _>(|scope| {
                    let (time_input, time_stream) = scope.new_unordered_input::<TimeFlowValues>();
                    let (lhs_input, lhs_stream) = scope.new_unordered_input::<Record>();

                    let (_attr, output) = match join_var {
                        0 => {
                            lhs_stream
                                .distribute(&mut 0, true, &lhs_attrs, &vec![]).1
                                .next(&mut 0, &time_stream.broadcast(), &lhs_attrs, time_interval).1
                                .exhaust(&mut 0, time_stream.broadcast(),vec![], default_options())
                        }
                        1 => {
                            lhs_stream
                                .distribute(&mut 0, true, &lhs_attrs, &vec![]).1
                                .prev(&mut 0, &time_stream.broadcast(), &lhs_attrs, time_interval).1
                                .exhaust(&mut 0, time_stream.broadcast(),vec![], default_options())
                        }
                        _ => panic!(),
                    };

                    let probe = output.probe();
                    output.capture_into(send);
                    (lhs_input, time_input, probe)
                });

            // New order
            let mut lhs_inds = Vec::new();
            for i in 0..data_lhs.len() {
                lhs_inds.push(i);
            }
            let new_order_lhs = new_order(lhs_inds.clone());

            // give a list of pairs tp -> ts
            let max_length = data_lhs.len();
            for round in 0usize..max_length {
                if worker.index() == 0 {
                    if round < data_lhs.len() {
                        // let get new index and original time point
                        let new_index = new_order_lhs[round];
                        let org_tp = times[new_index].0;

                        time_input.session(cap_time.delayed(&new_index)).give(Timestamp(times[new_index].1));


                        let prod = org_tp.clone();
                        let tmp_cap = cap_lhs.delayed(&prod);

                        data_lhs.get(new_index).unwrap().iter().for_each(|data| {
                            match data.clone() {
                                StreamData::D(_ts, data) => {
                                    let event = Record::Data(
                                        true,
                                        data.iter().cloned().map(|arg| (Int(arg))).collect(),
                                    );
                                    lhs_input.session(tmp_cap.clone()).give(event);
                                }
                                StreamData::M(_ts, bool) => {
                                    let event = Record::MetaData(bool.clone(), bool);
                                    lhs_input.session(tmp_cap.clone()).give(event);
                                }
                            };
                        });
                    }
                }
                worker.step();
            }

            let tmp_cap = cap_lhs.delayed(&max_length.clone());
            lhs_input
                .session(tmp_cap)
                .give(Record::MetaData(false, false));

        }).unwrap();

        let mut actual_res: Vec<_> = recv
            .extract()
            .iter()
            .cloned()
            .map(|(time, tuples)| (time, tuples))
            .collect();

        //Vector of (timepoint, Vector of (timestamp, data))
        let mut expected_res = expected
            .into_iter()
            .map(|(timepoint, tuples)| {
                // process tuples
                let mut tmp_vec_in = vec![];
                for (_ts, tup) in tuples.iter() {
                    let mut tmp_vec = vec![];
                    for t in tup {
                        tmp_vec.push(Int(*t))
                    }

                    tmp_vec_in.push(Record::Data(true, tmp_vec));
                }
                (timepoint, tmp_vec_in)
            })
            .collect();

        sort_operator_result(&mut actual_res);
        sort_operator_result(&mut expected_res);

        // Printing results:
        println!("\nExpected:");
        print_operator_result(&expected_res);
        println!("\nActual:");
        print_operator_result(&actual_res);

        assert_eq!(expected_res, actual_res);
    }


    fn test_next_prev_property(
        join_var: i32,
        data_lhs: Vec<Vec<StreamData>>,
        lhs_attrs: Vec<String>,
        times: Vec<(usize, usize)>,
        expected: Vec<(usize, Vec<(usize, Vec<i32>)>)>,
        new_order: Vec<usize>,
        time_interval: TimeInterval,
    ) {
        let (send, recv) = std::sync::mpsc::channel();
        let send = std::sync::Arc::new(std::sync::Mutex::new(send));
        timely::execute(timely::Config::process(NUM_WORKERS), move |worker| {
            let send = send.lock().unwrap().clone();

            let ((mut lhs_input, cap_lhs), (mut time_input, cap_time), _probe) = worker
                .dataflow::<usize, _, _>(|scope| {
                    let (time_input, time_stream) = scope.new_unordered_input::<TimeFlowValues>();
                    let (lhs_input, lhs_stream) = scope.new_unordered_input::<Record>();

                    let (_attr, output) = match join_var {
                        0 => {
                            lhs_stream
                                .distribute(&mut 0, true, &lhs_attrs, &vec![]).1
                                .next(&mut 0, &time_stream.broadcast(), &lhs_attrs, time_interval).1
                                .exhaust(&mut 0, time_stream.broadcast(),vec![], default_options())
                        }
                        1 => {
                            lhs_stream
                                .distribute(&mut 0, true, &lhs_attrs, &vec![]).1
                                .prev(&mut 0, &time_stream.broadcast(), &lhs_attrs, time_interval).1
                                .exhaust(&mut 0, time_stream.broadcast(),vec![], default_options())
                        }
                        _ => panic!(),
                    };

                    let probe = output.probe();
                    output.capture_into(send);
                    (lhs_input, time_input, probe)
                });

            // give a list of pairs tp -> ts
            let max_length = data_lhs.len();
            for round in 0usize..max_length {
                if worker.index() == 0 {
                    if round < data_lhs.len() {
                        // let get new index and original time point
                        let new_index = new_order[round];
                        let org_tp = times[new_index].0;

                        time_input.session(cap_time.delayed(&new_index)).give(Timestamp(times[new_index].1));

                        let tmp_cap = cap_lhs.delayed(&org_tp);

                        data_lhs.get(new_index).unwrap().iter().for_each(|data| {
                            match data.clone() {
                                StreamData::D(_ts, data) => {
                                    let event = Record::Data(
                                        true,
                                        data.iter().cloned().map(|arg| (Int(arg))).collect(),
                                    );
                                    lhs_input.session(tmp_cap.clone()).give(event);
                                }
                                StreamData::M(_ts, bool) => {
                                    let event = Record::MetaData(bool.clone(), bool);
                                    lhs_input.session(tmp_cap.clone()).give(event);
                                }
                            };
                        });
                    }
                }
                worker.step();
            }

            let tmp_cap = cap_lhs.delayed(&max_length.clone());
            lhs_input
                .session(tmp_cap)
                .give(Record::MetaData(false, false));

        }).unwrap();

        let mut actual_res: Vec<_> = recv
            .extract()
            .iter()
            .cloned()
            .map(|(time, tuples)| (time, tuples))
            .collect();

        //Vector of (timepoint, Vector of (timestamp, data))
        let mut expected_res = expected
            .into_iter()
            .map(|(timepoint, tuples)| {
                // process tuples
                let mut tmp_vec_in = vec![];
                for (_ts, tup) in tuples.iter() {
                    let mut tmp_vec = vec![];
                    for t in tup {
                        tmp_vec.push(Int(*t))
                    }

                    tmp_vec_in.push(Record::Data(true, tmp_vec));
                }
                (timepoint, tmp_vec_in)
            })
            .collect();

        sort_operator_result(&mut actual_res);
        sort_operator_result(&mut expected_res);

        // Printing results:
        println!("\nExpected:");
        print_operator_result(&expected_res);
        println!("\nActual:");
        print_operator_result(&actual_res);

        assert_eq!(expected_res, actual_res);
    }

    fn test_unary_once_eventually(
        join_var: i32,
        data_lhs: Vec<Vec<StreamData>>,
        lhs_attrs: Vec<String>,
        times: Vec<(usize, usize)>,
        expected: Vec<(usize, Vec<(usize, Vec<i32>)>)>,
        new_order: fn(Vec<usize>) -> Vec<usize>,
        time_interval: TimeInterval,
    ) {
        let (send, recv) = std::sync::mpsc::channel();
        let send = std::sync::Arc::new(std::sync::Mutex::new(send));
        timely::execute(timely::Config::process(NUM_WORKERS), move |worker| {
            let send = send.lock().unwrap().clone();

            let ((mut lhs_input, cap_lhs), (mut time_input, cap_time), _probe) = worker
                .dataflow::<usize, _, _>(|scope| {
                    let (time_input, time_stream) = scope.new_unordered_input::<TimeFlowValues>();
                    let (lhs_input, lhs_stream) = scope.new_unordered_input::<Record>();

                    let (_attr, output) = match join_var {
                        0 => {
                            lhs_stream
                                .distribute(&mut 0, true, &lhs_attrs, &vec![]).1
                                .once(&mut 0, &time_stream.broadcast(), &lhs_attrs, time_interval, false).1
                                .exhaust(&mut 0, time_stream.broadcast(),vec![], default_options())
                        }
                        1 => {
                            lhs_stream
                                .distribute(&mut 0, true, &lhs_attrs, &vec![]).1
                                .eventually(&mut 0, &time_stream.broadcast(), &lhs_attrs, time_interval, false).1
                                .exhaust(&mut 0, time_stream.broadcast(),vec![], default_options())
                        }
                        _ => panic!(),
                    };

                    let probe = output.probe();
                    output.capture_into(send);
                    (lhs_input, time_input, probe)
                });

            // New order
            let mut lhs_inds = Vec::new();
            for i in 0..data_lhs.len() {
                lhs_inds.push(i);
            }
            let new_order_lhs = new_order(lhs_inds.clone());

            // give a list of pairs tp -> ts
            let max_length = data_lhs.len();
            for round in 0usize..max_length {
                if worker.index() == 0 {
                    if round < data_lhs.len() {
                        // let get new index and original time point
                        let new_index = new_order_lhs[round];
                        let org_tp = times[new_index].0;
                        let org_ts = times[new_index].1;

                        let prod = org_tp.clone();
                        time_input.session(cap_time.delayed(&org_tp)).give(Timestamp(org_ts));
                        let tmp_cap = cap_lhs.delayed(&prod);


                        data_lhs.get(new_index).unwrap().iter().for_each(|data| {
                            match data.clone() {
                                StreamData::D(_ts, data) => {
                                    let event = Record::Data(
                                        true,
                                        data.iter().cloned().map(|arg| (Int(arg))).collect(),
                                    );
                                    lhs_input.session(tmp_cap.clone()).give(event);
                                }
                                StreamData::M(_ts, bool) => {
                                    let event = Record::MetaData(bool.clone(), bool);
                                    lhs_input.session(tmp_cap.clone()).give(event);
                                }
                            };
                        });
                    }
                }
                worker.step();
            }

            let tmp_cap = cap_lhs.delayed(&max_length.clone());
            lhs_input.session(tmp_cap).give(Record::MetaData(false, false));
        }).unwrap();

        let mut actual_res: Vec<_> = recv
            .extract()
            .iter()
            .cloned()
            .map(|(time, tuples)| (time, tuples))
            .collect();

        //Vector of (timepoint, Vector of (timestamp, data))
        let mut expected_res = expected
            .into_iter()
            .map(|(timepoint, tuples)| {
                // process tuples
                let mut tmp_vec_in = vec![];
                for (_ts, tup) in tuples.iter() {
                    let mut tmp_vec = vec![];
                    for t in tup {
                        tmp_vec.push(Int(*t))
                    }

                    tmp_vec_in.push(Record::Data(true, tmp_vec));
                }
                (timepoint, tmp_vec_in)
            })
            .collect();

        sort_operator_result(&mut actual_res);
        sort_operator_result(&mut expected_res);

        // Printing results:
        println!("\nExpected:");
        print_operator_result(&expected_res);
        println!("\nActual:");
        print_operator_result(&actual_res);

        assert_eq!(expected_res, actual_res);
    }


    fn test_unary_eventually_special(data_lhs: Vec<Vec<StreamData>>, lhs_attrs: Vec<String>, times: Vec<(usize, usize)>, expected: Vec<(usize, Vec<(usize, Vec<i32>)>)>, new_order: Vec<usize>) {
        let (send, recv) = std::sync::mpsc::channel();
        let send = std::sync::Arc::new(std::sync::Mutex::new(send));
        timely::execute(timely::Config::process(NUM_WORKERS), move |worker| {
            let send = send.lock().unwrap().clone();
            let ((mut lhs_input, cap_lhs), _probe) = worker
                .dataflow::<usize, _, _>(|scope| {
                    let (_time_input, time_stream) = scope.new_unordered_input::<TimeFlowValues>();
                    let (lhs_input, lhs_stream) = scope.new_unordered_input::<Record>();
                    let (_attr, output) = lhs_stream
                                .distribute(&mut 0, true, &lhs_attrs, &vec![]).1
                                .eventually_zero_inf(&mut 0, &lhs_attrs).1
                                .exhaust(&mut 0, time_stream.broadcast(),vec![], default_options());
                    let probe = output.probe();
                    output.capture_into(send);
                    (lhs_input, probe)
                });

            // New order
            let new_order_lhs = new_order.clone();
            // give a list of pairs tp -> ts
            let max_length = data_lhs.len();
            for round in 0usize..max_length {
                if worker.index() == 0 {
                    if round < data_lhs.len() {
                        // let get new index and original time point
                        let new_index = new_order_lhs[round];
                        let org_tp = times[new_index].0;
                        let prod = org_tp.clone();
                        let tmp_cap = cap_lhs.delayed(&prod);
                        data_lhs.get(new_index).unwrap().iter().for_each(|data| {
                            match data.clone() {
                                StreamData::D(_ts, data) => {
                                    let event = Record::Data(
                                        true,
                                        data.iter().cloned().map(|arg| (Int(arg))).collect(),
                                    );
                                    lhs_input.session(tmp_cap.clone()).give(event);
                                }
                                StreamData::M(_ts, bool) => {
                                    let event = Record::MetaData(bool.clone(), bool);
                                    lhs_input.session(tmp_cap.clone()).give(event);
                                }
                            };
                        });
                    }
                }
                worker.step();
            }
            let tmp_cap = cap_lhs.delayed(&max_length.clone());
            lhs_input.session(tmp_cap).give(Record::MetaData(false, false));
        }).unwrap();

        let mut actual_res: Vec<_> = recv
            .extract()
            .iter()
            .cloned()
            .map(|(time, tuples)| (time, tuples))
            .collect();

        //Vector of (timepoint, Vector of (timestamp, data))
        let mut expected_res = expected
            .into_iter()
            .map(|(timepoint, tuples)| {
                // process tuples
                let mut tmp_vec_in = vec![];
                for (_ts, tup) in tuples.iter() {
                    let mut tmp_vec = vec![];
                    for t in tup {
                        tmp_vec.push(Int(*t))
                    }

                    tmp_vec_in.push(Record::Data(true, tmp_vec));
                }
                (timepoint, tmp_vec_in)
            })
            .collect();

        sort_operator_result(&mut actual_res);
        sort_operator_result(&mut expected_res);

        // Printing results:
        println!("\nExpected:");
        print_operator_result(&expected_res);
        println!("\nActual:");
        print_operator_result(&actual_res);
        assert_eq!(expected_res, actual_res);
    }

    fn test_unary_once_special(data_lhs: Vec<Vec<StreamData>>, lhs_attrs: Vec<String>, times: Vec<(usize, usize)>, expected: Vec<(usize, Vec<(usize, Vec<i32>)>)>, new_order: Vec<usize>) {
        let (send, recv) = std::sync::mpsc::channel();
        let send = std::sync::Arc::new(std::sync::Mutex::new(send));
        timely::execute(timely::Config::process(NUM_WORKERS), move |worker| {
            let send = send.lock().unwrap().clone();
            let ((mut lhs_input, cap_lhs), (mut time_input, cap_time), _probe) = worker
                .dataflow::<usize, _, _>(|scope| {
                    let (time_input, time_stream) = scope.new_unordered_input::<TimeFlowValues>();
                    let (lhs_input, lhs_stream) = scope.new_unordered_input::<Record>();
                    let (_attr, output) = lhs_stream
                        .distribute(&mut 0, true, &lhs_attrs, &vec![]).1
                        .once_zero_inf(&mut 0, &time_stream.broadcast(), &lhs_attrs).1
                        .exhaust(&mut 0, time_stream.broadcast(), vec![], default_options());
                    let probe = output.probe();
                    output.capture_into(send);
                    (lhs_input, time_input, probe)
                });

            // New order
            let new_order_lhs = new_order.clone();
            // give a list of pairs tp -> ts
            let max_length = data_lhs.len();
            for round in 0usize..max_length {
                if worker.index() == 0 {
                    if round < data_lhs.len() {
                        // let get new index and original time point
                        let new_index = new_order_lhs[round];
                        let org_tp = times[new_index].0;

                        time_input.session(cap_time.delayed(&new_index)).give(Timestamp(times[new_index].1));

                        let tmp_cap = cap_lhs.delayed(&org_tp);
                        data_lhs.get(new_index).unwrap().iter().for_each(|data| {
                            match data.clone() {
                                StreamData::D(_ts, data) => {
                                    let event = Record::Data(
                                        true,
                                        data.iter().cloned().map(|arg| (Int(arg))).collect(),
                                    );
                                    lhs_input.session(tmp_cap.clone()).give(event);
                                }
                                StreamData::M(_ts, bool) => {
                                    let event = Record::MetaData(bool.clone(), bool);
                                    lhs_input.session(tmp_cap.clone()).give(event);
                                }
                            };
                        });
                    }
                }
                worker.step();
            }
            let t_cap = cap_lhs.delayed(&times.len().clone());
            let t_cap1 = cap_time.delayed(&times.len().clone());
            lhs_input.session(t_cap).give(Record::MetaData(false, false));
            time_input.session(t_cap1).give(EOS);
        }).unwrap();

        let mut actual_res: Vec<_> = recv
            .extract()
            .iter()
            .cloned()
            .map(|(time, tuples)| (time, tuples))
            .collect();

        //Vector of (timepoint, Vector of (timestamp, data))
        let mut expected_res = expected
            .into_iter()
            .map(|(timepoint, tuples)| {
                // process tuples
                let mut tmp_vec_in = vec![];
                for (_ts, tup) in tuples.iter() {
                    let mut tmp_vec = vec![];
                    for t in tup {
                        tmp_vec.push(Int(*t))
                    }

                    tmp_vec_in.push(Record::Data(true, tmp_vec));
                }
                (timepoint, tmp_vec_in)
            })
            .collect();

        sort_operator_result(&mut actual_res);
        sort_operator_result(&mut expected_res);

        // Printing results:
        println!("\nExpected:");
        print_operator_result(&expected_res);
        println!("\nActual:");
        print_operator_result(&actual_res);
        assert_eq!(expected_res, actual_res);
    }


    fn test_next_special_case(
        join_var: i32,
        data_lhs: Vec<Vec<StreamData>>,
        lhs_attrs: Vec<String>,
        times: Vec<(usize, usize)>,
        expected: Vec<(usize, Vec<(usize, Vec<i32>)>)>,
        new_order: Vec<usize>
    ) {
        let (send, recv) = std::sync::mpsc::channel();
        let send = std::sync::Arc::new(std::sync::Mutex::new(send));
        timely::execute(timely::Config::process(NUM_WORKERS), move |worker| {
            let send = send.lock().unwrap().clone();

            let ((mut lhs_input, cap_lhs), _probe) = worker
                .dataflow::<usize, _, _>(|scope| {
                    let (_time_input, time_stream) = scope.new_unordered_input::<TimeFlowValues>();
                    let (lhs_input, lhs_stream) = scope.new_unordered_input::<Record>();

                    let (_attr, output) = match join_var {
                        0 => {
                            lhs_stream
                                .distribute(&mut 0, true, &lhs_attrs, &vec![]).1
                                .next_zero_inf(&mut 0, &lhs_attrs).1
                                .exhaust(&mut 0, time_stream.broadcast(),vec![], default_options())
                        }
                        _ => panic!(),
                    };

                    let probe = output.probe();
                    output.capture_into(send);
                    (lhs_input, probe)
                });

            // give a list of pairs tp -> ts
            let max_length = data_lhs.len();
            for round in 0usize..max_length {
                if worker.index() == 0 {
                    if round < data_lhs.len() {
                        let new_index = new_order[round];
                        let org_tp = times[new_index].0;

                        let tmp_cap = cap_lhs.delayed(&org_tp);
                        data_lhs.get(new_index).unwrap().iter().for_each(|data| {
                            match data.clone() {
                                StreamData::D(_ts, data) => {
                                    let event = Record::Data(
                                        true,
                                        data.iter().cloned().map(|arg| (Int(arg))).collect(),
                                    );
                                    lhs_input.session(tmp_cap.clone()).give(event);
                                }
                                StreamData::M(_ts, bool) => {
                                    let event = Record::MetaData(bool.clone(), bool);
                                    lhs_input.session(tmp_cap.clone()).give(event);
                                }
                            };
                        });
                    }
                }
                worker.step();
            }

            let tmp_cap = cap_lhs.delayed(&max_length.clone());
            lhs_input.session(tmp_cap).give(Record::MetaData(false, false));
        }).unwrap();

        let mut actual_res: Vec<_> = recv.extract().iter().cloned()
            .map(|(time, tuples)| (time, tuples)).collect();

        //Vector of (timepoint, Vector of (timestamp, data))
        let mut expected_res = expected
            .into_iter()
            .map(|(timepoint, tuples)| {
                // process tuples
                let mut tmp_vec_in = vec![];
                for (_ts, tup) in tuples.iter() {
                    let mut tmp_vec = vec![];
                    for t in tup {
                        tmp_vec.push(Int(*t))
                    }
                    tmp_vec_in.push(Record::Data(true, tmp_vec));
                }
                (timepoint, tmp_vec_in)
            }).collect();

        sort_operator_result(&mut actual_res);
        sort_operator_result(&mut expected_res);

        // Printing results:
        println!("\nExpected:");
        print_operator_result(&expected_res);
        println!("\nActual:");
        print_operator_result(&actual_res);

        assert_eq!(expected_res, actual_res);
    }

    #[test]
    fn make_prev_special_fail() {
        let data_lhs = vec![
            vec![StreamData::M(0,true)],
            vec![StreamData::D(10, vec![1,2])],
            vec![StreamData::M(20, true)],
            vec![StreamData::D(30, vec![2,3])],
            vec![StreamData::M(40, true)],
        ];

        let expected = vec![
            (2, vec![(20, vec![1, 2])]),
            (4, vec![(40, vec![2, 3])])
        ];

        let times = vec![(0,0), (1,10), (2,20), (3,30), (4,40)];

        let vecs = &[0, 1, 2, 3, 4];
        let permutations = permutations(vecs);

        let lhs_attrs: Vec<String> = vec!["x".into(), "y".into()];
        for rhs in permutations.clone() {
            test_prev_special_case(
                data_lhs.clone(),
                lhs_attrs.clone(),
                times.clone(),
                expected.clone(),
                rhs.clone(),
            );
        }
    }

    fn test_prev_special_case(
        data_lhs: Vec<Vec<StreamData>>,
        lhs_attrs: Vec<String>,
        times: Vec<(usize, usize)>,
        expected: Vec<(usize, Vec<(usize, Vec<i32>)>)>,
        new_order: Vec<usize>) {
        let (send, recv) = std::sync::mpsc::channel();
        let send = std::sync::Arc::new(std::sync::Mutex::new(send));
timely::execute(timely::Config::process(NUM_WORKERS), move |worker| {
            let send = send.lock().unwrap().clone();
            let ((mut lhs_input, cap_lhs), (mut time_input, time_cap), _probe) = worker
                .dataflow::<usize, _, _>(|scope| {
                    let (time_input, time_stream) = scope.new_unordered_input::<TimeFlowValues>();
                    let (lhs_input, lhs_stream) = scope.new_unordered_input::<Record>();

                    let (_attr, output) = lhs_stream
                                .distribute(&mut 0, true, &lhs_attrs, &vec![]).1
                                .prev_zero_inf(&mut 0, &time_stream.broadcast(), &lhs_attrs).1
                                //.prev_zero_inf(&mut 0, &time_stream.broadcast(), &lhs_attrs).1
                                .exhaust(&mut 0, time_stream.broadcast(),vec![], default_options());

                    let probe = output.probe();
                    output.capture_into(send);
                    (lhs_input, time_input, probe)
                });

            // give a list of pairs tp -> ts

            for round in 0usize..data_lhs.len() {
                if worker.index() == 0 {
                    let new_index = new_order[round];
                    let org_tp = times[new_index].0;

                    time_input.session(time_cap.delayed(&new_index)).give(Timestamp(times[new_index].1));

                    let tmp_cap = cap_lhs.delayed(&org_tp);
                    if let Some(x) = data_lhs.get(new_index) {
                        x.iter().for_each(|data| {
                            match data.clone() {
                                StreamData::D(_ts, data) => {
                                    let event = Record::Data(
                                        true,
                                        data.iter().cloned().map(|arg| (Int(arg))).collect(),
                                    );
                                    lhs_input.session(tmp_cap.clone()).give(event);
                                }
                                StreamData::M(_ts, bool) => {
                                    let event = Record::MetaData(bool.clone(), bool);
                                    lhs_input.session(tmp_cap.clone()).give(event);
                                }
                            };
                        });
                    }
                }
                worker.step();
            }

            let eos_tp = max(data_lhs.len(), times.len());
            let tmp_cap = cap_lhs.delayed(&eos_tp.clone());
            time_input.session(time_cap).give(EOS);
            lhs_input.session(tmp_cap).give(Record::MetaData(false, false));
        }).unwrap();

        let mut actual_res: Vec<_> = recv.extract().iter().cloned()
            .map(|(time, tuples)| (time, tuples)).collect();

        //Vector of (timepoint, Vector of (timestamp, data))
        let mut expected_res = expected
            .into_iter()
            .map(|(timepoint, tuples)| {
                // process tuples
                let mut tmp_vec_in = vec![];
                for (_ts, tup) in tuples.iter() {
                    let mut tmp_vec = vec![];
                    for t in tup {
                        tmp_vec.push(Int(*t))
                    }
                    tmp_vec_in.push(Record::Data(true, tmp_vec));
                }
                (timepoint, tmp_vec_in)
            }).collect();

        sort_operator_result(&mut actual_res);
        sort_operator_result(&mut expected_res);

        // Printing results:
        println!("\nExpected:");
        print_operator_result(&expected_res);
        println!("\nActual:");
        print_operator_result(&actual_res);

        assert_eq!(expected_res, actual_res);
    }

    // helper functions ----------------------------------------------------------------------------
    fn print_output_result(res: Vec<(usize, Vec<Arg>)>) {
        println!("Output: ");
        for (tp, rs) in res.clone() {
            print!("TP: {}  ", tp);
            print_rec(rs.clone());
        }
        println!();
    }

    fn print_rec(rec: Vec<Arg>) {
        for r in rec.clone() {
            print!("{}  ", r);
        }
        println!()
    }

    fn record_eq(rec1: Vec<Arg>, rec2: Vec<Arg>) -> bool {
        if rec1.len() != rec2.len() {
            return false;
        }
        for i in 0..rec1.len() {
            if rec1[i] != rec2[i] {
                return false;
            }
        }
        return true;
    }

    fn ts_vec_eq(
        res: Vec<(usize, usize, Vec<Arg>)>,
        expected: Vec<(usize, usize, Vec<Arg>)>,
    ) -> bool {
        if res.len() != expected.len() {
            return false;
        }

        for i in 0..res.len() {
            if res[i].0 != res[i].0 {
                return false;
            }
            if res[i].1 != res[i].1 {
                return false;
            }
            if !record_eq(res[i].2.clone(), res[i].2.clone()) {
                return false;
            }
        }
        return true;
    }

    fn print_vec(vec1: Vec<Intervals>) {
        print!("[");
        for v in vec1.iter() {
            print!("{} ", v);
        }
        println!("]")
    }

    fn sort_operator_result(res: &mut Vec<(usize, Vec<Record>)>) {
        res.sort();
        for (_ts, tuples) in res.iter_mut() {
            tuples.sort();
        }
    }

    fn print_operator_result(res: &Vec<(usize, Vec<Record>)>) {
        res.iter().for_each(|(time, tuples)| {
            println!("Tp {}: ", time.to_string());
            for tuple in tuples {
                match tuple {
                    Record::Data(_, rec) => {
                        rec.into_iter().for_each(|d| {
                            print!(" {}", d.to_string());
                        });
                    }
                    _ => {}
                };
                println!();
            }
        });
    }

    fn interval_eq(int1: Intervals, int2: Intervals) -> bool {
        let res = match int1 {
            Literal(a) => match int2 {
                Literal(a_) => a == a_,
                Interval(_, _) => false,
            },
            Interval(a, b) => match int2 {
                Literal(_) => false,
                Interval(a_, b_) => a == a_ && b == b_,
            },
        };

        return res;
    }

    fn interval_vec_eq(vec1: Vec<Intervals>, vec2: Vec<Intervals>) -> bool {
        let l1 = vec1.len();
        let l2 = vec2.len();

        if l1 != l2 {
            return false;
        };

        for i in 0..l1 {
            if !interval_eq(vec1[i].clone(), vec2[i].clone()) {
                return false;
            }
        }

        return true;
    }

    #[test]
    fn restore_original_order_test() {
        // b a c
        let tup = vec![Arg::Cst(Int(2)), Arg::Cst(Int(1)), Arg::Cst(Int(1))];
        let lhs_attrs = vec!["a".to_string(), "b".to_string()];
        let rhs_attrs = vec!["b".to_string(), "c".to_string()];

        let _org_order = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let (new_order, _, _) = find_common_bound_variables(lhs_attrs.clone(), rhs_attrs.clone());

        for i in new_order.clone() {
            print!(" {} ", i);
        }

        let new_tup = restore_original_order(
            tup.clone(),
            lhs_attrs.clone(),
            rhs_attrs.clone(),
            new_order.clone(),
        );

        assert_eq!(new_tup, vec![Arg::Cst(Int(1)), Arg::Cst(Int(2)), Arg::Cst(Int(1))])
    }
}
