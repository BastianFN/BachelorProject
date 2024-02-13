#![allow(dead_code)]
#[cfg(test)]
mod test {
    use std::collections::{HashMap};
    use std::vec;
    use {std, TS};

    use dataflow_constructor::operators::*;
    use parser::formula_syntax_tree::{Constant};
    use timely;
    use dataflow_constructor::observation_sequence::{ObservationSequence, TimeSeq};
    use parser::formula_syntax_tree::Constant::{Int};
    use timeunits::TimeInterval;

/*
    #[test]
    fn test_process_once_alpha_infinite() {
        let tup = vec![Int(1)];
        let tup1 = vec![Int(2)];
        let interval = TimeInterval::new(TS::new(1), TS::INFINITY);
        let mut betas: HashMap<Vec<Constant>, Vec<(usize, usize, usize, bool)>> = HashMap::new();
        let mut obs_seq = ObservationSequence::init();

        // best case, all gaps are filled
        let mut res = Vec::new();
        let mut res1 = Vec::new();
        let mut alpha_res = Vec::new();
        for i in 0..5 {
            let x = process_once_beta_infinite(tup.clone(), i, i,interval, &mut betas, &mut obs_seq);
            if !x.is_empty() {
                res.push(x);
            }

            let x1 = process_once_beta_infinite(tup1.clone(), i, i,interval, &mut betas, &mut obs_seq);
            if !x1.is_empty() {
                res1.push(x1);
            }

            let alpha = process_once_alpha_infinite(&mut betas, &mut obs_seq, interval, i);
            if !alpha.is_empty() {
                alpha_res.push(alpha);
            }
        }

        obs_seq.insert(10, 10);
        println!("{:?}", obs_seq.observations);
        let alpha = process_once_alpha_infinite(&mut betas, &mut obs_seq, interval, 10);
        if !alpha.is_empty() {
            alpha_res.push(alpha);
        }

        if let Some(v) = betas.get(&tup) {
            assert_eq!(v.clone(), vec![(0, 0, 0, false), (1, 0, 0, false), (2, 0, 0, false), (3, 0, 0, false), (4, 0, 0, false)]);
        } else { assert!(false); }
        if let Some(v) = betas.get(&tup1) {
            assert_eq!(v.clone(), vec![(0, 0, 0, false), (1, 0, 0, false), (2, 0, 0, false), (3, 0, 0, false), (4, 0, 0, false)]);
        } else { assert!(false); }
    }

    #[test]
    fn test_process_once_alpha_infinite1() {
        let tup = vec![Int(1)];
        let tup1 = vec![Int(2)];
        let interval = TimeInterval::new(TS::new(1), TS::INFINITY);
        let mut betas: HashMap<Vec<Constant>, Vec<(usize, usize, usize, bool)>> = HashMap::new();
        let mut obs_seq = ObservationSequence::init();

        // best case, all gaps are filled
        let mut res = Vec::new();
        let mut res1 = Vec::new();
        let mut alpha_res = Vec::new();
        for i in 0..5 {
            obs_seq.insert(i, i);

            let x = process_once_beta_infinite(tup.clone(), i, i, interval, &mut betas, &mut obs_seq);
            if !x.is_empty() {
                res.push(x);
            }

            let x1 = process_once_beta_infinite(tup1.clone(), i, i, interval, &mut betas, &mut obs_seq);
            if !x1.is_empty() {
                res1.push(x1);
            }

            let alpha = process_once_alpha_infinite(&mut betas, &mut obs_seq, interval, i);
            if !alpha.is_empty() {
                alpha_res.push(alpha);
            }
        }

        obs_seq.insert(5,5);
        let alpha = process_once_alpha_infinite(&mut betas, &mut obs_seq, interval, 5);
        if !alpha.is_empty() {
            alpha_res.push(alpha);
        }

        if let Some(v) = betas.get(&tup) {
            assert_eq!(v.clone(), vec![(0, 1, 5, true), (1, 2, 5, true), (2, 3, 5, true), (3, 4, 5, true), (4,5,5,true)]);
        } else { assert!(false); }
        if let Some(v) = betas.get(&tup1) {
            assert_eq!(v.clone(), vec![(0, 1, 5, true), (1, 2, 5, true), (2, 3, 5, true), (3, 4, 5, true), (4,5,5,true)]);
        } else { assert!(false); }

        let mut final_res_tup = Vec::new();
        let mut final_res_tup1 = Vec::new();
        alpha_res.iter().for_each(|x| {
            for (k, v) in x {
                if k.clone() == tup {
                    for val in v {
                        final_res_tup.push(*val);
                    }
                } else {
                    for val in v {
                        final_res_tup1.push(*val);
                    }
                }
            }
        });

        for re in res {
            for r in re {
                if !final_res_tup.contains(&r) {
                    final_res_tup.push(r);
                }
            }
        }

        for re in res1 {
            for r in re {
                if !final_res_tup1.contains(&r) {
                    final_res_tup1.push(r);
                }
            }
        }

        final_res_tup.sort();
        final_res_tup1.sort();

        let exp : Vec<usize> = vec![1,2,3,4,5];
        assert_eq!(final_res_tup, exp);
        assert_eq!(final_res_tup1, exp);
    }

    #[test]
    fn test_process_once_alpha_finite_one() {
        let tup = vec![Int(1)];
        let tup1 = vec![Int(2)];
        let interval = TimeInterval::new(TS::new(1), TS::new(10));
        let mut betas: HashMap<Vec<Constant>, Vec<(usize, usize, usize, bool)>> = HashMap::new();
        let mut obs_seq = ObservationSequence::init();

        // best case, all gaps are filled
        let mut res = Vec::new();
        let mut res1 = Vec::new();
        let mut alpha_res = Vec::new();
        for i in 0..5 {
            let x = process_once_beta_finite(tup.clone(), i, interval, &mut betas, &mut obs_seq);
            if !x.is_empty() {
                res.push(x);
            }

            let x1 = process_once_beta_finite(tup1.clone(), i, interval, &mut betas, &mut obs_seq);
            if !x1.is_empty() {
                res1.push(x1);
            }

            let alpha = process_once_alpha_finite(&mut betas, &mut obs_seq, interval);
            if !alpha.is_empty() {
                alpha_res.push(alpha);
            }
        }

        obs_seq.insert(10, 10);
        println!("{:?}", obs_seq.observations);
        let alpha = process_once_alpha_finite(&mut betas, &mut obs_seq, interval);
        if !alpha.is_empty() {
            alpha_res.push(alpha);
        }

        if let Some(v) = betas.get(&tup) {
            assert_eq!(v.clone(), vec![(0, 0, 0, false), (1, 0, 0, false), (2, 0, 0, false), (3, 0, 0, false), (4, 0, 0, false)]);
        } else { assert!(false); }
        if let Some(v) = betas.get(&tup1) {
            assert_eq!(v.clone(), vec![(0, 0, 0, false), (1, 0, 0, false), (2, 0, 0, false), (3, 0, 0, false), (4, 0, 0, false)]);
        } else { assert!(false); }
    }

    #[test]
    fn test_process_once_alpha_finite_one1() {
        let tup = vec![Int(1)];
        let tup1 = vec![Int(2)];
        let interval = TimeInterval::new(TS::new(1), TS::new(10));
        let mut betas: HashMap<Vec<Constant>, Vec<(usize, usize, usize, bool)>> = HashMap::new();
        let mut obs_seq = ObservationSequence::init();

        // best case, all gaps are filled
        let mut res = Vec::new();
        let mut res1 = Vec::new();
        let mut alpha_res = Vec::new();
        for i in 0..5 {
            obs_seq.insert(i, i);

            let x = process_once_beta_finite(tup.clone(), i, interval, &mut betas, &mut obs_seq);
            if !x.is_empty() {
                res.push(x);
            }

            let x1 = process_once_beta_finite(tup1.clone(), i, interval, &mut betas, &mut obs_seq);
            if !x1.is_empty() {
                res1.push(x1);
            }

            let alpha = process_once_alpha_finite(&mut betas, &mut obs_seq, interval);
            if !alpha.is_empty() {
                alpha_res.push(alpha);
            }
        }

        if let Some(v) = betas.get(&tup) {
            assert_eq!(v.clone(), vec![(0, 1, 4, true), (1, 2, 4, true), (2, 3, 4, true), (3, 4, 4, true), (4,0,0,false)]);
        } else { assert!(false); }
        if let Some(v) = betas.get(&tup1) {
            assert_eq!(v.clone(), vec![(0, 1, 4, true), (1, 2, 4, true), (2, 3, 4, true), (3, 4, 4, true), (4,0,0,false)]);
        } else { assert!(false); }

        let mut final_res_tup = Vec::new();
        let mut final_res_tup1 = Vec::new();
        alpha_res.iter().for_each(|x| {
            for (k, v) in x {
                //println!("Key {:?}  Value {:?}", k, v);
                if k.clone() == tup {
                    for val in v {
                        final_res_tup.push(*val);
                    }
                } else {
                    for val in v {
                        final_res_tup1.push(*val);
                    }
                }
            }
        });

        for re in res {
            for r in re {
                if !final_res_tup.contains(&r) {
                    final_res_tup.push(r);
                }
            }
        }

        for re in res1 {
            for r in re {
                if !final_res_tup1.contains(&r) {
                    final_res_tup1.push(r);
                }
            }
        }

        final_res_tup.sort();
        final_res_tup1.sort();

        let exp : Vec<usize> = vec![1,2,3,4];
        assert_eq!(final_res_tup, exp);
        assert_eq!(final_res_tup1, exp);
    }






    #[test]
    fn test_process_once_alpha_finite() {
        let tup = vec![Int(1)];
        let tup1 = vec![Int(2)];
        let interval = TimeInterval::new(TS::new(0), TS::new(10));
        let mut betas: HashMap<Vec<Constant>, Vec<(usize, usize, usize, bool)>> = HashMap::new();
        let mut obs_seq = ObservationSequence::init();

        // best case, all gaps are filled
        let mut res = Vec::new();
        let mut res1 = Vec::new();
        let mut alpha_res = Vec::new();
        for i in 0..5 {
            obs_seq.insert(i, i);
            let x = process_once_beta_finite(tup.clone(), i, interval, &mut betas, &mut obs_seq);
            if !x.is_empty() {
                res.push(x);
            }

            let x1 = process_once_beta_finite(tup1.clone(), i, interval, &mut betas, &mut obs_seq);
            if !x1.is_empty() {
                res1.push(x1);
            }

            let alpha = process_once_alpha_finite(&mut betas, &mut obs_seq, interval);
            if !alpha.is_empty() {
                alpha_res.push(alpha);
            }
        }
        assert_eq!(res, vec![vec![0], vec![1], vec![2], vec![3], vec![4]]);
        assert_eq!(res1, vec![vec![0], vec![1], vec![2], vec![3], vec![4]]);
        if let Some(v) = betas.get(&tup) {
            assert_eq!(v.clone(), vec![(0, 0, 4, true), (1, 1, 4, true), (2, 2, 4, true), (3, 3, 4, true), (4, 4, 4, true)]);
        } else { assert!(false); }
        if let Some(v) = betas.get(&tup1) {
            assert_eq!(v.clone(), vec![(0, 0, 4, true), (1, 1, 4, true), (2, 2, 4, true), (3, 3, 4, true), (4, 4, 4, true)]);
        } else { assert!(false); }

        let mut final_res_tup = Vec::new();
        let mut final_res_tup1 = Vec::new();
        alpha_res.iter().for_each(|x| {
            for (k, v) in x {
                println!("Key {:?}  Value {:?}", k, v);
                if k.clone() == tup {
                    for val in v {
                        final_res_tup.push(*val);
                    }
                } else {
                    for val in v {
                        final_res_tup1.push(*val);
                    }
                }
            }
        });

        for re in res {
            for r in re {
                if !final_res_tup.contains(&r) {
                    final_res_tup.push(r);
                }
            }
        }

        for re in res1 {
            for r in re {
                if !final_res_tup1.contains(&r) {
                    final_res_tup1.push(r);
                }
            }
        }

        final_res_tup.sort();
        final_res_tup1.sort();

        let exp : Vec<usize> = vec![0,1,2,3,4];
        assert_eq!(final_res_tup, exp);
        assert_eq!(final_res_tup1, exp);
    }

    #[test]
    fn test_process_once_alpha_finite1() {
        let tup = vec![Int(1)];
        let tup1 = vec![Int(2)];
        let interval = TimeInterval::new(TS::new(0), TS::new(10));
        let mut betas: HashMap<Vec<Constant>, Vec<(usize, usize, usize, bool)>> = HashMap::new();
        let mut obs_seq = ObservationSequence::init();

        // best case, all gaps are filled
        let mut res = Vec::new();
        let mut res1 = Vec::new();
        let mut alpha_res = Vec::new();
        for i in 0..5 {
            if i != 3 {
                obs_seq.insert(i, i);
            }

            let x = process_once_beta_finite(tup.clone(), i, interval, &mut betas, &mut obs_seq);
            if !x.is_empty() {
                res.push(x);
            }

            let x1 = process_once_beta_finite(tup1.clone(), i, interval, &mut betas, &mut obs_seq);
            if !x1.is_empty() {
                res1.push(x1);
            }

            let alpha = process_once_alpha_finite(&mut betas, &mut obs_seq, interval);
            if !alpha.is_empty() {
                alpha_res.push(alpha);
            }
        }
        assert_eq!(res, vec![vec![0], vec![1], vec![2], vec![3], vec![4]]);
        assert_eq!(res1, vec![vec![0], vec![1], vec![2], vec![3], vec![4]]);
        if let Some(v) = betas.get(&tup) {
            assert_eq!(v.clone(), vec![(0, 0, 4, true), (1, 1, 4, true), (2, 2, 4, true), (3, 3, 4, true), (4, 4, 4, true)]);
        } else { assert!(false); }
        if let Some(v) = betas.get(&tup1) {
            assert_eq!(v.clone(), vec![(0, 0, 4, true), (1, 1, 4, true), (2, 2, 4, true), (3, 3, 4, true), (4, 4, 4, true)]);
        } else { assert!(false); }

        let mut final_res_tup = Vec::new();
        let mut final_res_tup1 = Vec::new();
        alpha_res.iter().for_each(|x| {
            for (k, v) in x {
                //println!("Key {:?}  Value {:?}", k, v);
                if k.clone() == tup {
                    for val in v {
                        final_res_tup.push(*val);
                    }
                } else {
                    for val in v {
                        final_res_tup1.push(*val);
                    }
                }
            }
        });

        for re in res {
            for r in re {
                if !final_res_tup.contains(&r) {
                    final_res_tup.push(r);
                }
            }
        }

        for re in res1 {
            for r in re {
                if !final_res_tup1.contains(&r) {
                    final_res_tup1.push(r);
                }
            }
        }

        final_res_tup.sort();
        final_res_tup1.sort();

        let exp : Vec<usize> = vec![0,1,2,3,4];
        assert_eq!(final_res_tup, exp);
        assert_eq!(final_res_tup1, exp);
    }

    #[test]
    fn test_process_once_alpha_finite2() {
        let tup = vec![Int(1)];
        let tup1 = vec![Int(2)];
        let interval = TimeInterval::new(TS::new(0), TS::new(10));
        let mut betas: HashMap<Vec<Constant>, Vec<(usize, usize, usize, bool)>> = HashMap::new();
        let mut obs_seq = ObservationSequence::init();

        // best case, all gaps are filled
        let mut res = Vec::new();
        let mut res1 = Vec::new();
        let mut alpha_res = Vec::new();
        for i in 0..5 {
            let x = process_once_beta_finite(tup.clone(), i, interval, &mut betas, &mut obs_seq);
            if !x.is_empty() {
                res.push(x);
            }

            let x1 = process_once_beta_finite(tup1.clone(), i, interval, &mut betas, &mut obs_seq);
            if !x1.is_empty() {
                res1.push(x1);
            }

            let alpha = process_once_alpha_finite(&mut betas, &mut obs_seq, interval);
            if !alpha.is_empty() {
                alpha_res.push(alpha);
            }
        }

        obs_seq.insert(10, 10);
        let alpha = process_once_alpha_finite(&mut betas, &mut obs_seq, interval);
        if !alpha.is_empty() {
            alpha_res.push(alpha);
        }

        assert_eq!(res, vec![vec![0], vec![1], vec![2], vec![3], vec![4]]);
        assert_eq!(res1, vec![vec![0], vec![1], vec![2], vec![3], vec![4]]);
        if let Some(v) = betas.get(&tup) {
            assert_eq!(v.clone(), vec![(0, 0, 10, true), (1, 1, 10, true), (2, 2, 10, true), (3, 3, 10, true), (4, 4, 10, true)]);
        } else { assert!(false); }
        if let Some(v) = betas.get(&tup1) {
            assert_eq!(v.clone(), vec![(0, 0, 10, true), (1, 1, 10, true), (2, 2, 10, true), (3, 3, 10, true), (4, 4, 10, true)]);
        } else { assert!(false); }

        let mut final_res_tup = Vec::new();
        let mut final_res_tup1 = Vec::new();
        alpha_res.iter().for_each(|x| {
            for (k, v) in x {
                //println!("Key {:?}  Value {:?}", k, v);
                if k.clone() == tup {
                    for val in v {
                        final_res_tup.push(*val);
                    }
                } else {
                    for val in v {
                        final_res_tup1.push(*val);
                    }
                }
            }
        });

        for re in res {
            for r in re {
                if !final_res_tup.contains(&r) {
                    final_res_tup.push(r);
                }
            }
        }

        for re in res1 {
            for r in re {
                if !final_res_tup1.contains(&r) {
                    final_res_tup1.push(r);
                }
            }
        }

        final_res_tup.sort();
        final_res_tup1.sort();

        let exp : Vec<usize> = vec![0,1,2,3,4,5,6,7,8,9,10];
        assert_eq!(final_res_tup, exp);
        assert_eq!(final_res_tup1, exp);
    }






    #[test]
    fn test_process_once_finite() {
        let tuple = vec![Int(1)];
        let interval = TimeInterval::new(TS::new(0), TS::new(10));
        let mut betas: HashMap<Vec<Constant>, Vec<(usize, usize, usize, bool)>> = HashMap::new();
        let mut obs_seq = ObservationSequence::init();

        obs_seq.insert(0, 0);
        obs_seq.insert(1, 1);
        obs_seq.insert(2, 2);
        obs_seq.insert(3, 3);
        obs_seq.insert(4, 4);

        // best case, all gaps are filled
        let mut res = Vec::new();
        for i in 0..10 {
            let x = process_once_beta_finite(tuple.clone(), i, interval, &mut betas, &mut obs_seq);
            if !x.is_empty() {
                res.push(x);
            }
        }

        println!("OBS seq: {:?}", obs_seq.observations);

        assert_eq!(res, vec![vec![0, 1, 2, 3, 4], vec![1, 2, 3, 4], vec![2, 3, 4], vec![3, 4], vec![4], vec![5], vec![6], vec![7], vec![8], vec![9]]);
        if let Some(v) = betas.get(&tuple) {
            assert_eq!(v.clone(), vec![(0, 0, 4, true), (1, 1, 4, true), (2, 2, 4, true), (3, 3, 4, true), (4, 4, 4, true), (5, 5, 5, true), (6, 6, 6, true), (7, 7, 7, true), (8, 8, 8, true), (9, 9, 9, true)]);
        } else {
            assert!(false);
        }


        let interval = TimeInterval::new(TS::new(0), TS::new(10));
        let mut betas: HashMap<Vec<Constant>, Vec<(usize, usize, usize, bool)>> = HashMap::new();
        let mut obs_seq = ObservationSequence::init();
        obs_seq.insert(0, 0);
        obs_seq.insert(3, 3);
        obs_seq.insert(4, 4);
        obs_seq.insert(5, 5);
        obs_seq.insert(8, 8);
        obs_seq.insert(9, 9);

        println!("OBS seq: {:?}", obs_seq.observations);

        let mut res = Vec::new();
        for i in 0..10 {
            let x = process_once_beta_finite(tuple.clone(), i, interval, &mut betas, &mut obs_seq);
            if !x.is_empty() {
                res.push(x);
            }
        }

        assert_eq!(res, vec![vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
                             vec![1, 2, 3, 4, 5, 6, 7, 8, 9],
                             vec![2, 3, 4, 5, 6, 7, 8, 9],
                             vec![3, 4, 5, 6, 7, 8, 9],
                             vec![4, 5, 6, 7, 8, 9],
                             vec![5, 6, 7, 8, 9],
                             vec![6, 7, 8, 9],
                             vec![7, 8, 9],
                             vec![8, 9],
                             vec![9]]);
        if let Some(v) = betas.get(&tuple) {
            assert_eq!(v.clone(), vec![(0, 0, 9, true), (1, 1, 9, true), (2, 2, 9, true), (3, 3, 9, true), (4, 4, 9, true), (5, 5, 9, true), (6, 6, 9, true), (7, 7, 9, true), (8, 8, 9, true), (9, 9, 9, true)]);
        } else {
            assert!(false);
        }
    }

    #[test]
    fn test_process_once_finite1() {
        let tuple = vec![Int(1)];
        let interval = TimeInterval::new(TS::new(0), TS::new(10));
        let mut betas: HashMap<Vec<Constant>, Vec<(usize, usize, usize, bool)>> = HashMap::new();
        let mut obs_seq = ObservationSequence::init();

        // best case, all gaps are filled
        let mut res = Vec::new();
        for i in 0..10 {
            obs_seq.insert(i, i);
            let x = process_once_beta_finite(tuple.clone(), i, interval, &mut betas, &mut obs_seq);
            if !x.is_empty() {
                res.push(x);
            }
        }
        assert_eq!(res, vec![vec![0], vec![1], vec![2], vec![3], vec![4], vec![5], vec![6], vec![7], vec![8], vec![9]]);
        if let Some(v) = betas.get(&tuple) {
            assert_eq!(v.clone(), vec![(0, 0, 0, true), (1, 1, 1, true), (2, 2, 2, true), (3, 3, 3, true), (4, 4, 4, true), (5, 5, 5, true), (6, 6, 6, true), (7, 7, 7, true), (8, 8, 8, true), (9, 9, 9, true)]);
        } else {
            assert!(false);
        }

        // no data available stash all values
        let interval = TimeInterval::new(TS::new(0), TS::new(10));
        let mut betas: HashMap<Vec<Constant>, Vec<(usize, usize, usize, bool)>> = HashMap::new();
        let mut obs_seq = ObservationSequence::init();
        let mut res = Vec::new();
        for i in 0..10 {
            let x = process_once_beta_finite(tuple.clone(), i, interval, &mut betas, &mut obs_seq);
            if !x.is_empty() {
                res.push(x);
            }
        }
        assert_eq!(res, vec![vec![0], vec![1], vec![2], vec![3], vec![4], vec![5], vec![6], vec![7], vec![8], vec![9]]);
        if let Some(v) = betas.get(&tuple) {
            assert_eq!(v.clone(), vec![(0, 0, 0, true), (1, 1, 1, true), (2, 2, 2, true), (3, 3, 3, true), (4, 4, 4, true), (5, 5, 5, true), (6, 6, 6, true), (7, 7, 7, true), (8, 8, 8, true), (9, 9, 9, true)]);
        } else {
            assert!(false);
        }
    }

    #[test]
    fn test_process_once_finite2() {
        let tuple = vec![Int(1)];
        let interval = TimeInterval::new(TS::new(1), TS::new(10));
        let mut betas: HashMap<Vec<Constant>, Vec<(usize, usize, usize, bool)>> = HashMap::new();
        let mut obs_seq = ObservationSequence::init();
        obs_seq.insert(0, 0);
        obs_seq.insert(3, 3);
        obs_seq.insert(4, 4);
        obs_seq.insert(5, 5);
        obs_seq.insert(8, 8);
        obs_seq.insert(9, 9);

        println!("OBS seq: {:?}", obs_seq.observations);

        let mut res = Vec::new();
        for i in 0..10 {
            let x = process_once_beta_finite(tuple.clone(), i, interval, &mut betas, &mut obs_seq);
            if !x.is_empty() {
                res.push(x);
            }
        }

        assert_eq!(res, vec![vec![3, 4, 5, 6, 7, 8, 9], //0
                             vec![4, 5, 6, 7, 8, 9],   //1
                             vec![4, 5, 6, 7, 8, 9],   //2
                             vec![4, 5, 6, 7, 8, 9],   //3
                             vec![5, 6, 7, 8, 9],     //4
                             vec![8, 9],           //5
                             vec![9],             //6
                             vec![9],             //7
                             vec![9],             //8
        ]);
        if let Some(v) = betas.get(&tuple) {
            assert_eq!(v.clone(), vec![(0, 3, 9, true), (1, 4, 9, true), (2, 4, 9, true), (3, 4, 9, true), (4, 5, 9, true), (5, 8, 9, true), (6, 9, 9, true), (7, 9, 9, true), (8, 9, 9, true), (9, 0, 0, false)]);
        } else {
            assert!(false);
        }
    }

    #[test]
    fn test_process_once_infinite() {
        let tuple = vec![Int(1)];
        let interval = TimeInterval::new(TS::new(0), TS::INFINITY);
        let mut betas: HashMap<Vec<Constant>, Vec<(usize, usize, usize, bool)>> = HashMap::new();
        let mut obs_seq = ObservationSequence::init();

        // best case, all gaps are filled
        let mut res = Vec::new();
        for i in 0..10 {
            obs_seq.insert(i, i);
            let x = process_once_beta_infinite(tuple.clone(), i, 9, interval, &mut betas, &mut obs_seq);
            if !x.is_empty() {
                res.push(x);
            }
        }
        assert_eq!(res, vec![vec![0,1,2,3,4,5,6,7,8,9],
                             vec![1,2,3,4,5,6,7,8,9],
                             vec![2,3,4,5,6,7,8,9],
                             vec![3,4,5,6,7,8,9],
                             vec![4,5,6,7,8,9],
                             vec![5,6,7,8,9],
                             vec![6,7,8,9],
                             vec![7,8,9],
                             vec![8,9],
                             vec![9]]);
        if let Some(v) = betas.get(&tuple) {
            assert_eq!(v.clone(), vec![(0, 0, 9, true), (1, 1, 9, true), (2, 2, 9, true), (3, 3, 9, true), (4, 4, 9, true), (5, 5, 9, true), (6, 6, 9, true), (7, 7, 9, true), (8, 8, 9, true), (9, 9, 9, true)]);
        } else {
            assert!(false);
        }
    }

    #[test]
    fn test_process_once_infinte1() {
        let tuple = vec![Int(1)];
        let interval = TimeInterval::new(TS::new(1), TS::INFINITY);
        let mut betas: HashMap<Vec<Constant>, Vec<(usize, usize, usize, bool)>> = HashMap::new();
        let mut obs_seq = ObservationSequence::init();
        obs_seq.insert(0, 0);
        obs_seq.insert(3, 3);
        obs_seq.insert(4, 4);
        obs_seq.insert(5, 5);
        obs_seq.insert(8, 8);
        obs_seq.insert(9, 9);

        println!("OBS seq: {:?}", obs_seq.observations);

        let mut res = Vec::new();
        for i in 0..10 {
            let x = process_once_beta_infinite(tuple.clone(), i, 9, interval, &mut betas, &mut obs_seq);
            if !x.is_empty() {
                res.push(x);
            }
        }

        assert_eq!(res, vec![vec![3, 4, 5, 6, 7, 8, 9], //0
                             vec![4, 5, 6, 7, 8, 9],   //1
                             vec![4, 5, 6, 7, 8, 9],   //2
                             vec![4, 5, 6, 7, 8, 9],   //3
                             vec![5, 6, 7, 8, 9],     //4
                             vec![8, 9],           //5
                             vec![9],             //6
                             vec![9],             //7
                             vec![9],             //8
        ]);
        if let Some(v) = betas.get(&tuple) {
            assert_eq!(v.clone(), vec![(0, 3, 9, true), (1, 4, 9, true), (2, 4, 9, true), (3, 4, 9, true), (4, 5, 9, true), (5, 8, 9, true), (6, 9, 9, true), (7, 9, 9, true), (8, 9, 9, true), (9, 0, 0, false)]);
        } else {
            assert!(false);
        }
    }




    #[test]
    fn test_process_eventually_finite() {
        let tuple = vec![Int(1)];
        let interval = TimeInterval::new(TS::new(0), TS::new(10));
        let mut betas: HashMap<Vec<Constant>, Vec<(usize, usize, usize, bool)>> = HashMap::new();
        let mut obs_seq = ObservationSequence::init();

        obs_seq.insert(0, 0);
        obs_seq.insert(1, 1);
        obs_seq.insert(2, 2);
        obs_seq.insert(3, 3);
        obs_seq.insert(4, 4);

        // best case, all gaps are filled
        let mut res = Vec::new();
        for i in 0..10 {
            let x = process_eventually_beta_finite(tuple.clone(), i, interval, &mut betas, &mut obs_seq);
            if !x.is_empty() {
                res.push(x);
            }
        }

        println!("OBS seq: {:?}", obs_seq.observations);

        assert_eq!(res, vec![vec![0], vec![0,1], vec![0,1,2], vec![0,1,2,3], vec![0,1,2,3,4], vec![5], vec![6], vec![7], vec![8], vec![9]]);
        if let Some(v) = betas.get(&tuple) {
            assert_eq!(v.clone(), vec![(0, 0, 0, true), (1, 0, 1, true), (2, 0, 2, true), (3, 0, 3, true), (4, 0, 4, true), (5, 5, 5, true), (6, 6, 6, true), (7, 7, 7, true), (8, 8, 8, true), (9, 9, 9, true)]);
        } else {
            assert!(false);
        }


        let interval = TimeInterval::new(TS::new(0), TS::new(10));
        let mut betas: HashMap<Vec<Constant>, Vec<(usize, usize, usize, bool)>> = HashMap::new();
        let mut obs_seq = ObservationSequence::init();
        obs_seq.insert(0, 0);
        obs_seq.insert(3, 3);
        obs_seq.insert(4, 4);
        obs_seq.insert(5, 5);
        obs_seq.insert(8, 8);
        obs_seq.insert(9, 9);

        println!("OBS seq: {:?}", obs_seq.observations);

        let mut res = Vec::new();
        for i in 0..10 {
            let x = process_eventually_beta_finite(tuple.clone(), i, interval, &mut betas, &mut obs_seq);
            if !x.is_empty() {
                res.push(x);
            }
        }

        assert_eq!(res, vec![vec![0], vec![0,1], vec![0,1,2], vec![0,1,2,3], vec![0,1,2,3,4], vec![0,1,2,3,4,5], vec![0,1,2,3,4,5,6], vec![0,1,2,3,4,5,6,7], vec![0,1,2,3,4,5,6,7,8], vec![0,1,2,3,4,5,6,7,8,9]]);
        if let Some(v) = betas.get(&tuple) {
            assert_eq!(v.clone(), vec![(0, 0, 0, true), (1, 0, 1, true), (2, 0, 2, true), (3, 0, 3, true), (4, 0, 4, true), (5, 0, 5, true), (6, 0, 6, true), (7, 0, 7, true), (8, 0, 8, true), (9, 0, 9, true)]);
        } else {
            assert!(false);
        }
    }

    #[test]
    fn test_process_eventually_finite1() {
        let tuple = vec![Int(1)];
        let interval = TimeInterval::new(TS::new(1), TS::new(10));
        let mut betas: HashMap<Vec<Constant>, Vec<(usize, usize, usize, bool)>> = HashMap::new();
        let mut obs_seq = ObservationSequence::init();
        obs_seq.insert(0, 0);
        obs_seq.insert(3, 3);
        obs_seq.insert(4, 4);
        obs_seq.insert(5, 5);
        obs_seq.insert(8, 8);
        obs_seq.insert(9, 9);

        println!("OBS seq: {:?}", obs_seq.observations);

        let mut res = Vec::new();
        for i in 0..10 {
            let x = process_eventually_beta_finite(tuple.clone(), i, interval, &mut betas, &mut obs_seq);
            if !x.is_empty() {
                res.push(x);
            }
        }

        assert_eq!(res, vec![vec![0],                  //3
                             vec![0,1,2,3],            //4
                             vec![0,1,2,3,4],          //5
                             vec![0,1,2,3,4],        //6
                             vec![0,1,2,3,4],        //7
                             vec![0,1,2,3,4,5],        //8
                             vec![0,1,2,3,4,5,6,7,8],  //9
        ]);
        if let Some(v) = betas.get(&tuple) {
            assert_eq!(v.clone(), vec![(0,0,0,false),(1, 0, 0, false), (2, 0, 0, false), (3, 0, 0, true), (4, 0, 3, true), (5, 0, 4, true), (6, 0, 4, true), (7, 0, 4, true), (8, 0, 5, true), (9, 0, 8, true)]);
        } else {
            assert!(false);
        }
    }

    #[test]
    fn test_process_eventually_finite2() {
        let tuple = vec![Int(1)];
        let interval = TimeInterval::new(TS::new(1), TS::new(10));
        let mut betas: HashMap<Vec<Constant>, Vec<(usize, usize, usize, bool)>> = HashMap::new();
        let mut obs_seq = ObservationSequence::init();

        println!("OBS seq: {:?}", obs_seq.observations);

        let mut res = Vec::new();
        for i in 0..10 {
            let x = process_eventually_beta_finite(tuple.clone(), i, interval, &mut betas, &mut obs_seq);
            if !x.is_empty() {
                res.push(x);
            }
        }


        assert!(res.is_empty());
        if let Some(v) = betas.get(&tuple) {
            assert_eq!(v.clone(), vec![(0,0,0,false),(1, 0, 0, false), (2, 0, 0, false), (3, 0, 0, false), (4, 0, 0, false), (5, 0, 0, false), (6, 0, 0, false), (7, 0, 0, false), (8, 0, 0, false), (9, 0, 0, false)]);
        } else {
            assert!(false);
        }
    }

    #[test]
    fn test_process_eventually_finite3() {
        let tuple = vec![Int(1)];
        let interval = TimeInterval::new(TS::new(1), TS::new(10));
        let mut betas: HashMap<Vec<Constant>, Vec<(usize, usize, usize, bool)>> = HashMap::new();
        let mut obs_seq = ObservationSequence::init();

        let mut res = Vec::new();
        for i in 0..10 {
            obs_seq.insert(i,i);
            let x = process_eventually_beta_finite(tuple.clone(), i, interval, &mut betas, &mut obs_seq);
            if !x.is_empty() {
                res.push(x);
            }
        }


        assert_eq!(res, vec![vec![0],                  //1
                             vec![0,1],                  //2
                             vec![0,1,2],                  //3
                             vec![0,1,2,3],            //4
                             vec![0,1,2,3,4],          //5
                             vec![0,1,2,3,4,5],        //6
                             vec![0,1,2,3,4,5,6],        //7
                             vec![0,1,2,3,4,5,6,7],        //8
                             vec![0,1,2,3,4,5,6,7,8],  //9
        ]);

        if let Some(v) = betas.get(&tuple) {
            assert_eq!(v.clone(), vec![(0,0,0, false), (1, 0, 0, true), (2, 0, 1, true), (3, 0, 2, true), (4, 0, 3, true), (5, 0, 4, true), (6, 0, 5, true), (7, 0, 6, true), (8, 0, 7, true), (9, 0, 8, true)]);
        } else {
            assert!(false);
        }
    }




    #[test]
    fn test_process_eventually_alpha_finite() {
        let tup = vec![Int(1)];
        let tup1 = vec![Int(2)];
        let interval = TimeInterval::new(TS::new(0), TS::new(10));
        let mut betas: HashMap<Vec<Constant>, Vec<(usize, usize, usize, bool)>> = HashMap::new();
        let mut obs_seq = ObservationSequence::init();

        // best case, all gaps are filled
        let mut res = Vec::new();
        let mut res1 = Vec::new();
        let mut alpha_res = Vec::new();
        for i in 0..5 {
            obs_seq.insert(i, i);
            let x = process_eventually_beta_finite(tup.clone(), i, interval, &mut betas, &mut obs_seq);
            if !x.is_empty() {
                res.push(x);
            }

            let x1 = process_eventually_beta_finite(tup1.clone(), i, interval, &mut betas, &mut obs_seq);
            if !x1.is_empty() {
                res1.push(x1);
            }

            let alpha = process_eventually_alpha_finite(&mut betas, &mut obs_seq, interval);
            if !alpha.is_empty() {
                alpha_res.push(alpha);
            }
        }
        assert_eq!(res, vec![vec![0], vec![0,1], vec![0,1,2], vec![0,1,2,3], vec![0,1,2,3,4]]);
        assert_eq!(res1, vec![vec![0], vec![0,1], vec![0,1,2], vec![0,1,2,3], vec![0,1,2,3,4]]);
        if let Some(v) = betas.get(&tup) {
            assert_eq!(v.clone(), vec![(0, 0, 0, true), (1, 0, 1, true), (2, 0, 2, true), (3, 0, 3, true), (4, 0, 4, true)]);
        } else { assert!(false); }
        if let Some(v) = betas.get(&tup1) {
            assert_eq!(v.clone(), vec![(0, 0, 0, true), (1, 0, 1, true), (2, 0, 2, true), (3, 0, 3, true), (4, 0, 4, true)]);
        } else { assert!(false); }

        let mut final_res_tup = Vec::new();
        let mut final_res_tup1 = Vec::new();
        alpha_res.iter().for_each(|x| {
            for (k, v) in x {
                println!("Key {:?}  Value {:?}", k, v);
                if k.clone() == tup {
                    for val in v {
                        final_res_tup.push(*val);
                    }
                } else {
                    for val in v {
                        final_res_tup1.push(*val);
                    }
                }
            }
        });

        for re in res {
            for r in re {
                if !final_res_tup.contains(&r) {
                    final_res_tup.push(r);
                }
            }
        }

        for re in res1 {
            for r in re {
                if !final_res_tup1.contains(&r) {
                    final_res_tup1.push(r);
                }
            }
        }

        final_res_tup.sort();
        final_res_tup1.sort();

        let exp : Vec<usize> = vec![0,1,2,3,4];
        assert_eq!(final_res_tup, exp);
        assert_eq!(final_res_tup1, exp);
    }

    #[test]
    fn test_process_eventually_alpha_finite1() {
        let tup = vec![Int(1)];
        let tup1 = vec![Int(2)];
        let interval = TimeInterval::new(TS::new(0), TS::new(10));
        let mut betas: HashMap<Vec<Constant>, Vec<(usize, usize, usize, bool)>> = HashMap::new();
        let mut obs_seq = ObservationSequence::init();

        // best case, all gaps are filled
        let mut res = Vec::new();
        let mut res1 = Vec::new();
        let mut alpha_res = Vec::new();
        for i in 0..5 {
            if i != 3 {
                obs_seq.insert(i, i);
            }

            println!("@{i}  {:?}", obs_seq.observations);

            let x = process_eventually_beta_finite(tup.clone(), i, interval, &mut betas, &mut obs_seq);
            if !x.is_empty() {
                res.push(x);
            }

            let x1 = process_eventually_beta_finite(tup1.clone(), i, interval, &mut betas, &mut obs_seq);
            if !x1.is_empty() {
                res1.push(x1);
            }

            let alpha = process_eventually_alpha_finite(&mut betas, &mut obs_seq, interval);
            if !alpha.is_empty() {
                alpha_res.push(alpha);
            }
        }
        assert_eq!(res, vec![vec![0], vec![0,1], vec![0,1,2], vec![3], vec![0,1,2,3,4]]);
        assert_eq!(res1, vec![vec![0], vec![0,1], vec![0,1,2], vec![3], vec![0,1,2,3,4]]);
        if let Some(v) = betas.get(&tup) {
            assert_eq!(v.clone(), vec![(0, 0, 0, true), (1, 0, 1, true), (2, 0, 2, true), (3, 0, 3, true), (4, 0, 4, true)]);
        } else { assert!(false); }
        if let Some(v) = betas.get(&tup1) {
            assert_eq!(v.clone(), vec![(0, 0, 0, true), (1, 0, 1, true), (2, 0, 2, true), (3, 0, 3, true), (4, 0, 4, true)]);
        } else { assert!(false); }

        let mut final_res_tup = Vec::new();
        let mut final_res_tup1 = Vec::new();
        alpha_res.iter().for_each(|x| {
            for (k, v) in x {
                //println!("Key {:?}  Value {:?}", k, v);
                if k.clone() == tup {
                    for val in v {
                        final_res_tup.push(*val);
                    }
                } else {
                    for val in v {
                        final_res_tup1.push(*val);
                    }
                }
            }
        });

        for re in res {
            for r in re {
                if !final_res_tup.contains(&r) {
                    final_res_tup.push(r);
                }
            }
        }

        for re in res1 {
            for r in re {
                if !final_res_tup1.contains(&r) {
                    final_res_tup1.push(r);
                }
            }
        }

        final_res_tup.sort();
        final_res_tup1.sort();

        let exp : Vec<usize> = vec![0,1,2,3,4];
        assert_eq!(final_res_tup, exp);
        assert_eq!(final_res_tup1, exp);
    }

    #[test]
    fn test_process_eventually_alpha_finite2() {
        let tup = vec![Int(1)];
        let tup1 = vec![Int(2)];
        let interval = TimeInterval::new(TS::new(0), TS::new(10));
        let mut betas: HashMap<Vec<Constant>, Vec<(usize, usize, usize, bool)>> = HashMap::new();
        let mut obs_seq = ObservationSequence::init();

        // best case, all gaps are filled
        let mut res = Vec::new();
        let mut res1 = Vec::new();
        let mut alpha_res = Vec::new();
        for i in 0..5 {
            let x = process_eventually_beta_finite(tup.clone(), i, interval, &mut betas, &mut obs_seq);
            if !x.is_empty() {
                res.push(x);
            }

            let x1 = process_eventually_beta_finite(tup1.clone(), i, interval, &mut betas, &mut obs_seq);
            if !x1.is_empty() {
                res1.push(x1);
            }

            let alpha = process_eventually_alpha_finite(&mut betas, &mut obs_seq, interval);
            if !alpha.is_empty() {
                alpha_res.push(alpha);
            }
        }

        obs_seq.insert(0, 0);
        obs_seq.insert(4,4);
        let alpha = process_eventually_alpha_finite(&mut betas, &mut obs_seq, interval);
        if !alpha.is_empty() {
            alpha_res.push(alpha);
        }

        assert_eq!(res, vec![vec![0], vec![1], vec![2], vec![3], vec![4]]);
        assert_eq!(res1, vec![vec![0], vec![1], vec![2], vec![3], vec![4]]);
        if let Some(v) = betas.get(&tup) {
            assert_eq!(v.clone(), vec![(0, 0, 0, true), (1, 0, 1, true), (2, 0, 2, true), (3, 0, 3, true), (4, 0, 4, true)]);
        } else { assert!(false); }
        if let Some(v) = betas.get(&tup1) {
            assert_eq!(v.clone(), vec![(0, 0, 0, true), (1, 0, 1, true), (2, 0, 2, true), (3, 0, 3, true), (4, 0, 4, true)]);
        } else { assert!(false); }

        let mut final_res_tup = Vec::new();
        let mut final_res_tup1 = Vec::new();
        alpha_res.iter().for_each(|x| {
            for (k, v) in x {
                //println!("Key {:?}  Value {:?}", k, v);
                if k.clone() == tup {
                    for val in v {
                        final_res_tup.push(*val);
                    }
                } else {
                    for val in v {
                        final_res_tup1.push(*val);
                    }
                }
            }
        });

        for re in res {
            for r in re {
                if !final_res_tup.contains(&r) {
                    final_res_tup.push(r);
                }
            }
        }

        for re in res1 {
            for r in re {
                if !final_res_tup1.contains(&r) {
                    final_res_tup1.push(r);
                }
            }
        }

        final_res_tup.sort();
        final_res_tup1.sort();

        let exp : Vec<usize> = vec![0,1,2,3,4];
        assert_eq!(final_res_tup, exp);
        assert_eq!(final_res_tup1, exp);
    }





    #[test]
    fn test_process_beta_infinite() {
        let tup = vec![Int(1)];
        let interval = TimeInterval::new(TS::new(0), TS::INFINITY);
        let mut betas: HashMap<Vec<Constant>, Vec<(usize, usize, usize, bool)>> = HashMap::new();
        let mut obs_seq = ObservationSequence::init();

        // best case, all gaps are filled
        let mut res = Vec::new();
        for i in 0..5 {
            obs_seq.insert(i, i);
            let x = process_eventually_beta_infinite(tup.clone(), i, interval, &mut betas, &mut obs_seq);
            if !x.is_empty() {
                res.push(x);
            }
        }
        assert_eq!(res, vec![vec![0], vec![0,1], vec![0,1,2], vec![0,1,2,3], vec![0,1,2,3,4]]);
        if let Some(v) = betas.get(&tup) {
            assert_eq!(v.clone(), vec![(0, 0, 0, true), (1, 0, 1, true), (2, 0, 2, true), (3, 0, 3, true), (4, 0, 4, true)]);
        } else { assert!(false); }

        let mut final_res_tup = Vec::new();
        for re in res {
            for r in re {
                if !final_res_tup.contains(&r) {
                    final_res_tup.push(r);
                }
            }
        }

        final_res_tup.sort();
        let exp : Vec<usize> = vec![0,1,2,3,4];
        assert_eq!(final_res_tup, exp);
    }

    #[test]
    fn test_process_beta_infinite1() {
        let tup = vec![Int(1)];
        let interval = TimeInterval::new(TS::new(1), TS::INFINITY);
        let mut betas: HashMap<Vec<Constant>, Vec<(usize, usize, usize, bool)>> = HashMap::new();
        let mut obs_seq = ObservationSequence::init();

        // best case, all gaps are filled
        let mut res = Vec::new();
        for i in 0..5 {
            obs_seq.insert(i, i);
            let x = process_eventually_beta_infinite(tup.clone(), i, interval, &mut betas, &mut obs_seq);
            if !x.is_empty() {
                res.push(x);
            }
        }
        assert_eq!(res, vec![vec![0], vec![0,1], vec![0,1,2], vec![0,1,2,3]]);
        if let Some(v) = betas.get(&tup) {
            assert_eq!(v.clone(), vec![(0,0,0,false), (1, 0, 0, true), (2, 0, 1, true), (3, 0, 2, true), (4, 0, 3, true)]);
        } else { assert!(false); }

        let mut final_res_tup = Vec::new();
        for re in res {
            for r in re {
                if !final_res_tup.contains(&r) {
                    final_res_tup.push(r);
                }
            }
        }

        final_res_tup.sort();
        let exp : Vec<usize> = vec![0,1,2,3];
        assert_eq!(final_res_tup, exp);
    }

    #[test]
    fn test_process_beta_infinite2() {
        let tup = vec![Int(1)];
        let interval = TimeInterval::new(TS::new(1), TS::INFINITY);
        let mut betas: HashMap<Vec<Constant>, Vec<(usize, usize, usize, bool)>> = HashMap::new();
        let mut obs_seq = ObservationSequence::init();

        // best case, all gaps are filled
        let mut res = Vec::new();
        for i in 0..5 {
            if i != 1 && i != 3 {
                obs_seq.insert(i, i);
            }

            let x = process_eventually_beta_infinite(tup.clone(), i, interval, &mut betas, &mut obs_seq);
            if !x.is_empty() {
                res.push(x);
            }
        }
        println!("{:?}", obs_seq.observations);
        println!("{:?}", res);
        if let Some(v) = betas.get(&tup) {
            assert_eq!(v.clone(), vec![(0,0,0, false), (1, 0, 0, false), (2, 0, 0, false), (3, 0, 0, false), (4, 0, 0, false)]);
        } else { assert!(false); }
    }

    #[test]
    fn test_process_beta_infinite3() {
        let tup = vec![Int(1)];
        let interval = TimeInterval::new(TS::new(0), TS::INFINITY);
        let mut betas: HashMap<Vec<Constant>, Vec<(usize, usize, usize, bool)>> = HashMap::new();
        let mut obs_seq = ObservationSequence::init();

        // best case, all gaps are filled
        let mut res = Vec::new();
        for i in 0..5 {
            let x = process_eventually_beta_infinite(tup.clone(), i, interval, &mut betas, &mut obs_seq);
            if !x.is_empty() {
                res.push(x);
            }
        }
        assert_eq!(res, vec![vec![0], vec![0,1], vec![0,1,2], vec![0,1,2,3], vec![0,1,2,3,4]]);
        if let Some(v) = betas.get(&tup) {
            assert_eq!(v.clone(), vec![(0, 0, 0, true), (1, 0, 1, true), (2, 0, 2, true), (3, 0, 3, true), (4, 0, 4, true)]);
        } else { assert!(false); }

        let mut final_res_tup = Vec::new();
        for re in res {
            for r in re {
                if !final_res_tup.contains(&r) {
                    final_res_tup.push(r);
                }
            }
        }

        final_res_tup.sort();
        let exp : Vec<usize> = vec![0,1,2,3,4];
        assert_eq!(final_res_tup, exp);
    }



    #[test]
    fn test_process_eventually_alpha_infinite() {
        let tup = vec![Int(1)];
        let tup1 = vec![Int(2)];
        let interval = TimeInterval::new(TS::new(0), TS::INFINITY);
        let mut betas: HashMap<Vec<Constant>, Vec<(usize, usize, usize, bool)>> = HashMap::new();
        let mut obs_seq = ObservationSequence::init();

        // best case, all gaps are filled
        let mut res = Vec::new();
        let mut res1 = Vec::new();
        let mut alpha_res = Vec::new();
        for i in 0..5 {
            let x = process_eventually_beta_infinite(tup.clone(), i, interval, &mut betas, &mut obs_seq);
            if !x.is_empty() {
                res.push(x);
            }

            let x1 = process_eventually_beta_infinite(tup1.clone(), i, interval, &mut betas, &mut obs_seq);
            if !x1.is_empty() {
                res1.push(x1);
            }

            let alpha = process_eventually_alpha_infinite(&mut betas, &mut obs_seq, interval);
            if !alpha.is_empty() {
                alpha_res.push(alpha);
            }
        }

        obs_seq.insert(0, 0);
        obs_seq.insert(4,4);
        let alpha = process_eventually_alpha_infinite(&mut betas, &mut obs_seq, interval);
        if !alpha.is_empty() {
            alpha_res.push(alpha);
        }

        assert_eq!(res, vec![vec![0], vec![0,1], vec![0,1,2], vec![0,1,2,3], vec![0,1,2,3,4]]);
        assert_eq!(res1, vec![vec![0], vec![0,1], vec![0,1,2], vec![0,1,2,3], vec![0,1,2,3,4]]);
        if let Some(v) = betas.get(&tup) {
            assert_eq!(v.clone(), vec![(0, 0, 0, true), (1, 0, 1, true), (2, 0, 2, true), (3, 0, 3, true), (4, 0, 4, true)]);
        } else { assert!(false); }
        if let Some(v) = betas.get(&tup1) {
            assert_eq!(v.clone(), vec![(0, 0, 0, true), (1, 0, 1, true), (2, 0, 2, true), (3, 0, 3, true), (4, 0, 4, true)]);
        } else { assert!(false); }

        let mut final_res_tup = Vec::new();
        let mut final_res_tup1 = Vec::new();
        alpha_res.iter().for_each(|x| {
            for (k, v) in x {
                //println!("Key {:?}  Value {:?}", k, v);
                if k.clone() == tup {
                    for val in v {
                        final_res_tup.push(*val);
                    }
                } else {
                    for val in v {
                        final_res_tup1.push(*val);
                    }
                }
            }
        });

        for re in res {
            for r in re {
                if !final_res_tup.contains(&r) {
                    final_res_tup.push(r);
                }
            }
        }

        for re in res1 {
            for r in re {
                if !final_res_tup1.contains(&r) {
                    final_res_tup1.push(r);
                }
            }
        }

        final_res_tup.sort();
        final_res_tup1.sort();

        let exp : Vec<usize> = vec![0,1,2,3,4];
        assert_eq!(final_res_tup, exp);
        assert_eq!(final_res_tup1, exp);
    }

    #[test]
    fn test_process_eventually_alpha_infinite1() {
        let tup = vec![Int(1)];
        let tup1 = vec![Int(2)];
        let interval = TimeInterval::new(TS::new(1), TS::INFINITY);
        let mut betas: HashMap<Vec<Constant>, Vec<(usize, usize, usize, bool)>> = HashMap::new();
        let mut obs_seq = ObservationSequence::init();

        // best case, all gaps are filled
        let mut res = Vec::new();
        let mut res1 = Vec::new();
        let mut alpha_res = Vec::new();
        for i in 0..5 {
            let x = process_eventually_beta_infinite(tup.clone(), i, interval, &mut betas, &mut obs_seq);
            if !x.is_empty() {
                res.push(x);
            }

            let x1 = process_eventually_beta_infinite(tup1.clone(), i, interval, &mut betas, &mut obs_seq);
            if !x1.is_empty() {
                res1.push(x1);
            }
        }

        assert!(res.is_empty());
        assert!(res1.is_empty());

        println!("Insert 0,3,4    {:?}", obs_seq.observations);

        if let Some(v) = betas.get(&tup) {
            assert_eq!(v.clone(), vec![(0, 0, 0, false), (1, 0, 0, false), (2, 0, 0, false), (3, 0, 0, false), (4, 0, 0, false)]);
        } else { assert!(false); }
        if let Some(v) = betas.get(&tup1) {
            assert_eq!(v.clone(), vec![(0, 0, 0, false), (1, 0, 0, false), (2, 0, 0, false), (3, 0, 0, false), (4, 0, 0, false)]);
        } else { assert!(false); }

        obs_seq.insert(0, 0);
        obs_seq.insert(3,3);
        obs_seq.insert(4,4);
        let alpha = process_eventually_alpha_infinite(&mut betas, &mut obs_seq, interval);
        if !alpha.is_empty() {
            alpha_res.push(alpha);
        }

        println!("Inserted 0,3,4    {:?}", obs_seq.observations);

        //assert_eq!(alpha_res, vec![vec![0,1,2,3]]);
        if let Some(v) = betas.get(&tup) {
            assert_eq!(v.clone(), vec![(0, 0, 0, false), (1, 0, 0, false), (2, 0, 0, false), (3, 0, 0, true), (4, 0, 3, true)]);
        } else { assert!(false); }
        if let Some(v) = betas.get(&tup1) {
            assert_eq!(v.clone(), vec![(0, 0, 0, false), (1, 0, 0, false), (2, 0, 0, false), (3, 0, 0, true), (4, 0, 3, true)]);
        } else { assert!(false); }

        println!("\nInserted       {:?}", obs_seq.observations);

        obs_seq.insert(1,1);
        let alpha = process_eventually_alpha_infinite(&mut betas, &mut obs_seq, interval);
        if !alpha.is_empty() {
            alpha_res.push(alpha);
        }

        println!("Inserted 1       {:?}", obs_seq.observations);

        if let Some(v) = betas.get(&tup) {
            assert_eq!(v.clone(), vec![(0, 0, 0, false), (1, 0, 0, true), (2, 0, 0, true), (3, 0, 1, true), (4, 0, 3, true)]);
        } else { assert!(false); }
        if let Some(v) = betas.get(&tup1) {
            assert_eq!(v.clone(), vec![(0, 0, 0, false), (1, 0, 0, true), (2, 0, 0, true), (3, 0, 1, true), (4, 0, 3, true)]);
        } else { assert!(false); }

        obs_seq.insert(2,2);
        let alpha = process_eventually_alpha_infinite(&mut betas, &mut obs_seq, interval);
        if !alpha.is_empty() {
            alpha_res.push(alpha);
        }

        println!("Insert 2       {:?}", obs_seq.observations);
        //assert_eq!(alpha_res, vec![vec![0,1,2,3], vec![0], vec![0,1], vec![0,1,2]]);

        if let Some(v) = betas.get(&tup) {
            assert_eq!(v.clone(), vec![(0, 0, 0, false), (1, 0, 0, true), (2, 0, 1, true), (3, 0, 2, true), (4, 0, 3, true)]);
        } else { assert!(false); }
        if let Some(v) = betas.get(&tup1) {
            assert_eq!(v.clone(), vec![(0, 0, 0, false), (1, 0, 0, true), (2, 0, 1, true), (3, 0, 2, true), (4, 0, 3, true)]);
        } else { assert!(false); }

        let mut final_res_tup = Vec::new();
        let mut final_res_tup1 = Vec::new();
        alpha_res.iter().for_each(|x| {
            for (k, v) in x {
                //println!("Key {:?}  Value {:?}", k, v);
                if k.clone() == tup {
                    for val in v {
                        final_res_tup.push(*val);
                    }
                } else {
                    for val in v {
                        final_res_tup1.push(*val);
                    }
                }
            }
        });

        for re in res {
            for r in re {
                if !final_res_tup.contains(&r) {
                    final_res_tup.push(r);
                }
            }
        }

        for re in res1 {
            for r in re {
                if !final_res_tup1.contains(&r) {
                    final_res_tup1.push(r);
                }
            }
        }

        final_res_tup.sort();
        final_res_tup1.sort();

        let exp : Vec<usize> = vec![0,0,1,1,1,2,2,3];
        assert_eq!(final_res_tup, exp);
        assert_eq!(final_res_tup1, exp);
    }*/
}