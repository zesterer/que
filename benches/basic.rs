#![feature(test)]

extern crate test;

use std::sync::Arc;
use test::{Bencher, black_box};

mod stupid {
    use std::{
        sync::Mutex,
        collections::VecDeque,
    };

    pub struct Queue<T> {
        queue: Mutex<VecDeque<T>>,
    }

    impl<T> Queue<T> {
        pub fn new() -> Self {
            Self {
                queue: Mutex::new(VecDeque::new()),
            }
        }

        pub fn insert(&self, x: T) {
            self.queue.lock().unwrap().push_front(x);
        }

        pub fn remove(&self) -> Option<T> {
            self.queue.lock().unwrap().pop_back()
        }
    }
}

#[bench]
fn que_hydra(b: &mut Bencher) {
    b.iter(|| {
        let q = Arc::new(que::Queue::new());

        let its = (0..10)
            .map(|i| {
                let q = q.clone();
                std::thread::spawn(move || {
                    for j in 0..10000 {
                        q.insert(i * 10000 + j);
                    }
                })
            })
            .collect::<Vec<_>>();

        let total = std::thread::spawn(move || {
            (0..100000u64)
                .map(|i| loop { if let Some(x) = q.remove() { break x } })
                .sum::<u64>()
        })
            .join().unwrap();

        its
            .into_iter()
            .for_each(|t| t.join().unwrap());

        assert_eq!(total, (0..100000).sum());

        black_box(total);
    });
}

#[bench]
fn stupid_hydra(b: &mut Bencher) {
    b.iter(|| {
        let q = Arc::new(stupid::Queue::new());

        let its = (0..10)
            .map(|i| {
                let q = q.clone();
                std::thread::spawn(move || {
                    for j in 0..10000 {
                        q.insert(i * 10000 + j);
                    }
                })
            })
            .collect::<Vec<_>>();

        let total = std::thread::spawn(move || {
            (0..100000u64)
                .map(|i| loop { if let Some(x) = q.remove() { break x } })
                .sum::<u64>()
        })
            .join().unwrap();

        its
            .into_iter()
            .for_each(|t| t.join().unwrap());

        assert_eq!(total, (0..100000).sum());

        black_box(total);
    });
}
