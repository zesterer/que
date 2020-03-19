#![feature(atomic_min_max, option_unwrap_none, no_more_cas)]

use std::{
    ptr,
    mem::{MaybeUninit, ManuallyDrop},
    cell::UnsafeCell,
    sync::{
        Mutex, Arc,
        atomic::{AtomicU64, AtomicUsize, AtomicPtr, AtomicBool, Ordering},
    },
};
use allocator_api::raw_vec::RawVec;
use arc_swap::ArcSwapAny;

const SLAB: u64 = 4096;

struct Slab<T> {
    offset: u64,
    buf: RawVec<T>,
    done: Vec<AtomicBool>,
    total_done: AtomicU64,
    next: ArcSwapAny<Option<Arc<Self>>>,
}

impl<T> Slab<T> {
    pub fn new(offset: u64) -> Arc<Self> {
        Arc::new(Self {
            offset,
            buf: RawVec::with_capacity(SLAB as usize),
            done: (0..SLAB).map(|_| AtomicBool::new(false)).collect(),
            total_done: AtomicU64::new(0),
            next: ArcSwapAny::empty(),
        })
    }

    pub fn slot(&self, x: u64) -> *mut T {
        unsafe { self.buf.ptr().offset((x - self.offset) as isize) }
    }

    fn done(&self, x: u64) -> &AtomicBool {
        unsafe { &*self.done.as_ptr().offset((x - self.offset) as isize) }
    }

    unsafe fn set_done(&self, x: u64) {
        debug_assert!(x < self.offset + SLAB && x >= self.offset, "x = {}, offset = {}", x, self.offset);
        self.done(x).store(true, Ordering::Relaxed);
        self.total_done.fetch_add(1, Ordering::Release);
    }

    unsafe fn is_done(&self, x: u64) -> bool {
        debug_assert!(x < self.offset + SLAB && x >= self.offset, "x = {}, offset = {}", x, self.offset);
        self.done(x).load(Ordering::Relaxed)
    }
}

pub struct Queue<T> {
    tail_slab: ArcSwapAny<Arc<Slab<T>>>,
    head_slab: ArcSwapAny<Arc<Slab<T>>>,
    head: AtomicU64,
    tail: AtomicU64,
    make_next: Mutex<u64>,
}

impl<T> Queue<T> {
    pub fn new() -> Self {
        let slab = Slab::new(0);

        Self {
            tail_slab: ArcSwapAny::new(slab.clone()),
            head_slab: ArcSwapAny::new(slab.clone()),
            head: AtomicU64::new(0),
            tail: AtomicU64::new(0),
            make_next: Mutex::new(0),
        }
    }

    pub fn len(&self) -> usize {
        let tail = self.tail.load(Ordering::Acquire);
        let head = self.head.load(Ordering::Relaxed);
        (head - tail) as usize
    }

    // TS
    fn insert_with(&self, x: T, slot: u64, slab: &Arc<Slab<T>>) {
        if slot < slab.offset + SLAB {
            unsafe {
                slab.slot(slot).write(x);
                slab.set_done(slot);
            }
        } else {
            if slab.next.load().is_none() {
                let mut guard = self.make_next.lock().unwrap();
                if *guard == slab.offset {
                    let next_slab = Slab::new(slab.offset + SLAB);
                    slab.next.store(Some(next_slab.clone()));
                    self.head_slab.store(next_slab);
                    *guard = slab.offset + SLAB;
                }
            }
            self.insert_with(x, slot, slab.next.load().as_ref().unwrap());
        }
    }

    // TS
    pub fn insert(&self, x: T) {
        let mut slab = self.head_slab.load();
        let slot = self.head.fetch_add(1, Ordering::SeqCst);

        self.insert_with(x, slot, &slab);
    }

    // !TS
    pub fn remove(&self) -> Option<T> {
        let head = self.head.load(Ordering::SeqCst);
        let slot = self.tail.load(Ordering::SeqCst);

        loop {
            let slab = self.tail_slab.load();
            if slot < slab.offset + SLAB {
                if unsafe { slab.is_done(slot) } {
                    self.tail.fetch_add(1, Ordering::Relaxed);
                    break Some(unsafe { slab.slot(slot).read() });
                } else {
                    break None;
                }
            } else {
                let next = slab.next.load();
                if next.is_none() {
                    break None
                } else if slab.total_done.load(Ordering::Acquire) == SLAB {
                    self.tail_slab.store(arc_swap::Guard::into_inner(next).unwrap());
                }
            }
        }
    }

    fn iter_raw(&self) -> impl Iterator<Item=*mut T> + '_ {
        let mut from = self.tail.load(Ordering::Relaxed);
        let to = self.head.load(Ordering::Relaxed);

        let mut slab = self.tail_slab.load_full();
        std::iter::from_fn(move || {
            if from == to {
                None
            } else {
                while from >= slab.offset + SLAB {
                    slab = slab.next.load_full().unwrap();
                }
                let ptr = slab.slot(from);
                from += 1;
                Some(ptr)
            }
        })
    }

    pub fn iter(&self) -> impl Iterator<Item=&T> + '_ {
        self
            .iter_raw()
            .map(|ptr| unsafe { &*ptr })
    }
}

impl<T> Drop for Queue<T> {
    fn drop(&mut self) {
        self
            .iter_raw()
            .for_each(|ptr| unsafe { drop(ptr.read()); });
    }
}

unsafe impl<T> Send for Queue<T> {}
unsafe impl<T> Sync for Queue<T> {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::AtomicUsize,
        Arc,
    };

    #[test]
    fn basic() {
        let q = Queue::new();

        for i in 0..10000 {
            q.insert(i);
        }

        for i in 0..1000 {
            assert_eq!(q.remove(), Some(i));
        }

        assert_eq!(unsafe { q.iter() }.copied().sum::<u32>(), (1000..10000).sum());

        for i in 1000..10000 {
            assert_eq!(q.remove(), Some(i));
        }

        assert_eq!(q.remove(), None);
    }

    #[test]
    fn drop_correctly() {
        static DROP_COUNTER: AtomicUsize = AtomicUsize::new(0);

        struct Foo(usize);

        impl Drop for Foo {
            fn drop(&mut self) {
                self.0 -= 1;
                if self.0 != 0 {
                    panic!("Double drop!");
                }
                DROP_COUNTER.fetch_sub(1, Ordering::Relaxed);
            }
        }

        let q = Queue::new();

        for _ in 0..1000 {
            q.insert(Foo(1));
            DROP_COUNTER.fetch_add(1, Ordering::Relaxed);
        }

        for i in 0..100 {
            q.remove().unwrap();
        }

        drop(q);

        assert_eq!(DROP_COUNTER.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn threaded_insert() {
        let q = Arc::new(Queue::new());

        let ts = (0..10)
            .map(|i| {
                let q = q.clone();
                std::thread::spawn(move || {
                    for j in 0..1000 {
                        q.insert(i * 1000 + j);
                    }
                })
            })
            .collect::<Vec<_>>();

        for t in ts {
            t.join().unwrap();
        }

        assert_eq!(q.iter().copied().sum::<u32>(), (0..10000).sum());
    }

    /*
    #[test]
    fn threaded_remove() {
        let q = Arc::new(Queue::new());

        for i in 0..1000000 {
            q.insert(i);
        }

        let ts = (0..10)
            .map(|i| {
                let q = q.clone();
                std::thread::spawn(move || (0..100000).map(|i| q.remove().unwrap()).collect())
            })
            .collect::<Vec<_>>();

        let total = ts
            .into_iter()
            .map(|t: std::thread::JoinHandle<Vec<u32>>| t.join().unwrap().into_iter())
            .flatten()
            .sum::<u32>();

        assert_eq!(total, (0..1000000).sum());
    }
    */

    #[test]
    fn threaded_insert_remove() {
        let q = Arc::new(Queue::new());

        let its = (0..10)
            .map(|i| {
                let q = q.clone();
                std::thread::spawn(move || {
                    for j in 0..10000000 {
                        q.insert(i * 10000000 + j);
                    }
                })
            })
            .collect::<Vec<_>>();

        let total = std::thread::spawn(move || {
            (0..100000000u64)
                .map(|i| loop { if let Some(x) = q.remove() { break x } })
                .sum::<u64>()
        })
            .join().unwrap();

        its
            .into_iter()
            .for_each(|t| t.join().unwrap());

        assert_eq!(total, (0..100000000).sum());
    }
}
