use std::collections::HashSet;
use std::time::Duration;

use bit_set::BitSet;

pub use model::KvInput;
pub use model::KvModel;
pub use model::KvOutput;
pub use model::Model;

use crate::offset_linked_list::{NodeRef, OffsetLinkedList};

mod model;
mod offset_linked_list;

pub struct Operation<C, R> {
    call_op: C,
    call_time: u64,
    return_op: R,
    return_time: u64,
}

enum EntryKind<'a, C, R> {
    Call(&'a Operation<C, R>),
    Return,
}

struct Entry<'a, C, R> {
    kind: EntryKind<'a, C, R>,
    id: usize,
    time: u64,
    other: usize,
}

fn operation_to_entries<'a, C, R>(
    ops: &[&'a Operation<C, R>],
) -> Vec<Entry<'a, C, R>> {
    let mut result = vec![];
    for op in ops {
        let id = result.len() >> 1;
        result.push(Entry {
            kind: EntryKind::Return,
            id,
            time: op.return_time,
            other: 0,
        });
        result.push(Entry {
            kind: EntryKind::Call(op),
            id,
            time: op.call_time,
            other: 0,
        });
    }
    result.sort_by_cached_key(|e| e.time);
    let mut this = vec![0; ops.len()];
    let mut that = vec![0; ops.len()];
    for (index, entry) in result.iter().enumerate() {
        match entry.kind {
            EntryKind::Call(_) => this[entry.id] = index,
            EntryKind::Return => that[entry.id] = index,
        }
    }
    for i in 0..ops.len() {
        result[this[i]].other = that[i];
        result[that[i]].other = this[i];
    }
    result
}

fn check_history<T: Model>(
    ops: &[&Operation<<T as Model>::Input, <T as Model>::Output>],
) -> bool {
    let entries = operation_to_entries(ops);
    let mut list = OffsetLinkedList::create(entries);

    let mut all = HashSet::new();
    let mut stack = vec![];

    let mut flag = BitSet::new();
    let mut leg = list.first().expect("Linked list should not be empty");
    let mut curr = T::create();
    while !list.is_empty() {
        let entry = list.get(leg);
        let other = NodeRef(entry.other);
        match entry.kind {
            EntryKind::Call(ops) => {
                let mut next = curr.clone();
                if next.step(&ops.call_op, &ops.return_op) {
                    let mut next_flag = flag.clone();
                    next_flag.insert(entry.id);
                    if all.insert((next_flag.clone(), next.clone())) {
                        std::mem::swap(&mut curr, &mut next);
                        std::mem::swap(&mut flag, &mut next_flag);
                        stack.push((leg, next, next_flag));

                        list.lift(leg);
                        list.lift(other);

                        if let Some(first) = list.first() {
                            leg = first;
                        } else {
                            break;
                        }
                    } else {
                        leg = list
                            .succ(leg)
                            .expect("There should be another element");
                    }
                } else {
                    leg = list
                        .succ(leg)
                        .expect("There should be another element");
                }
            }
            EntryKind::Return => {
                if stack.is_empty() {
                    return false;
                }
                let (prev_leg, prev, prev_flag) = stack.pop().unwrap();
                leg = prev_leg;
                curr = prev;
                flag = prev_flag;

                list.unlift(leg);
            }
        }
    }
    true
}

pub fn check_operations_timeout<T: Model>(
    history: &'static [Operation<<T as Model>::Input, <T as Model>::Output>],
    _: Option<Duration>,
) -> bool
where
    <T as Model>::Input: Sync,
    <T as Model>::Output: Sync,
{
    let mut results = vec![];
    for sub_history in T::partition(history) {
        results
            .push(std::thread::spawn(move || check_history::<T>(&sub_history)));
    }
    let mut failed = vec![];
    for (index, result) in results.into_iter().enumerate() {
        let result = result.join().expect("Search thread should never panic");
        if !result {
            eprintln!("Partition {} failed.", index);
            failed.push(index);
        }
    }
    failed.is_empty()
}

#[cfg(test)]
mod tests {
    use crate::{check_operations_timeout, Model, Operation};

    #[derive(Clone, Eq, PartialEq, Hash)]
    struct CountingModel {
        base: usize,
        cnt: usize,
    }

    impl Model for CountingModel {
        type Input = usize;
        type Output = usize;

        fn create() -> Self {
            Self { base: 0, cnt: 0 }
        }

        fn step(&mut self, input: &Self::Input, output: &Self::Output) -> bool {
            if self.base == 0 && *input != 0 && *output == 1 {
                self.base = *input;
                self.cnt = 1;
                true
            } else if self.base == *input && self.cnt + 1 == *output {
                self.cnt += 1;
                true
            } else {
                false
            }
        }
    }
    #[test]
    fn no_accept() {
        let ops = Box::leak(Box::new(vec![]));
        for i in 0..4 {
            ops.push(Operation {
                call_op: 0usize,
                call_time: 0,
                return_op: i as usize,
                return_time: i,
            });
        }
        assert!(!check_operations_timeout::<CountingModel>(ops, None));
    }

    #[test]
    fn accept() {
        let mut ops = Box::leak(Box::new(vec![]));
        for i in 0..4 {
            ops.push(Operation {
                call_op: 1usize,
                call_time: i * 2,
                return_op: (i + 1) as usize,
                return_time: i + 4,
            });
        }
        assert!(check_operations_timeout::<CountingModel>(ops, None));
    }
}
