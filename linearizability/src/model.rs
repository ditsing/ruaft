use std::collections::HashMap;

use crate::Operation;

pub trait Model:
    std::cmp::Eq + std::clone::Clone + std::hash::Hash + std::fmt::Debug
{
    type Input: std::fmt::Debug;
    type Output: std::fmt::Debug;

    fn create() -> Self;
    fn partition(
        history: &[Operation<Self::Input, Self::Output>],
    ) -> Vec<Vec<&Operation<Self::Input, Self::Output>>> {
        let history: Vec<&Operation<Self::Input, Self::Output>> =
            history.iter().map(|e| e).collect();
        return vec![history];
    }
    fn step(&mut self, input: &Self::Input, output: &Self::Output) -> bool;
}

#[derive(Clone, Debug)]
pub enum KvOp {
    Get,
    Put,
    Append,
}

#[derive(Clone, Debug)]
pub struct KvInput {
    pub op: KvOp,
    pub key: String,
    pub value: String,
}
pub type KvOutput = String;

unsafe impl Sync for KvInput {}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct KvModel {
    expected_output: String,
}

impl Model for KvModel {
    type Input = KvInput;
    type Output = KvOutput;

    fn create() -> Self {
        KvModel {
            expected_output: String::new(),
        }
    }

    fn partition(
        history: &[Operation<KvInput, KvOutput>],
    ) -> Vec<Vec<&Operation<KvInput, KvOutput>>> {
        let mut by_key =
            HashMap::<String, Vec<&Operation<KvInput, KvOutput>>>::new();
        for op in history.into_iter() {
            by_key.entry(op.call_op.key.clone()).or_default().push(op);
        }
        let mut result = vec![];
        for (_, values) in by_key {
            result.push(values);
        }
        result
    }

    fn step(&mut self, input: &KvInput, output: &KvOutput) -> bool {
        match input.op {
            KvOp::Get => self.expected_output == *output,
            KvOp::Put => {
                self.expected_output = input.value.clone();
                true
            }
            KvOp::Append => {
                self.expected_output += &input.value;
                true
            }
        }
    }
}
