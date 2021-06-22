use std::mem::MaybeUninit;

struct Node<T> {
    prev: usize,
    succ: usize,
    data: MaybeUninit<T>,
}

impl<T> Default for Node<T> {
    fn default() -> Self {
        Self {
            prev: 0,
            succ: 0,
            data: MaybeUninit::uninit(),
        }
    }
}

pub struct OffsetLinkedList<T> {
    nodes: Vec<Node<T>>,
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub struct NodeRef(pub usize);

impl NodeRef {
    fn from_index(index: usize) -> Option<Self> {
        if index == OffsetLinkedList::<()>::HEAD {
            None
        } else {
            Some(Self(index - 1))
        }
    }
}

pub struct Iter<'a, T> {
    list: &'a OffsetLinkedList<T>,
    index: usize,
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index == OffsetLinkedList::<()>::HEAD {
            None
        } else {
            let node = self.list.at(self.index);
            self.index = node.succ;
            Some(unsafe { &*node.data.as_ptr() })
        }
    }
}

impl<T> OffsetLinkedList<T> {
    const HEAD: usize = 0;

    pub fn create(data: Vec<T>) -> Self {
        let len = data.len();
        let mut nodes = Vec::with_capacity(len + 1);
        for _ in 0..len + 1 {
            nodes.push(Node::default());
        }
        for (i, data) in data.into_iter().enumerate() {
            nodes[i].succ = i + 1;
            nodes[i + 1].prev = i;
            nodes[i + 1].data = MaybeUninit::new(data);
        }
        nodes[Self::HEAD].prev = len;
        nodes[len].succ = Self::HEAD;
        Self { nodes }
    }

    fn offset_index(&self, index: NodeRef) -> usize {
        assert!(index.0 + 1 < self.nodes.len());
        index.0 + 1
    }

    pub fn lift(&mut self, index: NodeRef) {
        let index = self.offset_index(index);
        let prev = self.nodes[index].prev;
        let succ = self.nodes[index].succ;
        self.nodes[prev].succ = succ;
        self.nodes[succ].prev = prev;
    }

    pub fn unlift(&mut self, index: NodeRef) {
        let index = self.offset_index(index);
        let prev = self.nodes[index].prev;
        let succ = self.nodes[index].succ;
        self.nodes[prev].succ = index;
        self.nodes[succ].prev = index;
    }

    pub fn get(&self, index: NodeRef) -> &T {
        let index = self.offset_index(index);
        unsafe { &*self.nodes[index].data.as_ptr() }
    }

    #[allow(dead_code)]
    pub fn prev(&self, index: NodeRef) -> Option<NodeRef> {
        let index = self.offset_index(index);
        let succ = self.nodes[index].prev;
        NodeRef::from_index(succ)
    }

    pub fn succ(&self, index: NodeRef) -> Option<NodeRef> {
        let index = self.offset_index(index);
        NodeRef::from_index(self.nodes[index].succ)
    }

    pub fn first(&self) -> Option<NodeRef> {
        NodeRef::from_index(self.nodes[Self::HEAD].succ)
    }

    #[allow(dead_code)]
    pub fn last(&self) -> Option<NodeRef> {
        NodeRef::from_index(self.nodes[Self::HEAD].prev)
    }

    pub fn is_empty(&self) -> bool {
        self.nodes[Self::HEAD].succ == Self::HEAD
    }

    fn at(&self, index: usize) -> &Node<T> {
        &self.nodes[index]
    }

    #[allow(dead_code)]
    pub fn iter(&self) -> Iter<'_, T> {
        Iter {
            list: self,
            index: 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::offset_linked_list::{NodeRef, OffsetLinkedList};

    fn make_list() -> OffsetLinkedList<char> {
        let data: Vec<char> = ('a'..='z').collect();
        OffsetLinkedList::create(data)
    }

    fn assert_char_list_eq(list: &OffsetLinkedList<char>, ans: &str) {
        let mut list_str = String::new();
        let mut leg = list.first();
        while let Some(curr) = leg {
            list_str.push(*list.get(curr));
            leg = list.succ(curr);
        }
        assert_eq!(&list_str, ans);
    }

    #[test]
    fn linked_list() {
        let mut list = make_list();
        let data_str: String = ('a'..='z').collect();
        assert_char_list_eq(&list, &data_str);

        let mut leg = list.first().unwrap();
        for i in 0..10 {
            if i % 3 == 0 {
                list.lift(leg);
            }
            leg = list.succ(leg).unwrap();
        }
        list.lift(leg);
        assert_char_list_eq(&list, &"bcefhilmnopqrstuvwxyz");

        list.unlift(NodeRef(0));
        list.unlift(NodeRef(3));
        list.unlift(NodeRef(10));
        assert_char_list_eq(&list, &"abcdefhiklmnopqrstuvwxyz");
    }

    #[test]
    fn empty_linked_list() {
        let mut list = make_list();
        assert!(!list.is_empty());

        let mut leg = list.first();
        while let Some(curr) = leg {
            leg = list.succ(curr);
            list.lift(curr);
        }
        assert!(list.is_empty())
    }

    #[test]
    fn iterate_linked_list() {
        let list_str: String = make_list().iter().collect();
        let data_str: String = ('a'..='z').collect();
        assert_eq!(data_str, list_str);
    }
}
