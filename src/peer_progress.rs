use crate::Index;

#[derive(Default)]
#[repr(align(64))]
pub(crate) struct PeerProgress {
    next_index: Index,
    current_step: i64,
}

impl PeerProgress {
    pub fn reset_progress(&mut self, next_index: Index) {
        assert!(next_index > 0, "Cannot reset next index to zero");
        self.next_index = next_index;
        self.current_step = 0;
    }

    pub fn record_failure(&mut self, committed_index: Index) {
        let step = &mut self.current_step;
        if *step < 5 {
            *step += 1;
        }
        let diff = 4 << *step;

        let next_index = &mut self.next_index;
        if diff >= *next_index {
            *next_index = 1usize;
        } else {
            *next_index -= diff;
        }

        if *next_index < committed_index {
            *next_index = committed_index;
        }
    }

    pub fn record_success(&mut self, match_index: Index) {
        self.next_index = match_index + 1;
        self.current_step = 0;
    }

    pub fn next_index(&self) -> Index {
        self.next_index
    }
}
