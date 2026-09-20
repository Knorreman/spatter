#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RddId(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ShuffleId(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Dependency {
    Narrow { parent: RddId },
    Shuffle { parent: RddId, shuffle: ShuffleId },
}

impl Dependency {
    pub fn parent(self) -> RddId {
        match self {
            Dependency::Narrow { parent } | Dependency::Shuffle { parent, .. } => parent,
        }
    }
}
