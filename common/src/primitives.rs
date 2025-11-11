use std::fmt::Display;
use crate::{BLOB_KEY, INDEX_KEY, TREE_KEY};


#[derive(Debug)]
pub enum Mode {
    Tree = 040000,
    Normal = 100644,
    Executable = 100755,
    SymbolicLink = 120000,
}

const TREE_MODE: &str = "040000";
const NORMAL_MODE: &str = "100644";
const EXECUTABLE_MODE: &str = "100755";
const SYMBOLIC_LINK_MODE: &str = "120000";

impl Mode {
    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            TREE_MODE => Some(Mode::Tree),
            NORMAL_MODE => Some(Mode::Normal),
            EXECUTABLE_MODE => Some(Mode::Executable),
            SYMBOLIC_LINK_MODE => Some(Mode::SymbolicLink),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Tree => TREE_MODE,
            Self::Normal => NORMAL_MODE,
            Self::Executable => EXECUTABLE_MODE,
            Self::SymbolicLink => SYMBOLIC_LINK_MODE,
        }
    }
}

impl Display for Mode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            self.as_str()
        )
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ObjectType {
    Blob,
    Tree,
    Index,
}

impl ObjectType {
    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            BLOB_KEY => Some(Self::Blob),
            TREE_KEY => Some(Self::Tree),
            INDEX_KEY => Some(Self::Index),
            _ => None,
        }
    }

    pub fn to_str(&self) -> &'static str {
        match self {
            Self::Blob => BLOB_KEY,
            Self::Index => INDEX_KEY,
            Self::Tree => TREE_KEY,
        }
    }
}
