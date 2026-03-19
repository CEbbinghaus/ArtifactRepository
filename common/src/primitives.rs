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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mode_round_trip_tree() {
        let m = Mode::from_str("040000").unwrap();
        assert_eq!(m.as_str(), "040000");
    }

    #[test]
    fn mode_round_trip_normal() {
        let m = Mode::from_str("100644").unwrap();
        assert_eq!(m.as_str(), "100644");
    }

    #[test]
    fn mode_round_trip_executable() {
        let m = Mode::from_str("100755").unwrap();
        assert_eq!(m.as_str(), "100755");
    }

    #[test]
    fn mode_round_trip_symlink() {
        let m = Mode::from_str("120000").unwrap();
        assert_eq!(m.as_str(), "120000");
    }

    #[test]
    fn mode_unknown_returns_none() {
        assert!(Mode::from_str("999999").is_none());
        assert!(Mode::from_str("").is_none());
        assert!(Mode::from_str("blob").is_none());
    }

    #[test]
    fn mode_display_matches_as_str() {
        let m = Mode::Normal;
        assert_eq!(format!("{}", m), m.as_str());
    }

    #[test]
    fn object_type_round_trip_blob() {
        let t = ObjectType::from_str("blob").unwrap();
        assert_eq!(t.to_str(), "blob");
    }

    #[test]
    fn object_type_round_trip_tree() {
        let t = ObjectType::from_str("tree").unwrap();
        assert_eq!(t.to_str(), "tree");
    }

    #[test]
    fn object_type_round_trip_index() {
        let t = ObjectType::from_str("indx").unwrap();
        assert_eq!(t.to_str(), "indx");
    }

    #[test]
    fn object_type_unknown_returns_none() {
        assert!(ObjectType::from_str("unknown").is_none());
        assert!(ObjectType::from_str("").is_none());
        assert!(ObjectType::from_str("BLOB").is_none());
    }
}
