mod view_applicator;
mod view_memory_storage;
mod view_storage;

pub use view_applicator::ViewApplicator;
pub use view_memory_storage::InMemoryViewStorage;
pub use view_storage::ViewStorage;

use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(transparent)]
#[serde(transparent)]
pub struct ViewId(SmolStr);

impl fmt::Display for ViewId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl ViewId {
    pub fn new(id: impl AsRef<str>) -> Self {
        Self(SmolStr::new(id))
    }
}

impl From<&str> for ViewId {
    fn from(id: &str) -> Self {
        Self::new(id)
    }
}

impl From<String> for ViewId {
    fn from(id: String) -> Self {
        Self::new(id)
    }
}

impl AsRef<str> for ViewId {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

/// Context updating views
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewContext {
    /// name of the class of view
    pub view_name: SmolStr,

    /// unique identifier of the view instance that is being modified.
    // pub view_id: ViewId,

    /// current version of the view instance, used for optimistic locking.
    pub version: i64,
}

impl ViewContext {
    // pub fn new(view_name: impl AsRef<str>, view_id: impl Into<ViewId>, version: i64) -> Self {
    pub fn new(view_name: impl AsRef<str>, version: i64) -> Self {
        Self {
            view_name: SmolStr::new(view_name),
            // view_id: view_id.into(),
            version,
        }
    }
}
