mod view_applicator;
mod view_memory_storage;
mod view_storage;

pub use view_applicator::ViewApplicator;
pub use view_memory_storage::InMemoryViewStorage;
pub use view_storage::ViewStorage;

// use serde::{Deserialize, Serialize};
// use smol_str::SmolStr;
// use std::fmt;

// #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
// #[repr(transparent)]
// #[serde(transparent)]
// pub struct ViewId(SmolStr);
//
// impl fmt::Display for ViewId {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         write!(f, "{}", self.0)
//     }
// }
//
// impl ViewId {
//     pub fn new(id: impl AsRef<str>) -> Self {
//         Self(SmolStr::new(id))
//     }
// }
//
// impl From<&str> for ViewId {
//     fn from(id: &str) -> Self {
//         Self::new(id)
//     }
// }
//
// impl From<String> for ViewId {
//     fn from(id: String) -> Self {
//         Self::new(id)
//     }
// }
//
// impl AsRef<str> for ViewId {
//     fn as_ref(&self) -> &str {
//         self.0.as_str()
//     }
// }

// impl<'q, DB: Database> Encode<'q, DB> for ViewId {
//     fn encode_by_ref(&self, buf: &mut <DB as HasArguments<'q>>::ArgumentBuffer) -> IsNull {
//         <String as sqlx::Encode<'q, DB>>::encode_by_ref(&self.0.to_string(), buf)
//         // self.0.to_string().encode_by_ref(buf)
//     }
// }
//
// impl<DB: Database> sqlx::Type<DB> for ViewId {
//     fn type_info() -> DB::TypeInfo {
//         // <&str as sqlx::Type<DB>>::type_info()
//         <String as sqlx::Type<DB>>::type_info()
//     }
//
//     fn compatible(ty: &DB::TypeInfo) -> bool {
//         // <&str as sqlx::Type<DB>>::compatible(ty)
//         <String as sqlx::Type<DB>>::compatible(ty)
//     }
// }

// /// Context updating views
// #[derive(Debug, Clone, PartialEq, Eq)]
// pub struct ViewContext {
//     /// name of the class of view
//     pub view_name: SmolStr,
//
//     /// current version of the view instance, used for optimistic locking.
//     pub version: i64,
// }
//
// impl ViewContext {
//     // pub fn new(view_name: impl AsRef<str>, view_id: impl Into<ViewId>, version: i64) -> Self {
//     pub fn new(view_name: impl AsRef<str>, version: i64) -> Self {
//         Self {
//             view_name: SmolStr::new(view_name),
//             // view_id: view_id.into(),
//             version,
//         }
//     }
// }
