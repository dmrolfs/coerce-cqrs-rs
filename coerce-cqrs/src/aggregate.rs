use coerce::actor::context::ActorContext;
use coerce::actor::message::Message;
use either::{Either, Left, Right};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use thiserror::Error;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SnapshotTrigger {
    None,
    OnEventCount {
        nr_events: u64,
        after_nr_events: u64,
    },
}

impl SnapshotTrigger {
    pub const fn none() -> Self {
        Self::None
    }

    pub const fn on_event_count(after_nr_events: u64) -> Self {
        Self::OnEventCount {
            nr_events: 0,
            after_nr_events,
        }
    }

    /// Increments the event count for the snapshot trigger and returns true
    /// if a snapshot should be taken.
    pub fn incr(&mut self) -> bool {
        match self {
            Self::None => false,
            Self::OnEventCount {
                ref mut nr_events,
                after_nr_events,
            } => {
                *nr_events += 1;
                *nr_events % *after_nr_events == 0
            }
        }
    }
}

#[derive(Debug, PartialEq, PartialOrd, Eq, Serialize, Deserialize)]
#[allow(dead_code)]
pub enum CommandResult<T, E> {
    Ok(T),
    Rejected(String),
    Err(E),
}

// impl<T> Eq for CommandResult<T> where T: Debug + PartialEq + Eq {}

impl<T, E> CommandResult<T, E>
// where
//     T: Debug + PartialEq + Serialize + DeserializeOwned,
{
    #[must_use = "if you intended to assert that this is ok, consider `.unwrap()` instead"]
    #[inline]
    pub const fn is_ok(&self) -> bool {
        matches!(*self, Self::Ok(_))
    }

    /// Returns `true` if the result is [`Ok`] and the value inside of it matches a predicate.
    ///
    /// # Examples
    ///
    /// ```
    /// use coerce_cqrs::CommandResult;
    ///
    /// let x: CommandResult<u32, &str> = CommandResult::Ok(2);
    /// assert_eq!(x.is_ok_and(|x| x > 1), true);
    ///
    /// let x: CommandResult<u32, &str> = CommandResult::Ok(0);
    /// assert_eq!(x.is_ok_and(|x| x > 1), false);
    ///
    /// let x: CommandResult<u32, &str> = CommandResult::Err("hey");
    /// assert_eq!(x.is_ok_and(|x| x > 1), false);
    /// ```
    #[must_use]
    #[inline]
    pub fn is_ok_and(self, f: impl FnOnce(T) -> bool) -> bool {
        match self {
            Self::Rejected(_) | Self::Err(_) => false,
            Self::Ok(x) => f(x),
        }
    }

    /// Returns `true` if the result is [`Rejected`].
    ///
    /// # Examples
    ///
    /// ```
    /// use coerce_cqrs::CommandResult;
    ///
    /// let x: CommandResult<i32, &str> = CommandResult::Ok(-3);
    /// assert_eq!(x.is_rejected(), false);
    ///
    /// let x: CommandResult<i32, &str> = CommandResult::Err("Some error message");
    /// assert_eq!(x.is_rejected(), false);
    ///
    /// let x: CommandResult<i32, &str> = CommandResult::Rejected("command not allowed".to_string());
    /// assert_eq!(x.is_rejected(), true);
    /// ```
    #[must_use = "if you intended to assert that this is rejected, consider `.unwrap_rejected()` instead"]
    #[inline]
    pub const fn is_rejected(&self) -> bool {
        matches!(*self, Self::Rejected(_))
    }

    /// Returns `true` if the result is [`Err`].
    ///
    /// # Examples
    ///
    /// ```
    /// use coerce_cqrs::CommandResult;
    ///
    /// let x: CommandResult<i32, &str> = CommandResult::Ok(-3);
    /// assert_eq!(x.is_err(), false);
    ///
    /// let x: CommandResult<i32, &str> = CommandResult::Err("Some error message");
    /// assert_eq!(x.is_err(), true);
    /// ```
    #[must_use = "if you intended to assert that this is err, consider `.unwrap_err()` instead"]
    #[inline]
    pub const fn is_err(&self) -> bool {
        matches!(*self, Self::Err(_))
    }

    /// Returns `true` if the result is [`Err`] and the value inside of it matches a predicate.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::io::{Error, ErrorKind};
    /// use std::process::Command;
    /// use coerce_cqrs::CommandResult;
    ///
    /// let x: CommandResult<u32, Error> = CommandResult::Err(Error::new(ErrorKind::NotFound, "!"));
    /// assert_eq!(x.is_err_and(|x| x.kind() == ErrorKind::NotFound), true);
    ///
    /// let x: CommandResult<u32, Error> = CommandResult::Err(Error::new(ErrorKind::PermissionDenied, "!"));
    /// assert_eq!(x.is_err_and(|x| x.kind() == ErrorKind::NotFound), false);
    ///
    /// let x: CommandResult<u32, Error> = CommandResult::Ok(123);
    /// assert_eq!(x.is_err_and(|x| x.kind() == ErrorKind::NotFound), false);
    /// ```
    #[must_use]
    #[inline]
    pub fn is_err_and(self, f: impl FnOnce(E) -> bool) -> bool {
        match self {
            Self::Ok(_) | Self::Rejected(_) => false,
            Self::Err(e) => f(e),
        }
    }

    pub const fn ok(payload: T) -> Self {
        Self::Ok(payload)
    }

    pub fn rejected(message: impl Into<String>) -> Self {
        Self::Rejected(message.into())
    }

    pub const fn err(error: E) -> Self {
        Self::Err(error)
    }

    #[allow(clippy::missing_const_for_fn)]
    #[inline]
    pub fn as_ok(self) -> Option<T> {
        match self {
            Self::Ok(x) => Some(x),
            Self::Rejected(_) | Self::Err(_) => None,
        }
    }

    #[allow(clippy::missing_const_for_fn)]
    #[inline]
    pub fn as_rejected(self) -> Option<String> {
        match self {
            Self::Rejected(msg) => Some(msg),
            Self::Ok(_) | Self::Err(_) => None,
        }
    }

    #[allow(clippy::missing_const_for_fn)]
    #[inline]
    pub fn as_err(self) -> Option<E> {
        match self {
            Self::Err(x) => Some(x),
            Self::Ok(_) | Self::Rejected(_) => None,
        }
    }

    /// Maps a `CommandResult<T, E>` to `CommandResult<U, E>` by applying a function to a
    /// contained [`Ok`] value, leaving an [`Rejected`] or [`Err`] value untouched.
    #[inline]
    pub fn map<U, F: FnOnce(T) -> U>(self, op: F) -> CommandResult<U, E> {
        match self {
            Self::Ok(t) => CommandResult::Ok(op(t)),
            Self::Rejected(msg) => CommandResult::Rejected(msg),
            Self::Err(e) => CommandResult::Err(e),
        }
    }

    /// Returns the provided default (if [`CommandResult::Err`] or [`CommandResult::Rejected`]), or
    /// applies a function to the contained value (if [`CommandResult::Ok`]),
    ///
    /// Arguments passed to `map_or` are eagerly evaluated; if you are passing
    /// the result of a function call, it is recommended to use [`map_or_else`],
    /// which is lazily evaluated.
    ///
    /// [`map_or_else`]: CommandResult::map_or_else
    ///
    /// # Examples
    ///
    /// ```
    /// use coerce_cqrs::CommandResult;
    /// let x: CommandResult<_, &str> = CommandResult::Ok("foo");
    /// assert_eq!(x.map_or(42, |v| v.len()), 3);
    ///
    /// let x: CommandResult<&str, _> = CommandResult::Err("bar");
    /// assert_eq!(x.map_or(42, |v| v.len()), 42);
    /// ```
    #[inline]
    pub fn map_or<U, F: FnOnce(T) -> U>(self, default: U, f: F) -> U {
        match self {
            Self::Ok(t) => f(t),
            Self::Rejected(_) | Self::Err(_) => default,
        }
    }

    /// Maps a `CommandResult<T, E>` to `U` by applying fallback function `default` to
    /// a contained either [`CommandResult::Rejected`] or [`CommandResult::Err`] value, or function
    /// `f` to a contained [`CommandResult::Ok`] value.
    ///
    /// This function can be used to unpack a successful result while handling an error.
    ///
    /// # Examples
    ///
    /// ```
    /// use coerce_cqrs::CommandResult;
    /// let k = 21;
    ///
    /// let x : CommandResult<_, &str> = CommandResult::Ok("foo");
    /// assert_eq!(x.map_or_else(|e| k * 2, |v| v.len()), 3);
    ///
    /// let x : CommandResult<&str, _> = CommandResult::Err("bar");
    /// assert_eq!(x.map_or_else(|e| k * 2, |v| v.len()), 42);
    /// ```
    #[inline]
    pub fn map_or_else<U, D: FnOnce(Either<String, E>) -> U, F: FnOnce(T) -> U>(
        self,
        default: D,
        f: F,
    ) -> U {
        match self {
            Self::Ok(t) => f(t),
            Self::Rejected(msg) => default(Left(msg)),
            Self::Err(e) => default(Right(e)),
        }
    }

    /// Maps a `CommandResult<T, E>` to `CommandResult<T, F>` by applying a function to a
    /// contained [`CommandResult::Err`] value, leaving an [`CommandResult::Ok`] or
    /// [`CommandResult::Rejected`] value untouched.
    ///
    /// This function can be used to pass through a successful result while handling an error.
    ///
    ///
    /// # Examples
    ///
    /// ```
    /// use coerce_cqrs::CommandResult;
    /// fn stringify(x: u32) -> String { format!("error code: {x}") }
    ///
    /// let x: CommandResult<u32, u32> = CommandResult::Ok(2);
    /// assert_eq!(x.map_err(stringify), CommandResult::Ok(2));
    ///
    /// let x: CommandResult<u32, u32> = CommandResult::Err(13);
    /// assert_eq!(x.map_err(stringify), CommandResult::Err("error code: 13".to_string()));
    /// ```
    #[inline]
    pub fn map_err<F, O: FnOnce(E) -> F>(self, op: O) -> CommandResult<T, F> {
        match self {
            Self::Ok(t) => CommandResult::Ok(t),
            Self::Rejected(msg) => CommandResult::Rejected(msg),
            Self::Err(e) => CommandResult::Err(op(e)),
        }
    }

    /// Calls the provided closure with a reference to the contained value (if [`CommandResult::Ok`]).
    #[inline]
    pub fn inspect<F: FnOnce(&T)>(self, f: F) -> Self {
        if let Self::Ok(ref t) = self {
            f(t);
        }

        self
    }

    /// Returns the contained [`CommandResult::Ok`] value, consuming the `self` value.
    ///
    /// Because this function may panic, its use is generally discouraged.
    /// Instead, prefer to use pattern matching and handle the [`CommandResult::Rejected`] or
    /// [`CommandResult::Err`] cases explicitly, or call [`unwrap_or`], [`unwrap_or_else`], or
    /// [`unwrap_or_default`].
    ///
    /// [`unwrap_or`]: CommandResult::unwrap_or
    /// [`unwrap_or_else`]: CommandResult::unwrap_or_else
    /// [`unwrap_or_default`]: CommandResult::unwrap_or_default
    ///
    /// # Panics
    ///
    /// Panics if the value is either an [`CommandResult::Rejected`] or [`CommandResult::Err`],
    /// with a panic message provided by the [`CommandResult::Rejected`] or [`CommandResult::Err`]'s
    /// value.
    ///
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// use coerce_cqrs::CommandResult;
    /// let x: CommandResult<u32, &str> = CommandResult::Ok(2);
    /// assert_eq!(x.unwrap(), 2);
    /// ```
    ///
    /// ```should_panic
    /// use coerce_cqrs::CommandResult;
    /// let x: CommandResult<u32, &str> = CommandResult::Err("emergency failure");
    /// x.unwrap(); // panics with `emergency failure`
    /// ```
    #[inline]
    #[track_caller]
    pub fn unwrap(self) -> T
    where
        E: Debug,
    {
        match self {
            Self::Ok(t) => t,
            Self::Rejected(msg) => {
                panic!("called `CommandResult::unwrap() on a `Rejected` value: {msg}")
            }
            Self::Err(e) => panic!("called `CommandResult::unwrap()` on an `Err` value: {e:?}"),
        }
    }

    /// Returns the contained [`CommandResult::Ok`] value or a default
    ///
    /// Consumes the `self` argument then, if [`CommandResult::Ok`], returns the contained
    /// value, otherwise if [`CommandResult::Rejected`] or [`CommandResult::Err`], returns the
    /// default value for that type.
    #[inline]
    pub fn unwrap_or_default(self) -> T
    where
        T: Default,
    {
        match self {
            Self::Ok(x) => x,
            Self::Rejected(_) | Self::Err(_) => Default::default(),
        }
    }

    /// Returns the contained [`CommandResult::Ok`] value or a provided default.
    ///
    /// Arguments passed to `unwrap_or` are eagerly evaluated; if you are passing
    /// the result of a function call, it is recommended to use [`unwrap_or_else`],
    /// which is lazily evaluated.
    ///
    /// [`unwrap_or_else`]: CommandResult::unwrap_or_else
    ///
    /// # Examples
    ///
    /// ```
    /// use coerce_cqrs::CommandResult;
    /// let default = 2;
    /// let x: CommandResult<u32, &str> = CommandResult::Ok(9);
    /// assert_eq!(x.unwrap_or(default), 9);
    ///
    /// let x: CommandResult<u32, &str> = CommandResult::Err("error");
    /// assert_eq!(x.unwrap_or(default), default);
    /// ```
    #[allow(clippy::missing_const_for_fn)]
    #[inline]
    pub fn unwrap_or(self, default: T) -> T {
        match self {
            Self::Ok(t) => t,
            Self::Rejected(_) | Self::Err(_) => default,
        }
    }

    /// Returns the contained [`CommandResult::Ok`] value or computes it from a closure.
    ///
    ///
    /// # Examples
    ///
    /// ```
    /// use either::Either;
    /// use coerce_cqrs::CommandResult;
    /// fn count(x: Either<String, &str>) -> usize { x.either(|l| l.len() + 1, |r| r.len() * 2) }
    ///
    /// assert_eq!(CommandResult::Ok(2).unwrap_or_else(count), 2);
    /// assert_eq!(CommandResult::Err("foo").unwrap_or_else(count), 6);
    /// ```
    #[inline]
    pub fn unwrap_or_else<F: FnOnce(Either<String, E>) -> T>(self, op: F) -> T {
        match self {
            Self::Ok(t) => t,
            Self::Rejected(msg) => op(Left(msg)),
            Self::Err(e) => op(Right(e)),
        }
    }
}

impl<T, E> From<E> for CommandResult<T, E> {
    fn from(error: E) -> Self {
        Self::Err(error)
    }
}

// pub trait ApplyAggregateEvent<E> {
//     type BaseType;
//     fn apply_event(&mut self, event: E, ctx: &mut ActorContext) -> Option<Self::BaseType>;
// }

pub trait AggregateState<C, E>
where
    C: Message,
{
    type Error;
    type State;

    fn handle_command(
        &self,
        command: C,
        ctx: &mut ActorContext,
    ) -> CommandResult<Vec<E>, Self::Error>;

    fn apply_event(&mut self, event: E, ctx: &mut ActorContext) -> Option<Self::State>;
}

#[derive(Debug, Error)]
#[allow(dead_code)]
pub enum AggregateError {
    #[error("{0}")]
    Persist(#[from] coerce::persistent::journal::PersistErr),
}

#[cfg(test)]
mod tests {
    use crate::postgres::PostgresStorageConfig;
    use crate::projection::{processor::ProcessorSourceProvider, PersistenceId};
    use crate::SnapshotTrigger;
    use claim::{assert_ok, assert_some};
    use coerce::actor::system::ActorSystem;
    use coerce::actor::IntoActor;
    use coerce::persistent::Persistence;
    use coerce_cqrs_test::fixtures::actor::{Msg, TestActor};
    use once_cell::sync::Lazy;
    use pretty_assertions::assert_eq;
    use std::time::Duration;
    use tagid::Entity;

    #[test]
    pub fn test_snapshot_trigger_none() {
        let mut trigger = SnapshotTrigger::none();
        for _ in 0..10 {
            assert_eq!(trigger.incr(), false);
        }
    }

    #[test]
    pub fn test_snapshot_trigger_on_event_count() {
        Lazy::force(&coerce_cqrs_test::setup_tracing::TEST_TRACING);
        let main_span = tracing::info_span!("aggregate::test_snapshot_trigger_on_event_count");
        let _main_span_guard = main_span.enter();

        let mut trigger = SnapshotTrigger::on_event_count(3);
        for i in 1..=10 {
            assert_eq!(trigger.incr(), i % 3 == 0);
        }
    }

    #[test]
    pub fn test_aggregate_recovery() {
        Lazy::force(&coerce_cqrs_test::setup_tracing::TEST_TRACING);
        let main_span = tracing::info_span!("aggregate::test_aggregate_recovery");
        let _main_span_guard = main_span.enter();

        tracing::info!("entering tests...");
        tokio_test::block_on(async move {
            let provider_system = ActorSystem::new();
            let provider_config = PostgresStorageConfig {
                key_prefix: "tests".to_string(),
                username: "postgres".to_string(),
                password: secrecy::Secret::new("demo_pass".to_string()),
                host: "localhost".to_string(),
                port: 5432,
                database_name: "demo_cqrs_db".to_string(),
                event_journal_table_name: PostgresStorageConfig::default_event_journal_table(),
                projection_offsets_table_name:
                    PostgresStorageConfig::default_projection_offsets_table(),
                snapshot_table_name: PostgresStorageConfig::default_snapshot_table(),
                require_ssl: false,
                min_connections: None,
                max_connections: None,
                max_lifetime: None,
                acquire_timeout: Some(Duration::from_secs(10)),
                idle_timeout: None,
            };
            let storage_provider = assert_ok!(
                crate::postgres::PostgresStorageProvider::connect(
                    provider_config,
                    &provider_system
                )
                .await
            );
            let storage = assert_some!(storage_provider.processor_source());
            let system = ActorSystem::new().to_persistent(Persistence::from(storage_provider));
            let create_empty_actor = TestActor::default;

            info!("**** INITIAL AGGREGATE SETUP...");
            let id = TestActor::next_id();
            let actor = assert_ok!(
                create_empty_actor()
                    .into_actor(Some(id.clone()), &system)
                    .await
            );
            let pid: PersistenceId = id.clone().into();
            let journal = storage
                .read_latest_messages(&pid.as_persistence_id(), 0)
                .await;
            info!(?actor, ?journal, "**** before - actor and journal");
            assert_ok!(actor.notify(Msg(1)));
            assert_ok!(actor.notify(Msg(2)));
            assert_ok!(actor.notify(Msg(3)));
            assert_ok!(actor.notify(Msg(4)));

            let actual = assert_ok!(
                actor
                    .exec(|a| {
                        info!("received: {:?}", &a.received_numbers);
                        a.received_numbers.clone()
                    })
                    .await
            );
            info!(?actor, ?journal, "**** after - actor and journal");
            assert_eq!(actual, vec![1, 2, 3, 4]);
            assert_ok!(actor.stop().await);

            info!("**** RECOVER AGGREGATE...");
            let recovered_actor =
                assert_ok!(create_empty_actor().into_actor(Some(id), &system).await);
            info!("recovered_actor: {recovered_actor:?}");
            info!(
                ?recovered_actor,
                ?journal,
                "**** before - recovered_actor and journal"
            );
            let recovered = assert_ok!(
                recovered_actor
                    .exec(|a| {
                        info!("recovered received: {:?}", &a.received_numbers);
                        a.received_numbers.clone()
                    })
                    .await
            );
            info!(
                ?recovered_actor,
                ?journal,
                "**** after - recovered_actor and journal"
            );
            assert_eq!(recovered, vec![1, 2, 3, 4]);

            info!("**** SHUTDOWN...");
            system.shutdown().await;
        })
    }
}
