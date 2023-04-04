// use once_cell::sync::Lazy;
// use tracing::{subscriber, Subscriber};
// use tracing_bunyan_formatter::{BunyanFormattingLayer, JsonStorageLayer};
// use tracing_log::LogTracer;
// use tracing_subscriber::{EnvFilter, Registry, layer::SubscriberExt};
// use tracing_subscriber::fmt::MakeWriter;
//
// #[allow(dead_code)]
// pub static TEST_TRACING: Lazy<()> = Lazy::new(|| {
//     let default_filter_level = "info";
//     let subscriber_name = "test";
//     if std::env::var("TEST_LOG").is_ok() {
//         let subscriber = get_subscriber(subscriber_name, default_filter_level, std::io::stdout);
//         init_subscriber(subscriber);
//     } else {
//         let subscriber = get_subscriber(subscriber_name, default_filter_level, std::io::sink);
//         init_subscriber(subscriber);
//     };
// });
//
// #[allow(unused)]
// pub fn get_subscriber<S0, S1, W>(name: S0, env_filter: S1, sink: W) -> impl Subscriber + Sync + Send
//     where
//         S0: Into<String>,
//         S1: AsRef<str>,
//         W: for<'a> MakeWriter<'a> + Send + Sync + 'static,
// {
//     let env_filter =
//         EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(env_filter));
//
//     let formatting_layer = BunyanFormattingLayer::new(name.into(), sink);
//
//     Registry::default()
//         .with(env_filter)
//         .with(JsonStorageLayer)
//         .with(formatting_layer)
// }
//
// pub fn init_subscriber(subscriber: impl Subscriber + Sync + Send) {
//     LogTracer::init().expect("Failed to set logger");
//     subscriber::set_global_default(subscriber).expect("Failed to set subscriber");
// }
