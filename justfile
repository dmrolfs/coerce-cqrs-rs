# simple project just file
# see https://github.com/casey/just
# Find instructions on how to set up just locally at: https://just.systems/man/en/

set dotenv-load

alias c := full-check
alias u := update
#alias r := run
#alias d := build-docker
#alias drl := docker-run-local

default:
  just --list

full-check:
  cargo fmt
  cargo check
  cargo clippy

update:
  cargo upgrade --workspace
  cargo update

init-db:
  ./scripts/init_db.sh

#run:
#  RUST_BACKTRACE=full RUST_LOG="debug,coerce_cqrs=trace" cargo run -- --secrets ./resources/secrets.yaml | bunyan

#  cargo test
#  docker build --tag services --file Dockerfile
