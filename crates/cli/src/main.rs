//! Thin command-line shell over `strata-executor`.

mod admin;
mod app;
mod context;
mod init;
mod open;
mod parse;
mod render;
mod repl;
mod request;

fn main() {
    let code = app::run(std::env::args_os());
    std::process::exit(code);
}
