//! Local IPC transport for command execution.

mod client;
mod protocol;
mod server;
mod wire;

pub(crate) use client::IpcClient;
pub use server::IpcServer;
