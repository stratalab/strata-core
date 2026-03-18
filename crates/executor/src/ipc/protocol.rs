//! IPC request/response protocol types.
//!
//! Messages are serialized with MessagePack (rmp-serde) for compact binary
//! encoding, then framed with length-prefix wire protocol.

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{Command, Error, Output};

/// An IPC request from client to server.
#[derive(Debug, Serialize, Deserialize)]
pub struct Request {
    /// Monotonically increasing request ID for correlation.
    pub id: u64,
    /// The command to execute.
    pub command: Command,
}

/// An IPC response from server to client.
#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    /// Matches the request ID.
    pub id: u64,
    /// The result of executing the command.
    pub result: Result<Output, Error>,
}

/// Encode a value to MessagePack bytes.
pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, rmp_serde::encode::Error> {
    rmp_serde::to_vec_named(value)
}

/// Decode a value from MessagePack bytes.
pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, rmp_serde::decode::Error> {
    rmp_serde::from_slice(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_round_trip() {
        let req = Request {
            id: 42,
            command: Command::Ping,
        };
        let bytes = encode(&req).unwrap();
        let decoded: Request = decode(&bytes).unwrap();
        assert_eq!(decoded.id, 42);
        assert!(matches!(decoded.command, Command::Ping));
    }

    #[test]
    fn response_ok_round_trip() {
        let resp = Response {
            id: 1,
            result: Ok(Output::Pong {
                version: "0.1.0".to_string(),
            }),
        };
        let bytes = encode(&resp).unwrap();
        let decoded: Response = decode(&bytes).unwrap();
        assert_eq!(decoded.id, 1);
        assert!(decoded.result.is_ok());
    }

    #[test]
    fn response_err_round_trip() {
        let resp = Response {
            id: 2,
            result: Err(Error::KeyNotFound {
                key: "missing".to_string(),
                hint: None,
            }),
        };
        let bytes = encode(&resp).unwrap();
        let decoded: Response = decode(&bytes).unwrap();
        assert_eq!(decoded.id, 2);
        assert!(decoded.result.is_err());
    }
}
