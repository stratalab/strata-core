use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{Command, Error, Output};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Request {
    pub(crate) id: u64,
    pub(crate) command: Command,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Response {
    pub(crate) id: u64,
    pub(crate) result: Result<Output, Error>,
}

pub(crate) fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, rmp_serde::encode::Error> {
    rmp_serde::to_vec_named(value)
}

pub(crate) fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, rmp_serde::decode::Error> {
    rmp_serde::from_slice(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_round_trip() {
        let request = Request {
            id: 7,
            command: Command::Ping,
        };
        let encoded = encode(&request).expect("request should encode");
        let decoded: Request = decode(&encoded).expect("request should decode");
        assert_eq!(decoded.id, 7);
        assert!(matches!(decoded.command, Command::Ping));
    }

    #[test]
    fn response_round_trip() {
        let response = Response {
            id: 3,
            result: Ok(Output::Pong {
                version: "1.2.3".to_string(),
            }),
        };
        let encoded = encode(&response).expect("response should encode");
        let decoded: Response = decode(&encoded).expect("response should decode");
        assert_eq!(decoded.id, 3);
        assert!(matches!(decoded.result, Ok(Output::Pong { .. })));
    }
}
