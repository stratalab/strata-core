//! Length-prefixed frame protocol for IPC communication.
//!
//! Each frame is a 4-byte big-endian u32 length followed by `length` bytes
//! of payload. Maximum frame size is 64 MB.

use std::io::{self, Read, Write};

/// Maximum frame payload size: 64 MB.
const MAX_FRAME_SIZE: u32 = 64 * 1024 * 1024;

/// Write a length-prefixed frame to the stream.
pub fn write_frame(stream: &mut impl Write, payload: &[u8]) -> io::Result<()> {
    let len = payload.len();
    if len > MAX_FRAME_SIZE as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("frame payload too large: {} bytes (max {})", len, MAX_FRAME_SIZE),
        ));
    }
    let len_bytes = (len as u32).to_be_bytes();
    stream.write_all(&len_bytes)?;
    stream.write_all(payload)?;
    stream.flush()
}

/// Read a length-prefixed frame from the stream.
///
/// Returns the payload bytes. Returns an error if the frame exceeds the
/// 64 MB cap or the stream is closed.
pub fn read_frame(stream: &mut impl Read) -> io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let len = u32::from_be_bytes(len_buf);

    if len > MAX_FRAME_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame too large: {} bytes (max {})", len, MAX_FRAME_SIZE),
        ));
    }

    let mut payload = vec![0u8; len as usize];
    stream.read_exact(&mut payload)?;
    Ok(payload)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn round_trip() {
        let data = b"hello world";
        let mut buf = Vec::new();
        write_frame(&mut buf, data).unwrap();

        let mut cursor = Cursor::new(buf);
        let result = read_frame(&mut cursor).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn empty_frame() {
        let mut buf = Vec::new();
        write_frame(&mut buf, b"").unwrap();

        let mut cursor = Cursor::new(buf);
        let result = read_frame(&mut cursor).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn oversized_write_rejected() {
        let data = vec![0u8; (MAX_FRAME_SIZE as usize) + 1];
        let mut buf = Vec::new();
        let err = write_frame(&mut buf, &data).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn oversized_read_rejected() {
        let bad_len = (MAX_FRAME_SIZE + 1).to_be_bytes();
        let mut cursor = Cursor::new(bad_len.to_vec());
        let err = read_frame(&mut cursor).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
