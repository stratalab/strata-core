use std::io::{self, Read, Write};

const MAX_FRAME_SIZE: u32 = 64 * 1024 * 1024;

pub(crate) fn write_frame(stream: &mut impl Write, payload: &[u8]) -> io::Result<()> {
    let len = payload.len();
    if len > MAX_FRAME_SIZE as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "frame payload too large: {} bytes (max {})",
                len, MAX_FRAME_SIZE
            ),
        ));
    }

    stream.write_all(&(len as u32).to_be_bytes())?;
    stream.write_all(payload)?;
    stream.flush()
}

pub(crate) fn read_frame(stream: &mut impl Read) -> io::Result<Vec<u8>> {
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
    use std::io::Cursor;

    use super::{read_frame, write_frame, MAX_FRAME_SIZE};

    #[test]
    fn round_trip() {
        let mut bytes = Vec::new();
        write_frame(&mut bytes, b"hello").expect("frame should write");

        let mut cursor = Cursor::new(bytes);
        let payload = read_frame(&mut cursor).expect("frame should read");
        assert_eq!(payload, b"hello");
    }

    #[test]
    fn oversized_write_is_rejected() {
        let bytes = vec![0u8; MAX_FRAME_SIZE as usize + 1];
        let err = write_frame(&mut Vec::new(), &bytes).expect_err("oversized frame should fail");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }
}
