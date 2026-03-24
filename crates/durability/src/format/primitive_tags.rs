//! Canonical primitive type tags for on-disk formats.
//!
//! These tags identify primitive types in snapshots and writesets.
//! A single definition prevents divergence between format modules.

/// Key-Value store
pub const KV: u8 = 0x01;
/// Event log
pub const EVENT: u8 = 0x02;
// 0x03 was State (StateCell removed, never shipped)
// 0x04 was Trace — reserved, intentionally skipped
/// Branch
pub const BRANCH: u8 = 0x05;
/// JSON document
pub const JSON: u8 = 0x06;
/// Vector embedding
pub const VECTOR: u8 = 0x07;

/// All valid primitive tags in order
pub const ALL_TAGS: [u8; 5] = [KV, EVENT, BRANCH, JSON, VECTOR];

/// Get the tag name for display
pub fn tag_name(tag: u8) -> &'static str {
    match tag {
        KV => "KV",
        EVENT => "Event",
        BRANCH => "Branch",
        JSON => "Json",
        VECTOR => "Vector",
        _ => "Unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_tags_unique() {
        for (i, a) in ALL_TAGS.iter().enumerate() {
            for b in ALL_TAGS.iter().skip(i + 1) {
                assert_ne!(a, b, "duplicate tag value: 0x{:02x}", a);
            }
        }
    }

    #[test]
    fn tag_names() {
        assert_eq!(tag_name(KV), "KV");
        assert_eq!(tag_name(EVENT), "Event");
        assert_eq!(tag_name(BRANCH), "Branch");
        assert_eq!(tag_name(JSON), "Json");
        assert_eq!(tag_name(VECTOR), "Vector");
        assert_eq!(tag_name(0xFF), "Unknown");
    }

    #[test]
    fn all_tags_complete() {
        assert_eq!(ALL_TAGS.len(), 5);
        assert_eq!(ALL_TAGS, [KV, EVENT, BRANCH, JSON, VECTOR]);
    }
}
