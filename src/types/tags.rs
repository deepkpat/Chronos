use std::collections::BTreeMap;

/// Canonical tag set. We use a BTreeMap because it maintains
/// lexicographically sorted key order. This guarantees that equivalent
/// label sets (e.g., `host=A,region=US` and `region=US,host=A`)
/// produce identical byte fingerprints.
pub type Tags = BTreeMap<String, String>;
