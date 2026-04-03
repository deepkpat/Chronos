use std::sync::Arc;

use chronos::codec::registry::CodecRegistry;

#[test]
fn test_singleton() {
    let reg1 = CodecRegistry::instance();
    let reg2 = CodecRegistry::instance();
    assert!(Arc::ptr_eq(&reg1, &reg2));
}

#[test]
fn test_pre_registered_codecs() {
    let registry = CodecRegistry::instance();
    // json codec should be pre-registered
    let codec = registry.get("json");
    assert!(codec.is_ok());
    assert_eq!(codec.unwrap().name(), "json");
}
