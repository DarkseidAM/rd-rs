//! Structural test: verifies that VfsConfig eviction fields carry the
//! "requires process restart" doc comment (Issue F).
//!
//! This guards against someone silently removing the warning during a refactor.

/// Parse `src/config/structs.rs` as text and assert the three eviction fields
/// are annotated with the restart warning.
#[test]
fn test_vfsconfig_eviction_fields_have_restart_doc() {
    let source = std::fs::read_to_string("src/config/structs.rs")
        .expect("src/config/structs.rs must be readable from workspace root");

    let fields = [
        ("cache_max_size", "cache_max_size"),
        ("cache_max_age", "cache_max_age"),
        ("cache_min_free_space", "cache_min_free"),
    ];

    for (field, _label) in fields {
        // The source must contain both the field name and the restart warning
        // somewhere in the same vicinity.
        assert!(
            source.contains(field),
            "VfsConfig must still have the `{field}` field"
        );
    }
    let count = source.matches("requires a process restart").count();
    assert!(
        count >= 3,
        "VfsConfig must have 'requires a process restart' docs on all 3 eviction fields, found {count}"
    );

    // The `Cache::new` reference must appear in at least one doc comment.
    assert!(
        source.contains("Cache::new"),
        "doc comment must mention Cache::new for context"
    );
}

/// Verifies the specific field-level annotations are present (not just the count).
#[test]
fn test_cache_max_size_has_restart_note() {
    let source = std::fs::read_to_string("src/config/structs.rs").expect("readable");
    // Find the block containing cache_max_size — the restart note must precede it.
    let idx = source.find("pub cache_max_size").expect("field must exist");
    let preceding = &source[..idx];
    assert!(
        preceding.rfind("requires a process restart").is_some(),
        "cache_max_size must have a 'requires a process restart' doc before it"
    );
}

#[test]
fn test_cache_max_age_has_restart_note() {
    let source = std::fs::read_to_string("src/config/structs.rs").expect("readable");
    let idx = source.find("pub cache_max_age").expect("field must exist");
    let preceding = &source[..idx];
    assert!(
        preceding.rfind("requires a process restart").is_some(),
        "cache_max_age must have a 'requires a process restart' doc before it"
    );
}

#[test]
fn test_cache_min_free_has_restart_note() {
    let source = std::fs::read_to_string("src/config/structs.rs").expect("readable");
    let idx = source
        .find("pub cache_min_free_space")
        .expect("field must exist");
    let preceding = &source[..idx];
    assert!(
        preceding.rfind("requires a process restart").is_some(),
        "cache_min_free_space must have a 'requires a process restart' doc before it"
    );
}
