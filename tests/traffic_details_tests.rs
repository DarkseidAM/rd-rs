//! Tests for traffic / bandwidth JSON types.

use rd_rs::rd::types::{TrafficDetailDay, TrafficDetailsResponse};

#[test]
fn deserialize_traffic_details_per_day_shape() {
    let json = r#"{
        "2015-12-09": {
            "host": { "uptobox.com": 11066701819 },
            "bytes": 11066701819
        },
        "2015-12-08": {
            "host": { "uptobox.com": 872664221 },
            "bytes": 872664221
        }
    }"#;
    let m: TrafficDetailsResponse = serde_json::from_str(json).unwrap();
    assert_eq!(m.len(), 2);
    let d = m.get("2015-12-09").unwrap();
    assert_eq!(d.bytes, 11066701819);
    assert_eq!(d.host.get("uptobox.com").copied(), Some(11066701819));
}

#[test]
fn traffic_detail_day_roundtrip() {
    let d = TrafficDetailDay {
        host: [("x.com".into(), 42)].into_iter().collect(),
        bytes: 42,
    };
    let s = serde_json::to_string(&d).unwrap();
    let back: TrafficDetailDay = serde_json::from_str(&s).unwrap();
    assert_eq!(back.bytes, d.bytes);
    assert_eq!(back.host.get("x.com").copied(), Some(42));
}

#[test]
fn deserialize_empty_traffic_details_object() {
    let m: TrafficDetailsResponse = serde_json::from_str("{}").unwrap();
    assert!(m.is_empty());
}
