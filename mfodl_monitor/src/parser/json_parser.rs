use serde_json::{Result, Value};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

#[derive(Debug)]
pub enum Segment {
    Epoch(i64),
    Seg(usize, usize, Vec<String>),
}

// Function to extract a timestamp from a generic JSON object
fn extract_timestamp(json_data: &str) -> Result<Option<u64>> {
    let value: Value = serde_json::from_str(json_data)?;
    Ok(find_timestamp(&value))
}

// Helper function to recursively search for a timestamp field in the JSON
fn find_timestamp(value: &Value) -> Option<u64> {
    match value {
        Value::Object(map) => {
            for (k, v) in map {
                if k == "timestamp" {
                    if let Some(timestamp) = v.as_u64() {
                        return Some(timestamp);
                    }
                } else {
                    if let Some(found) = find_timestamp(v) {
                        return Some(found);
                    }
                }
            }
            None
        }
        Value::Array(arr) => {
            for item in arr {
                if let Some(found) = find_timestamp(item) {
                    return Some(found);
                }
            }
            None
        }
        _ => None,
    }
}

// Parses a JSON file and creates segments based on the extracted timestamps
pub fn parse_json_file_to_segments(path: PathBuf) -> Vec<Segment> {
    let mut segments: Vec<Segment> = Vec::new();
    let file = File::open(path).expect("File not found");
    let reader = BufReader::new(file);

    for line in reader.lines() {
        println!("line: {:?}", line);
        let json_data = line.expect("Error reading line");
        println!("json_data: {}", json_data);
        if let Ok(Some(timestamp)) = extract_timestamp(&json_data) {
            segments.push(Segment::Seg(timestamp as usize, 0, vec![json_data]));
        } else {
            // Error handling or logging can be improved here
            println!(
                "Error parsing JSON line or timestamp not found: {}",
                json_data
            );
        }
    }

    segments
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_extract_timestamp() {
        let json_data =
            r#"{"event": {"timestamp": 1707854392, "details": {"user": "ExampleUser"}}}"#;
        let timestamp = extract_timestamp(json_data).unwrap();

        assert_eq!(timestamp, Some(1707854392));
    }

    #[test]
    fn test_nested_extract_timestamp() {
        let json_data = r#"{"level1": {"level2": {"level3": {"timestamp": 1707854392}}}}"#;
        let timestamp = extract_timestamp(json_data).unwrap();
        assert_eq!(timestamp, Some(1707854392));
    }

    #[test]
    fn test_array_extract_timestamp() {
        let json_data = r#"{"events": [{"name": "event1"}, {"name": "event2", "timestamp": 1707854392}, {"name": "event3"}]}"#;
        let timestamp = extract_timestamp(json_data).unwrap();
        assert_eq!(timestamp, Some(1707854392));
    }

    #[test]
    fn test_missing_timestamp() {
        let json_data =
            r#"{"event": {"name": "eventWithoutTimestamp", "details": {"user": "ExampleUser"}}}"#;
        let timestamp = extract_timestamp(json_data).unwrap();
        assert_eq!(timestamp, None);
    }

    #[test]
    fn test_mixed_objects_and_arrays() {
        let json_data = r#"{"level1": [{"level2": {"timestamp": 1707854390}}, {"level2": {"level3": [{"timestamp": 1707854392}]}}]}"#;
        let timestamp = extract_timestamp(json_data).unwrap();
        assert_eq!(timestamp, Some(1707854390)); // Testing if it finds the first timestamp
    }

    #[test]
    fn test_timestamp_in_root_object() {
        let json_data = r#"{"timestamp": 1707854392, "event": {"name": "eventInRoot", "details": {"user": "ExampleUser"}}}"#;
        let timestamp = extract_timestamp(json_data).unwrap();
        assert_eq!(timestamp, Some(1707854392));
    }

    #[test]
    fn test_parse_json_file_to_segments_single_object() {
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(
            temp_file,
            r#"{{"timestamp": 1707854392, "event": "testEvent"}}"#
        )
        .unwrap();

        let path = temp_file.into_temp_path();
        let segments = parse_json_file_to_segments(path.to_path_buf());

        assert_eq!(segments.len(), 1);
        if let Segment::Seg(ts, _, data) = &segments[0] {
            assert_eq!(*ts, 1707854392 as usize);
            assert!(
                data.contains(&r#"{"timestamp": 1707854392, "event": "testEvent"}"#.to_string())
            );
        } else {
            panic!("Expected Segment::Seg, found {:?}", segments[0]);
        }
    }
}
