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
pub fn find_timestamp(value: &Value) -> Option<u64> {
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

pub fn find_nested_objects(value: &Value) -> Vec<Value> {
    let mut objects = Vec::new();

    match value {
        Value::Object(map) => {
            // If the current value is an object, add it to the list
            objects.push(value.clone());
            // Then iterate over its values to find nested objects
            for val in map.values() {
                objects.extend(find_nested_objects(val));
            }
        }
        Value::Array(arr) => {
            // If the current value is an array, look for objects in each element
            for item in arr {
                objects.extend(find_nested_objects(item));
            }
        }
        // Other JSON types do not contain nested objects
        _ => {}
    }

    objects
}

// Parses a JSON file and creates segments based on the extracted timestamps
pub fn parse_json_file_to_segments(path: PathBuf) -> Vec<Segment> {
    let mut segments: Vec<Segment> = Vec::new();
    let file = File::open(path).expect("File not found");
    let reader = BufReader::new(file);

    let mut json_data_accumulator = String::new();
    let mut brace_count = 0;
    let mut bracket_count = 0;
    let mut in_string = false;

    for line in reader.lines() {
        let line = line.expect("Error reading line");

        // Update brace and bracket count, considering strings
        for c in line.chars() {
            if c == '"' && !in_string {
                in_string = true;
            } else if c == '"' && in_string {
                in_string = false;
            }

            if !in_string {
                match c {
                    '{' => brace_count += 1,
                    '}' => brace_count -= 1,
                    '[' => bracket_count += 1,
                    ']' => bracket_count -= 1,
                    _ => {}
                }
            }
        }

        json_data_accumulator.push_str(&line);

        // Check for a complete JSON object or array
        if brace_count == 0 && bracket_count == 0 && !json_data_accumulator.trim().is_empty() {
            // Process the JSON data
            if let Ok(Some(timestamp)) = extract_timestamp(&json_data_accumulator) {
                segments.push(Segment::Seg(
                    timestamp as usize,
                    timestamp as usize,
                    vec![json_data_accumulator.clone()],
                ));
            }
            json_data_accumulator.clear(); // Reset for the next JSON object or array
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

    #[test]
    fn test_parse_json_file_to_segments_multiple_objects() {
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(
            temp_file,
            r#"{{"timestamp": 1707854392, "event": "testEvent1"}}
{{"timestamp": 1707854393, "event": "testEvent2"}}"#
        )
        .unwrap();

        let path = temp_file.into_temp_path();
        let segments = parse_json_file_to_segments(path.to_path_buf());

        assert_eq!(segments.len(), 2);
        if let Segment::Seg(ts, _, data) = &segments[0] {
            assert_eq!(*ts, 1707854392 as usize);
            assert!(
                data.contains(&r#"{"timestamp": 1707854392, "event": "testEvent1"}"#.to_string())
            );
        } else {
            panic!("Expected Segment::Seg, found {:?}", segments[0]);
        }
        if let Segment::Seg(ts, _, data) = &segments[1] {
            assert_eq!(*ts, 1707854393 as usize);
            assert!(
                data.contains(&r#"{"timestamp": 1707854393, "event": "testEvent2"}"#.to_string())
            );
        } else {
            panic!("Expected Segment::Seg, found {:?}", segments[1]);
        }
    }

    #[test]
    fn test_parse_json_file_to_segments_invalid_json() {
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(
            temp_file,
            r#"{{"timestamp": 1707854392, "event" "testEvent1"}}"#
        )
        .unwrap();

        let path = temp_file.into_temp_path();
        let segments = parse_json_file_to_segments(path.to_path_buf());

        assert_eq!(segments.len(), 0);
    }

    #[test]
    fn test_parse_json_file_to_segments_missing_timestamp() {
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, r#"{{"event": "testEvent1"}}"#).unwrap();

        let path = temp_file.into_temp_path();
        let segments = parse_json_file_to_segments(path.to_path_buf());

        assert_eq!(segments.len(), 0);
    }

    #[test]
    fn test_json_with_string_containing_braces() {
        let json_data = r#"{"timestamp": 1627848123, "message": "Example {with braces}"}"#;
        assert!(matches!(
            extract_timestamp(json_data).unwrap(),
            Some(1627848123)
        ));
    }

    #[test]
    fn test_json_arrays() {
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file
            .write_all(b"[{\"timestamp\": 1627848123}, {\"timestamp\": 1627848184}]\n")
            .unwrap();

        let path = temp_file.into_temp_path();
        let segments = parse_json_file_to_segments(path.to_path_buf());

        assert_eq!(segments.len(), 1);
    }

    #[test]
    fn test_complex_json_structure() {
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(b"{\"events\": [{\"timestamp\": 1627848123, \"user\": \"user1\"}, {\"event\": {\"timestamp\": 1627848184, \"user\": \"user2\"}}]}\n").unwrap();

        let path = temp_file.into_temp_path();
        let segments = parse_json_file_to_segments(path.to_path_buf());

        assert!(segments.len() > 0);
    }

    #[test]
    fn test_incomplete_json() {
        let json_data = r#"{"timestamp": 1627848123"#;
        assert!(extract_timestamp(json_data).is_err());
    }
}
