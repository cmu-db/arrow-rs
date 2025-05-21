// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Comprehensive tests for JSON to Variant conversion

#[cfg(test)]
mod tests {
    use crate::encoder::json::{json_to_variant, JsonParser};
    use crate::Variant;
    use arrow_schema::ArrowError;
    use std::collections::HashMap;
    use std::fmt::Write as FmtWrite;
    use std::io::Write;

    #[test]
    fn test_empty_objects_arrays() -> Result<(), ArrowError> {
        // Empty object
        let json = "{}";
        let mut metadata_buf = Vec::new();
        let mut value_buf = Vec::new();
        
        json_to_variant(json.as_bytes(), &mut metadata_buf, &mut value_buf)?;
        
        let variant = Variant::new(&metadata_buf, &value_buf);
        assert!(variant.is_object()?);
        
        // Empty array
        let json = "[]";
        let mut metadata_buf = Vec::new();
        let mut value_buf = Vec::new();
        
        json_to_variant(json.as_bytes(), &mut metadata_buf, &mut value_buf)?;
        
        let variant = Variant::new(&metadata_buf, &value_buf);
        assert!(variant.is_array()?);
        
        Ok(())
    }

    #[test]
    fn test_primitive_types() -> Result<(), ArrowError> {
        let json = r#"{
            "null_value": null,
            "bool_true": true,
            "bool_false": false,
            "int_small": 42,
            "int_medium": 32768,
            "int_large": 2147483648,
            "float": 3.14159,
            "string": "hello world"
        }"#;
        
        let mut metadata_buf = Vec::new();
        let mut value_buf = Vec::new();
        
        json_to_variant(json.as_bytes(), &mut metadata_buf, &mut value_buf)?;
        
        let variant = Variant::new(&metadata_buf, &value_buf);
        
        // Check null
        assert!(variant.get("null_value")?.unwrap().is_null()?);
        
        // Check booleans
        assert_eq!(variant.get("bool_true")?.unwrap().as_bool()?, true);
        assert_eq!(variant.get("bool_false")?.unwrap().as_bool()?, false);
        
        // Check integers of different sizes
        assert_eq!(variant.get("int_small")?.unwrap().as_i64()?, 42);
        assert_eq!(variant.get("int_medium")?.unwrap().as_i64()?, 32768);
        assert_eq!(variant.get("int_large")?.unwrap().as_i64()?, 2147483648);
        
        // Check float
        let float_val = variant.get("float")?.unwrap().as_f64()?;
        assert!((float_val - 3.14159).abs() < f64::EPSILON);
        
        // Check string
        assert_eq!(variant.get("string")?.unwrap().as_string()?, "hello world");
        
        Ok(())
    }

    #[test]
    fn test_deeply_nested_objects() -> Result<(), ArrowError> {
        let json = r#"{
            "level1": {
                "level2": {
                    "level3": {
                        "level4": {
                            "level5": {
                                "value": "deeply nested"
                            }
                        }
                    }
                }
            }
        }"#;
        
        let mut metadata_buf = Vec::new();
        let mut value_buf = Vec::new();
        
        json_to_variant(json.as_bytes(), &mut metadata_buf, &mut value_buf)?;
        
        let variant = Variant::new(&metadata_buf, &value_buf);
        
        // Navigate through the nested structure
        let level1 = variant.get("level1")?.unwrap();
        let level2 = level1.get("level2")?.unwrap();
        let level3 = level2.get("level3")?.unwrap();
        let level4 = level3.get("level4")?.unwrap();
        let level5 = level4.get("level5")?.unwrap();
        
        assert_eq!(level5.get("value")?.unwrap().as_string()?, "deeply nested");
        
        Ok(())
    }

    #[test]
    fn test_complex_arrays() -> Result<(), ArrowError> {
        let json = r#"{
            "empty_array": [],
            "int_array": [1, 2, 3, 4, 5],
            "mixed_array": [null, true, 42, "string", 3.14, [1, 2], {"key": "value"}],
            "array_of_arrays": [[1, 2], [3, 4], [5, 6]],
            "array_of_objects": [{"a": 1}, {"b": 2}, {"c": 3}]
        }"#;
        
        let mut metadata_buf = Vec::new();
        let mut value_buf = Vec::new();
        
        json_to_variant(json.as_bytes(), &mut metadata_buf, &mut value_buf)?;
        
        let variant = Variant::new(&metadata_buf, &value_buf);
        
        // Check empty array
        let empty_array = variant.get("empty_array")?.unwrap();
        assert!(empty_array.is_array()?);
        
        // Check integer array
        let int_array = variant.get("int_array")?.unwrap();
        assert!(int_array.is_array()?);
        for i in 0..5 {
            assert_eq!(int_array.get_index(i)?.unwrap().as_i64()?, (i + 1) as i64);
        }
        
        // Check mixed array
        let mixed_array = variant.get("mixed_array")?.unwrap();
        assert!(mixed_array.is_array()?);
        
        // null
        assert!(mixed_array.get_index(0)?.unwrap().is_null()?);
        // boolean
        assert_eq!(mixed_array.get_index(1)?.unwrap().as_bool()?, true);
        // integer
        assert_eq!(mixed_array.get_index(2)?.unwrap().as_i64()?, 42);
        // string
        assert_eq!(mixed_array.get_index(3)?.unwrap().as_string()?, "string");
        // float
        let float_val = mixed_array.get_index(4)?.unwrap().as_f64()?;
        assert!((float_val - 3.14).abs() < f64::EPSILON);
        // nested array
        let nested_array = mixed_array.get_index(5)?.unwrap();
        assert!(nested_array.is_array()?);
        assert_eq!(nested_array.get_index(0)?.unwrap().as_i64()?, 1);
        assert_eq!(nested_array.get_index(1)?.unwrap().as_i64()?, 2);
        // nested object
        let nested_obj = mixed_array.get_index(6)?.unwrap();
        assert!(nested_obj.is_object()?);
        assert_eq!(nested_obj.get("key")?.unwrap().as_string()?, "value");
        
        // Check array of arrays
        let array_of_arrays = variant.get("array_of_arrays")?.unwrap();
        assert!(array_of_arrays.is_array()?);
        for i in 0..3 {
            let inner_array = array_of_arrays.get_index(i)?.unwrap();
            assert!(inner_array.is_array()?);
            assert_eq!(inner_array.get_index(0)?.unwrap().as_i64()?, (i * 2 + 1) as i64);
            assert_eq!(inner_array.get_index(1)?.unwrap().as_i64()?, (i * 2 + 2) as i64);
        }
        
        // Check array of objects
        let array_of_objects = variant.get("array_of_objects")?.unwrap();
        assert!(array_of_objects.is_array()?);
        
        let obj0 = array_of_objects.get_index(0)?.unwrap();
        assert!(obj0.is_object()?);
        assert_eq!(obj0.get("a")?.unwrap().as_i64()?, 1);
        
        let obj1 = array_of_objects.get_index(1)?.unwrap();
        assert!(obj1.is_object()?);
        assert_eq!(obj1.get("b")?.unwrap().as_i64()?, 2);
        
        let obj2 = array_of_objects.get_index(2)?.unwrap();
        assert!(obj2.is_object()?);
        assert_eq!(obj2.get("c")?.unwrap().as_i64()?, 3);
        
        Ok(())
    }

    #[test]
    fn test_streaming_parser_chunks() -> Result<(), ArrowError> {
        let json = r#"{
            "id": 12345,
            "name": "Test Object",
            "tags": ["tag1", "tag2", "tag3"],
            "metadata": {
                "created_at": "2023-05-15",
                "updated_at": "2023-05-16"
            }
        }"#;
        
        // Break the JSON into multiple chunks to simulate streaming
        let chunks = vec![
            &json[0..20],
            &json[20..50],
            &json[50..100],
            &json[100..150],
            &json[150..],
        ];
        
        let mut metadata_buf = Vec::new();
        let mut value_buf = Vec::new();
        
        let mut parser = JsonParser::new(&mut metadata_buf, &mut value_buf);
        
        // Parse each chunk
        for chunk in chunks {
            parser.push(chunk.as_bytes())?;
        }
        
        // Finalize the parsing
        parser.finish()?;
        
        // Verify the result
        let variant = Variant::new(&metadata_buf, &value_buf);
        
        assert_eq!(variant.get("id")?.unwrap().as_i64()?, 12345);
        assert_eq!(variant.get("name")?.unwrap().as_string()?, "Test Object");
        
        let tags = variant.get("tags")?.unwrap();
        assert!(tags.is_array()?);
        assert_eq!(tags.get_index(0)?.unwrap().as_string()?, "tag1");
        assert_eq!(tags.get_index(1)?.unwrap().as_string()?, "tag2");
        assert_eq!(tags.get_index(2)?.unwrap().as_string()?, "tag3");
        
        let metadata = variant.get("metadata")?.unwrap();
        assert!(metadata.is_object()?);
        assert_eq!(metadata.get("created_at")?.unwrap().as_string()?, "2023-05-15");
        assert_eq!(metadata.get("updated_at")?.unwrap().as_string()?, "2023-05-16");
        
        Ok(())
    }

    #[test]
    fn test_large_json() -> Result<(), ArrowError> {
        // Generate a large JSON object with many fields
        let mut json = String::new();
        json.push_str("{\n");
        
        for i in 0..1000 {
            write!(&mut json, "  \"field{}\": {},\n", i, i).unwrap();
        }
        
        // Remove the trailing comma and close the object
        json.truncate(json.len() - 2);
        json.push_str("\n}");
        
        let mut metadata_buf = Vec::new();
        let mut value_buf = Vec::new();
        
        json_to_variant(json.as_bytes(), &mut metadata_buf, &mut value_buf)?;
        
        let variant = Variant::new(&metadata_buf, &value_buf);
        
        // Check some random fields to verify it worked
        assert_eq!(variant.get("field0")?.unwrap().as_i64()?, 0);
        assert_eq!(variant.get("field100")?.unwrap().as_i64()?, 100);
        assert_eq!(variant.get("field500")?.unwrap().as_i64()?, 500);
        assert_eq!(variant.get("field999")?.unwrap().as_i64()?, 999);
        
        Ok(())
    }

    #[test]
    fn test_json_field_reuse() -> Result<(), ArrowError> {
        // Create multiple similar JSON objects with the same field structure
        let json1 = r#"{"id": 1, "name": "Alice", "active": true}"#;
        let json2 = r#"{"id": 2, "name": "Bob", "active": false}"#;
        let json3 = r#"{"id": 3, "name": "Charlie", "active": true}"#;
        
        // We'll use the same metadata buffer for all conversions
        let mut metadata_buf = Vec::new();
        
        // Process each JSON separately but with shared metadata
        let mut value_buf1 = Vec::new();
        let mut value_buf2 = Vec::new();
        let mut value_buf3 = Vec::new();
        
        // Create a parser for the first object
        let mut parser1 = JsonParser::new(&mut metadata_buf, &mut value_buf1);
        parser1.push(json1.as_bytes())?;
        parser1.finish()?;
        
        // Create a parser for the second object
        let mut parser2 = JsonParser::new(&mut metadata_buf, &mut value_buf2);
        parser2.push(json2.as_bytes())?;
        parser2.finish()?;
        
        // Create a parser for the third object
        let mut parser3 = JsonParser::new(&mut metadata_buf, &mut value_buf3);
        parser3.push(json3.as_bytes())?;
        parser3.finish()?;
        
        // Check that all variants can be properly read
        let variant1 = Variant::new(&metadata_buf, &value_buf1);
        assert_eq!(variant1.get("id")?.unwrap().as_i64()?, 1);
        assert_eq!(variant1.get("name")?.unwrap().as_string()?, "Alice");
        assert_eq!(variant1.get("active")?.unwrap().as_bool()?, true);
        
        let variant2 = Variant::new(&metadata_buf, &value_buf2);
        assert_eq!(variant2.get("id")?.unwrap().as_i64()?, 2);
        assert_eq!(variant2.get("name")?.unwrap().as_string()?, "Bob");
        assert_eq!(variant2.get("active")?.unwrap().as_bool()?, false);
        
        let variant3 = Variant::new(&metadata_buf, &value_buf3);
        assert_eq!(variant3.get("id")?.unwrap().as_i64()?, 3);
        assert_eq!(variant3.get("name")?.unwrap().as_string()?, "Charlie");
        assert_eq!(variant3.get("active")?.unwrap().as_bool()?, true);
        
        Ok(())
    }

    #[test]
    fn test_error_handling() {
        // Invalid JSON
        let invalid_json = r#"{"unclosed_object": "value"#;
        let mut metadata_buf = Vec::new();
        let mut value_buf = Vec::new();
        
        let result = json_to_variant(invalid_json.as_bytes(), &mut metadata_buf, &mut value_buf);
        assert!(result.is_err());
        
        // Streaming with invalid JSON
        let mut metadata_buf = Vec::new();
        let mut value_buf = Vec::new();
        let mut parser = JsonParser::new(&mut metadata_buf, &mut value_buf);
        
        let chunk1 = r#"{"valid": "json", "but": "incomplete#;
        let result = parser.push(chunk1.as_bytes());
        assert!(result.is_ok()); // This should be ok because it's incomplete
        
        let chunk2 = r#" and invalid"}"#; // This makes it invalid
        let result = parser.push(chunk2.as_bytes());
        assert!(result.is_err());
    }

    #[test]
    fn test_special_numeric_values() -> Result<(), ArrowError> {
        let json = r#"{
            "integer_min": -2147483648,
            "integer_max": 2147483647,
            "long_min": -9223372036854775808,
            "long_max": 9223372036854775807,
            "float_small": 1.0e-10,
            "float_large": 1.0e+10
        }"#;
        
        let mut metadata_buf = Vec::new();
        let mut value_buf = Vec::new();
        
        json_to_variant(json.as_bytes(), &mut metadata_buf, &mut value_buf)?;
        
        let variant = Variant::new(&metadata_buf, &value_buf);
        
        // Check min/max integers
        assert_eq!(variant.get("integer_min")?.unwrap().as_i64()?, -2147483648);
        assert_eq!(variant.get("integer_max")?.unwrap().as_i64()?, 2147483647);
        
        // Check min/max longs
        assert_eq!(variant.get("long_min")?.unwrap().as_i64()?, i64::MIN);
        assert_eq!(variant.get("long_max")?.unwrap().as_i64()?, i64::MAX);
        
        // Check small/large floats
        let small_float = variant.get("float_small")?.unwrap().as_f64()?;
        assert!((small_float - 1.0e-10).abs() < 1.0e-15);
        
        let large_float = variant.get("float_large")?.unwrap().as_f64()?;
        assert!((large_float - 1.0e+10).abs() < 1.0e-5);
        
        Ok(())
    }
} 