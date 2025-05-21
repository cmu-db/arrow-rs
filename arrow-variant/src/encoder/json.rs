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

//! Module for converting JSON data to Variant binary format

use serde_json::Value;
use arrow_schema::ArrowError;
use std::io::Write;

use crate::builder::{VariantBuilder, PrimitiveValue, ArrayBuilder, ObjectBuilder};

/// Converts JSON data bytes to Variant binary format
///
/// # Arguments
///
/// * `json_data` - JSON data as bytes
/// * `metadata_writer` - Writer for variant metadata
/// * `value_writer` - Writer for variant value
///
/// # Returns
///
/// * `Ok(())` if successful
/// * `Err` with error details if conversion fails
pub fn json_to_variant<W1: Write, W2: Write>(
    json_data: &[u8],
    metadata_writer: &mut W1,
    value_writer: &mut W2,
) -> Result<(), ArrowError> {
    let json_value: Value = serde_json::from_slice(json_data)
        .map_err(|e| ArrowError::ParseError(format!("Failed to parse JSON: {}", e)))?;
    
    let mut builder = VariantBuilder::new(metadata_writer);
    encode_json_value(&mut builder, value_writer, &json_value)?;
    builder.finish();
    
    Ok(())
}

/// Encode a JSON value to variant binary format
fn encode_json_value<W: Write>(
    builder: &mut VariantBuilder<'_>,
    value_writer: &mut W,
    value: &Value,
) -> Result<(), ArrowError> {
    match value {
        Value::Null => {
            builder.append_null(value_writer);
            Ok(())
        },
        Value::Bool(b) => {
            builder.append_primitive(value_writer, *b);
            Ok(())
        },
        Value::Number(n) => {
            if n.is_i64() {
                builder.append_primitive(value_writer, n.as_i64().unwrap());
            } else {
                builder.append_primitive(value_writer, n.as_f64().unwrap());
            }
            Ok(())
        },
        Value::String(s) => {
            builder.append_primitive(value_writer, s.as_str());
            Ok(())
        },
        Value::Array(arr) => {
            let mut array_builder = builder.new_array(value_writer);
            for elem in arr {
                encode_json_array_element(&mut array_builder, elem)?;
            }
            array_builder.finish();
            Ok(())
        },
        Value::Object(obj) => {
            let mut object_builder = builder.new_object(value_writer);
            for (key, val) in obj {
                encode_json_object_field(&mut object_builder, key, val)?;
            }
            object_builder.finish();
            Ok(())
        }
    }
}

/// Encode a JSON array element
fn encode_json_array_element(
    builder: &mut ArrayBuilder<'_, '_>,
    value: &Value,
) -> Result<(), ArrowError> {
    match value {
        Value::Null => {
            builder.append_null();
            Ok(())
        },
        Value::Bool(b) => {
            builder.append_value(*b);
            Ok(())
        },
        Value::Number(n) => {
            if n.is_i64() {
                builder.append_value(n.as_i64().unwrap());
            } else {
                builder.append_value(n.as_f64().unwrap());
            }
            Ok(())
        },
        Value::String(s) => {
            builder.append_value(s.as_str());
            Ok(())
        },
        Value::Array(arr) => {
            let mut nested_array = builder.new_array();
            for elem in arr {
                encode_json_array_element(&mut nested_array, elem)?;
            }
            nested_array.finish();
            Ok(())
        },
        Value::Object(obj) => {
            let mut nested_object = builder.new_object();
            for (key, val) in obj {
                encode_json_object_field(&mut nested_object, key, val)?;
            }
            nested_object.finish();
            Ok(())
        }
    }
}

/// Encode a JSON object field
fn encode_json_object_field(
    builder: &mut ObjectBuilder<'_, '_>,
    key: &str,
    value: &Value,
) -> Result<(), ArrowError> {
    match value {
        Value::Null => {
            builder.append_null(key);
            Ok(())
        },
        Value::Bool(b) => {
            builder.append_value(key, *b);
            Ok(())
        },
        Value::Number(n) => {
            if n.is_i64() {
                builder.append_value(key, n.as_i64().unwrap());
            } else {
                builder.append_value(key, n.as_f64().unwrap());
            }
            Ok(())
        },
        Value::String(s) => {
            builder.append_value(key, s.as_str());
            Ok(())
        },
        Value::Array(arr) => {
            let mut array = builder.new_array(key);
            for elem in arr {
                encode_json_array_element(&mut array, elem)?;
            }
            array.finish();
            Ok(())
        },
        Value::Object(obj) => {
            let mut object = builder.new_object(key);
            for (nested_key, val) in obj {
                encode_json_object_field(&mut object, nested_key, val)?;
            }
            object.finish();
            Ok(())
        }
    }
}

/// Stream parser for incrementally processing JSON into Variant format
pub struct JsonParser<'a, W1: Write, W2: Write> {
    builder: VariantBuilder<'a>,
    value_writer: &'a mut W2,
    buffer: Vec<u8>,
    state: ParserState,
}

/// State of the JSON parser
enum ParserState {
    Parsing,
    Finished,
}

impl<'a, W1: Write, W2: Write> JsonParser<'a, W1, W2> {
    /// Creates a new JSON parser
    pub fn new(metadata_writer: &'a mut W1, value_writer: &'a mut W2) -> Self {
        Self {
            builder: VariantBuilder::new(metadata_writer),
            value_writer,
            buffer: Vec::new(),
            state: ParserState::Parsing,
        }
    }
    
    /// Process a chunk of JSON data
    pub fn push(&mut self, data: &[u8]) -> Result<(), ArrowError> {
        match self.state {
            ParserState::Finished => {
                Err(ArrowError::InvalidArgumentError(
                    "Cannot push more data after finishing".to_string()
                ))
            }
            ParserState::Parsing => {
                self.buffer.extend_from_slice(data);
                self.try_parse()
            }
        }
    }
    
    /// Try to parse the accumulated JSON data
    fn try_parse(&mut self) -> Result<(), ArrowError> {
        match serde_json::from_slice::<Value>(&self.buffer) {
            Ok(value) => {
                // Successfully parsed a complete JSON value
                encode_json_value(&mut self.builder, self.value_writer, &value)?;
                self.buffer.clear();
                Ok(())
            }
            Err(e) => {
                if e.is_eof() {
                    // This is an expected error for incomplete data
                    Ok(())
                } else {
                    // This is a parsing error
                    Err(ArrowError::ParseError(format!("JSON parse error: {}", e)))
                }
            }
        }
    }
    
    /// Finish parsing and finalize the variant
    pub fn finish(mut self) -> Result<(), ArrowError> {
        self.state = ParserState::Finished;
        
        if !self.buffer.is_empty() {
            match serde_json::from_slice::<Value>(&self.buffer) {
                Ok(value) => {
                    encode_json_value(&mut self.builder, self.value_writer, &value)?;
                }
                Err(e) => {
                    return Err(ArrowError::ParseError(format!("JSON parse error: {}", e)));
                }
            }
        }
        
        self.builder.finish();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Variant;
    
    #[test]
    fn test_json_to_variant_simple() -> Result<(), ArrowError> {
        let json = r#"{"name": "arrow", "number": 42, "is_open_source": true}"#;
        
        let mut metadata_buf = Vec::new();
        let mut value_buf = Vec::new();
        
        json_to_variant(json.as_bytes(), &mut metadata_buf, &mut value_buf)?;
        
        // Check that both buffers have data
        assert!(!metadata_buf.is_empty());
        assert!(!value_buf.is_empty());
        
        // Parse the variant
        let variant = Variant::new(&metadata_buf, &value_buf);
        
        // Check values
        assert_eq!(variant.get("name")?.unwrap().as_string()?, "arrow");
        assert_eq!(variant.get("number")?.unwrap().as_i64()?, 42);
        assert_eq!(variant.get("is_open_source")?.unwrap().as_bool()?, true);
        
        Ok(())
    }
    
    #[test]
    fn test_json_parser_streaming() -> Result<(), ArrowError> {
        let json = r#"{"name": "arrow", "number": 42}"#;
        
        let mut metadata_buf = Vec::new();
        let mut value_buf = Vec::new();
        
        let mut parser = JsonParser::new(&mut metadata_buf, &mut value_buf);
        
        // Parse in two chunks
        let chunk1 = &json.as_bytes()[0..10];
        let chunk2 = &json.as_bytes()[10..];
        
        parser.push(chunk1)?;
        parser.push(chunk2)?;
        parser.finish()?;
        
        // Check that both buffers have data
        assert!(!metadata_buf.is_empty());
        assert!(!value_buf.is_empty());
        
        // Parse the variant
        let variant = Variant::new(&metadata_buf, &value_buf);
        
        // Check values
        assert_eq!(variant.get("name")?.unwrap().as_string()?, "arrow");
        assert_eq!(variant.get("number")?.unwrap().as_i64()?, 42);
        
        Ok(())
    }
    
    #[test]
    fn test_complex_json() -> Result<(), ArrowError> {
        let json = r#"
        {
            "name": "Complex Object",
            "values": [1, 2, 3, 4],
            "nested": {
                "a": true,
                "b": "string value",
                "c": null
            },
            "mixed_array": [1, "two", 3.0, null, {"key": "value"}]
        }
        "#;
        
        let mut metadata_buf = Vec::new();
        let mut value_buf = Vec::new();
        
        json_to_variant(json.as_bytes(), &mut metadata_buf, &mut value_buf)?;
        
        // Parse the variant
        let variant = Variant::new(&metadata_buf, &value_buf);
        
        // Check basic values
        assert_eq!(variant.get("name")?.unwrap().as_string()?, "Complex Object");
        
        // Check array
        let values = variant.get("values")?.unwrap();
        assert!(values.is_array()?);
        assert_eq!(values.get_index(0)?.unwrap().as_i64()?, 1);
        assert_eq!(values.get_index(3)?.unwrap().as_i64()?, 4);
        
        // Check nested object
        let nested = variant.get("nested")?.unwrap();
        assert!(nested.is_object()?);
        assert_eq!(nested.get("a")?.unwrap().as_bool()?, true);
        assert_eq!(nested.get("b")?.unwrap().as_string()?, "string value");
        assert!(nested.get("c")?.unwrap().is_null()?);
        
        // Check mixed array
        let mixed = variant.get("mixed_array")?.unwrap();
        assert!(mixed.is_array()?);
        assert_eq!(mixed.get_index(0)?.unwrap().as_i64()?, 1);
        assert_eq!(mixed.get_index(1)?.unwrap().as_string()?, "two");
        assert_eq!(mixed.get_index(2)?.unwrap().as_f64()?, 3.0);
        assert!(mixed.get_index(3)?.unwrap().is_null()?);
        
        // Check nested object in array
        let nested_in_array = mixed.get_index(4)?.unwrap();
        assert!(nested_in_array.is_object()?);
        assert_eq!(nested_in_array.get("key")?.unwrap().as_string()?, "value");
        
        Ok(())
    }
} 