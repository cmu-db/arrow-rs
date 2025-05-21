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

//! Example that demonstrates converting JSON data to Variant format

use arrow_schema::ArrowError;
use arrow_variant::encoder::json_to_variant;
use arrow_variant::Variant;

fn main() -> Result<(), ArrowError> {
    // Example JSON data
    let json_data = r#"{
        "id": 12345,
        "name": "Example Product",
        "price": 49.99,
        "available": true,
        "tags": ["electronics", "gadgets", "new"],
        "dimensions": {
            "width": 10.5,
            "height": 5.0,
            "depth": 2.0
        },
        "reviews": [
            {
                "user": "user1",
                "rating": 5,
                "comment": "Great product!"
            },
            {
                "user": "user2",
                "rating": 4,
                "comment": "Good value for money"
            }
        ]
    }"#;

    println!("Converting JSON to Variant format...");

    // Create buffers for metadata and value
    let mut metadata_buffer = Vec::new();
    let mut value_buffer = Vec::new();

    // Convert JSON to Variant
    json_to_variant(json_data.as_bytes(), &mut metadata_buffer, &mut value_buffer)?;

    println!("Conversion successful!");
    println!("Metadata size: {} bytes", metadata_buffer.len());
    println!("Value size: {} bytes", value_buffer.len());

    // Parse the variant and access values
    let variant = Variant::new(&metadata_buffer, &value_buffer);

    // Access simple fields
    println!("\nAccessing fields:");
    println!("ID: {}", variant.get("id")?.unwrap().as_i64()?);
    println!("Name: {}", variant.get("name")?.unwrap().as_string()?);
    println!("Price: {}", variant.get("price")?.unwrap().as_f64()?);
    println!("Available: {}", variant.get("available")?.unwrap().as_bool()?);

    // Access array
    let tags = variant.get("tags")?.unwrap();
    println!("\nTags:");
    for i in 0..3 {
        println!("  - {}", tags.get_index(i)?.unwrap().as_string()?);
    }

    // Access nested object
    let dimensions = variant.get("dimensions")?.unwrap();
    println!("\nDimensions:");
    println!("  Width: {}", dimensions.get("width")?.unwrap().as_f64()?);
    println!("  Height: {}", dimensions.get("height")?.unwrap().as_f64()?);
    println!("  Depth: {}", dimensions.get("depth")?.unwrap().as_f64()?);

    // Access array of objects
    let reviews = variant.get("reviews")?.unwrap();
    println!("\nReviews:");
    for i in 0..2 {
        let review = reviews.get_index(i)?.unwrap();
        println!("  Review #{}:", i + 1);
        println!("    User: {}", review.get("user")?.unwrap().as_string()?);
        println!("    Rating: {}", review.get("rating")?.unwrap().as_i64()?);
        println!("    Comment: {}", review.get("comment")?.unwrap().as_string()?);
    }

    Ok(())
} 