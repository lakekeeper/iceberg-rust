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

//! Truncation of string and binary values to bounds, as Iceberg Java's
//! `UnicodeUtil` and `BinaryUtil` do it.
//!
//! A lower bound is the prefix of the value; it is less than or equal to the
//! value. An upper bound is the prefix with its last unit incremented; it is
//! greater than or equal to the value. A value that needs no truncation is its
//! own bound. Strings count Unicode scalar values (`char`s), not bytes, and
//! never produce a surrogate code point.

/// The first code point above the surrogate block.
const ABOVE_SURROGATES: u32 = 0xE000;
/// The last code point below the surrogate block.
const BELOW_SURROGATES: u32 = 0xD7FF;

/// Keeps the first `length` code points of `input`.
pub fn truncate_string(input: &str, length: usize) -> &str {
    match input.char_indices().nth(length) {
        None => input,
        Some((end, _)) => &input[..end],
    }
}

/// A string of at most `length` code points that is less than or equal to `input`.
pub fn truncate_string_min(input: &str, length: usize) -> &str {
    truncate_string(input, length)
}

/// A string of at most `length` code points that is greater than or equal to
/// `input`, or `None` if every retained code point is the largest one, so no
/// such string exists.
pub fn truncate_string_max(input: &str, length: usize) -> Option<String> {
    let truncated = truncate_string(input, length);
    if truncated.len() == input.len() {
        return Some(input.to_string());
    }
    // Increment the last retained code point; on overflow carry into the one
    // before it.
    let mut chars: Vec<char> = truncated.chars().collect();
    while let Some(last) = chars.pop() {
        if let Some(next) = increment_code_point(last) {
            chars.push(next);
            return Some(chars.into_iter().collect());
        }
    }
    None
}

/// The next Unicode scalar value after `c`, skipping the surrogate block, or
/// `None` for the largest one.
fn increment_code_point(c: char) -> Option<char> {
    let next = match u32::from(c) {
        BELOW_SURROGATES => ABOVE_SURROGATES,
        code_point => code_point + 1,
    };
    char::from_u32(next)
}

/// Keeps the first `length` bytes of `input`.
pub fn truncate_binary(input: &[u8], length: usize) -> &[u8] {
    if input.len() > length {
        &input[..length]
    } else {
        input
    }
}

/// A byte string of at most `length` bytes that is less than or equal to `input`.
pub fn truncate_binary_min(input: &[u8], length: usize) -> &[u8] {
    truncate_binary(input, length)
}

/// A byte string of at most `length` bytes that is greater than or equal to
/// `input`, or `None` if every retained byte is `0xFF`, so no such byte string
/// exists.
pub fn truncate_binary_max(input: &[u8], length: usize) -> Option<Vec<u8>> {
    if input.len() <= length {
        return Some(input.to_vec());
    }
    // Increment the last retained byte; on overflow carry into the one before it.
    let mut truncated = input[..length].to_vec();
    while let Some(last) = truncated.pop() {
        if let Some(next) = last.checked_add(1) {
            truncated.push(next);
            return Some(truncated);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAX_CODE_POINT: char = '\u{10FFFF}';

    #[test]
    fn truncate_string_counts_code_points() {
        assert_eq!(truncate_string("abcde", 3), "abc");
        assert_eq!(truncate_string("abc", 3), "abc");
        assert_eq!(truncate_string("abc", 5), "abc");
        assert_eq!(truncate_string("a😀b", 2), "a😀");
        assert_eq!(truncate_string("abc", 0), "");
    }

    #[test]
    fn string_min_is_the_prefix() {
        assert_eq!(truncate_string_min("abcde", 3), "abc");
        assert_eq!(truncate_string_min("abc", 3), "abc");
        assert_eq!(truncate_string_min("abc", 5), "abc");
        assert_eq!(truncate_string_min("", 3), "");
    }

    #[test]
    fn string_max_increments_the_last_code_point() {
        assert_eq!(truncate_string_max("abcde", 3).as_deref(), Some("abd"));
        assert_eq!(truncate_string_max("abc", 3).as_deref(), Some("abc"));
        assert_eq!(truncate_string_max("abc", 5).as_deref(), Some("abc"));
        assert_eq!(truncate_string_max("", 3).as_deref(), Some(""));
        assert_eq!(
            truncate_string_max("イロハニホヘト", 4).as_deref(),
            Some("イロハヌ")
        );
    }

    #[test]
    fn string_max_carries_on_overflow() {
        let input = format!("a{MAX_CODE_POINT}c");
        assert_eq!(truncate_string_max(&input, 2).as_deref(), Some("b"));
    }

    #[test]
    fn string_max_skips_the_surrogate_block() {
        let input = "\u{D7FF}extra";
        assert_eq!(truncate_string_max(input, 1).as_deref(), Some("\u{E000}"));
    }

    #[test]
    fn string_max_is_none_when_every_code_point_overflows() {
        let input = format!("{MAX_CODE_POINT}{MAX_CODE_POINT}");
        assert_eq!(truncate_string_max(&input, 1), None);
    }

    #[test]
    fn binary_min_is_the_prefix() {
        assert_eq!(truncate_binary_min(&[1, 2, 3, 4, 5], 3), &[1, 2, 3]);
        assert_eq!(truncate_binary_min(&[1, 2, 3], 3), &[1, 2, 3]);
        assert_eq!(truncate_binary_min(&[1, 2, 3], 5), &[1, 2, 3]);
        assert_eq!(truncate_binary_min(&[1, 2, 3], 0), &[] as &[u8]);
    }

    #[test]
    fn binary_max_increments_the_last_byte() {
        assert_eq!(truncate_binary_max(&[1, 2, 3, 4], 2), Some(vec![1, 3]));
        assert_eq!(truncate_binary_max(&[1, 2, 3], 3), Some(vec![1, 2, 3]));
        assert_eq!(truncate_binary_max(&[1, 2, 3], 5), Some(vec![1, 2, 3]));
    }

    #[test]
    fn binary_max_carries_on_overflow() {
        assert_eq!(truncate_binary_max(&[1, 0xFF, 3], 2), Some(vec![2]));
    }

    #[test]
    fn binary_max_is_none_when_every_byte_overflows() {
        assert_eq!(truncate_binary_max(&[0xFF, 0xFF, 1], 2), None);
    }
}
