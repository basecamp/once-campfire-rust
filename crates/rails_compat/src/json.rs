//! The JSON Rails writes: the json gem's `JSON.generate`, and `ActiveSupport::JSON.encode` on top
//! of it, which messages, cookies, Action Cable, Jbuilder views, webhooks and Active Storage use.
//! Both emit keys in insertion order, which relies on serde_json's `preserve_order` feature
//! (enabled in the workspace manifest).
//!
//! The json gem (2.21.2 in the reference) escapes the characters serde_json does (quote, backslash
//! and control characters, in lowercase hex) and leaves `/` alone. Floats are laid out its own way
//! (see `JsonGem`).
use std::io;

use serde::Serialize;
use serde_json::Value;
use serde_json::ser::Formatter;

/// `::JSON.generate` / `JSON.dump`: plain JSON, non-ASCII left as UTF-8.
pub fn generate<T: Serialize + ?Sized>(value: &T) -> String {
    let mut json = Vec::with_capacity(128);
    value.serialize(&mut serde_json::Serializer::with_formatter(&mut json, JsonGem)).expect("JSON serialization cannot fail");
    String::from_utf8(json).expect("serde_json writes UTF-8")
}

/// `ActiveSupport::JSON.encode` with `escape_html_entities_in_json` (the default): like
/// `JSON.generate`, plus `<`, `>` and `&` escaped as `\u003c`, `\u003e` and `\u0026`. U+2028/U+2029
/// are *not* escaped: `load_defaults` 8.1+ turns `escape_js_separators_in_json` off.
pub fn encode<T: Serialize + ?Sized>(value: &T) -> String {
    escape_html_entities(generate(value))
}

/// Re-escapes already-encoded JSON. `<`, `>` and `&` can only appear inside JSON strings, so
/// replacing them anywhere in the document is safe, and doing it twice is harmless.
pub fn escape_html_entities(json: String) -> String {
    // A broadcast is mostly rendered HTML, with many of the three. They are ASCII, and no byte of a
    // multi-byte UTF-8 character is ASCII, so a byte-wise replacement into one exact allocation
    // gives the same string as a character-wise one.
    let bytes = json.as_bytes();
    let count = bytes.iter().filter(|&&b| matches!(b, b'<' | b'>' | b'&')).count();
    if count == 0 {
        return json;
    }
    let mut escaped = Vec::with_capacity(bytes.len() + count * 5);
    let mut start = 0;
    for (at, &byte) in bytes.iter().enumerate() {
        let replacement: &[u8] = match byte {
            b'<' => b"\\u003c",
            b'>' => b"\\u003e",
            b'&' => b"\\u0026",
            _ => continue,
        };
        escaped.extend_from_slice(&bytes[start..at]);
        escaped.extend_from_slice(replacement);
        start = at + 1;
    }
    escaped.extend_from_slice(&bytes[start..]);
    String::from_utf8(escaped).expect("ASCII replacements keep UTF-8 valid")
}

pub fn parse(bytes: &[u8]) -> Option<Value> {
    serde_json::from_slice(bytes).ok()
}

/// serde_json's compact output with the json gem's floats. serde_json writes a non-finite float as
/// `null`, which is what `ActiveSupport::JSON` does (`Float#as_json`); `JSON.generate` would raise.
struct JsonGem;

impl Formatter for JsonGem {
    fn write_f64<W: ?Sized + io::Write>(&mut self, writer: &mut W, value: f64) -> io::Result<()> {
        writer.write_all(float(value).as_bytes())
    }
}

/// A finite float as the json gem writes it, which is not `Float#to_s`: json 2.21.2's
/// `fpconv_dtoa` (`ext/json/ext/vendor/fpconv.c`, `emit_digits`) writes `1e15` as `1e+15` and
/// `1e-5` as `0.00001`.
///
/// The digits are the shortest that round-trip. fpconv's Grisu2 now and then writes a longer
/// equivalent (`250.70174600000001` for `250.701746`), which parses to the same float.
fn float(f: f64) -> String {
    let sign = if f.is_sign_negative() { "-" } else { "" };
    if f == 0.0 {
        return format!("{sign}0.0");
    }
    // `{:e}` yields the shortest round-trip digits as `d.ddde<exponent>`; fpconv's `K` is the
    // power of ten of the last digit.
    let sci = format!("{:e}", f.abs());
    let (mantissa, exponent) = sci.split_once('e').unwrap();
    let exponent: i32 = exponent.parse().unwrap();
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let k = exponent + 1 - digits.len() as i32;

    let body = if k >= 0 && exponent < 15 {
        format!("{digits}{}.0", "0".repeat(k as usize))
    } else if k < 0 && (k > -7 || exponent.abs() < 10) {
        let point = exponent + 1;
        if point <= 0 {
            format!("0.{}{digits}", "0".repeat(-point as usize))
        } else {
            format!("{}.{}", &digits[..point as usize], &digits[point as usize..])
        }
    } else {
        let fraction = if digits.len() > 1 { format!(".{}", &digits[1..]) } else { String::new() };
        format!("{}{fraction}e{}{}", &digits[..1], if exponent < 0 { '-' } else { '+' }, exponent.abs())
    };
    format!("{sign}{body}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn escapes_html_entities_but_not_separators_or_slashes() {
        assert_eq!(
            encode(&json!({ "key": "<a href=\"/x\">&</a>\u{2028}" })),
            "{\"key\":\"\\u003ca href=\\\"/x\\\"\\u003e\\u0026\\u003c/a\\u003e\u{2028}\"}"
        );
        assert_eq!(generate("<&>"), "\"<&>\"");
        assert_eq!(
            encode("<a href=\"x\">&'\u{2028}é\n\t\u{1}\u{7f}/</a>"),
            "\"\\u003ca href=\\\"x\\\"\\u003e\\u0026'\u{2028}é\\n\\t\\u0001\u{7f}/\\u003c/a\\u003e\""
        );
    }

    #[test]
    fn escapes_control_characters_like_the_json_gem() {
        assert_eq!(encode("\u{1f}\n\t\u{8}\u{c}\u{7f}é"), "\"\\u001f\\n\\t\\b\\f\u{7f}é\"");
    }

    #[test]
    fn floats_match_the_json_gem() {
        // JSON.generate(f) and ActiveSupport::JSON.encode(f) in the reference (json 2.21.2).
        for (f, json) in [
            (320.0, "320.0"),
            (65.84, "65.84"),
            (-2.5, "-2.5"),
            (0.0, "0.0"),
            (-0.0, "-0.0"),
            (0.1, "0.1"),
            (0.0001, "0.0001"),
            (0.00001, "0.00001"),
            (1.25e-5, "0.0000125"),
            (1.5e-7, "0.00000015"),
            (-1.5e-7, "-0.00000015"),
            (1e-7, "0.0000001"),
            (1.2e-9, "0.0000000012"),
            (1.23456789012e-8, "0.0000000123456789012"),
            (1e-10, "1e-10"),
            (5e-324, "5e-324"),
            (1e14, "100000000000000.0"),
            (-1e14, "-100000000000000.0"),
            (123456789012345.6, "123456789012345.6"),
            (1e15, "1e+15"),
            (-1e15, "-1e+15"),
            (1.5e15, "1.5e+15"),
            (1234567890123456.0, "1.234567890123456e+15"),
            (9007199254740992.0, "9.007199254740992e+15"),
            (1e16, "1e+16"),
            (12345678901234567.0, "1.2345678901234568e+16"),
            (1e21, "1e+21"),
            (1e100, "1e+100"),
            (f64::MAX, "1.7976931348623157e+308"),
        ] {
            assert_eq!(generate(&f), json, "{f:e}");
            assert_eq!(encode(&json!([f])), format!("[{json}]"), "{f:e}");
        }
        assert_eq!(encode(&f64::NAN), "null");
        assert_eq!(encode(&f64::INFINITY), "null");
        // ActiveSupport::JSON.encode({ "a" => 1e16, "b" => [1e-5] })
        assert_eq!(encode(&json!({ "a": 1e16, "b": [1e-5] })), r#"{"a":1e+16,"b":[0.00001]}"#);
    }
}
