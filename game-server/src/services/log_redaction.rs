//! Shared redaction helpers for bot logs.

use std::sync::OnceLock;

use regex::{Captures, Regex};

pub(crate) fn redact_log_line(line: &str) -> String {
    let redacted_key_values = key_value_regex()
        .replace_all(line, |caps: &Captures<'_>| {
            let prefix = caps
                .name("prefix")
                .map(|value| value.as_str())
                .unwrap_or_default();
            let key = caps
                .name("key")
                .map(|value| value.as_str())
                .unwrap_or_default();
            let value = caps
                .name("value")
                .map(|value| value.as_str())
                .unwrap_or_default();
            if key.eq_ignore_ascii_case("authorization") && value.eq_ignore_ascii_case("bearer") {
                return format!("{prefix}{value}");
            }
            format!("{prefix}{}", mask_secret_value(value))
        })
        .into_owned();

    let redacted_bearer = bearer_regex()
        .replace_all(&redacted_key_values, |caps: &Captures<'_>| {
            let prefix = caps
                .name("prefix")
                .map(|value| value.as_str())
                .unwrap_or_default();
            let value = caps
                .name("value")
                .map(|value| value.as_str())
                .unwrap_or_default();
            format!("{prefix}{}", mask_secret_value(value))
        })
        .into_owned();

    jwt_regex()
        .replace_all(&redacted_bearer, |caps: &Captures<'_>| {
            let value = caps
                .name("value")
                .map(|value| value.as_str())
                .unwrap_or_default();
            mask_secret_value(value)
        })
        .into_owned()
}

fn key_value_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(
            r"(?ix)
            (?P<prefix>
              (?P<key>
                \b
                [A-Z0-9_.-]*
                (?:TOKEN|SECRET|PASSWORD|API_KEY|AUTH|COOKIE|KEY)
                [A-Z0-9_.-]*
                \b
              )
              \s*(?:=|:)\s*
            )
            (?P<value>[^\s,;)\]}]+)
            ",
        )
        .expect("invalid log redaction key-value regex")
    })
}

fn bearer_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(r"(?i)(?P<prefix>\bBearer\s+)(?P<value>[^\s,;)\]}]+)")
            .expect("invalid log redaction bearer regex")
    })
}

fn jwt_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(r"(?P<value>\b[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{5,}\b)")
            .expect("invalid log redaction jwt regex")
    })
}

fn mask_secret_value(raw_value: &str) -> String {
    if raw_value.is_empty() || raw_value.contains("***") {
        return raw_value.to_string();
    }

    let (prefix, core, suffix) = split_wrapping_quotes(raw_value);
    if core.is_empty() || core.contains("***") {
        return raw_value.to_string();
    }

    let chars: Vec<char> = core.chars().collect();
    let len = chars.len();
    let masked_core = match len {
        0 => String::new(),
        1 | 2 => "***".to_string(),
        3..=7 => format!("{}***{}", chars[0], chars[len - 1]),
        _ => format!(
            "{}{}***{}{}",
            chars[0],
            chars[1],
            chars[len - 2],
            chars[len - 1]
        ),
    };

    format!("{prefix}{masked_core}{suffix}")
}

fn split_wrapping_quotes(value: &str) -> (&str, &str, &str) {
    if value.len() < 2 {
        return ("", value, "");
    }
    let first = value.as_bytes()[0];
    let last = value.as_bytes()[value.len() - 1];
    if (first == b'"' && last == b'"') || (first == b'\'' && last == b'\'') {
        (&value[..1], &value[1..value.len() - 1], &value[value.len() - 1..])
    } else {
        ("", value, "")
    }
}
