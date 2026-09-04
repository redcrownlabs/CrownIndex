//! Parses torrent names into conservative media lookup candidates.

use regex::Regex;
use std::sync::OnceLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ParsedKind {
    Movie,
    Series,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParsedMedia {
    pub(crate) kind: ParsedKind,
    pub(crate) title: String,
    pub(crate) year: Option<i32>,
}

pub(crate) fn parse_torrent_name(name: &str, hint: Option<&str>) -> Option<ParsedMedia> {
    let mut text = strip_extension(name);
    text = strip_leading_release_group(text);
    let kind = kind_from_name(text, hint);
    let year = release_year(text);
    let title = match kind {
        ParsedKind::Series => series_title(text, year),
        ParsedKind::Movie => movie_title(text, year),
    }?;
    Some(ParsedMedia { kind, title, year })
}

pub(crate) fn normalized_title(value: &str) -> String {
    value
        .chars()
        .filter_map(|character| {
            if character.is_ascii_alphanumeric() {
                Some(character.to_ascii_lowercase())
            } else if character.is_alphanumeric() {
                Some(character)
            } else {
                None
            }
        })
        .collect()
}

fn strip_extension(name: &str) -> &str {
    name.rsplit_once('.').map_or(name, |(stem, extension)| {
        if matches!(
            extension.to_ascii_lowercase().as_str(),
            "mkv" | "mp4" | "avi" | "mov" | "wmv" | "m4v"
        ) {
            stem
        } else {
            name
        }
    })
}

fn strip_leading_release_group(name: &str) -> &str {
    let trimmed = name.trim();
    if !trimmed.starts_with('[') {
        return trimmed;
    }
    let Some(end) = trimmed.find(']') else {
        return trimmed;
    };
    let group = &trimmed[1..end];
    if group.chars().all(|character| {
        character.is_ascii_alphanumeric() || matches!(character, ' ' | '-' | '_' | '.')
    }) {
        trimmed[end + 1..].trim()
    } else {
        trimmed
    }
}

fn kind_from_name(name: &str, hint: Option<&str>) -> ParsedKind {
    match hint {
        Some("tv_show") => ParsedKind::Series,
        Some("movie") => ParsedKind::Movie,
        _ if series_marker().is_match(name) => ParsedKind::Series,
        _ => ParsedKind::Movie,
    }
}

fn movie_title(name: &str, year: Option<i32>) -> Option<String> {
    let cut = year
        .and_then(|value| name.find(&value.to_string()))
        .or_else(|| first_noise_token(name))
        .unwrap_or(name.len());
    normalize_display_title(&name[..cut])
}

fn series_title(name: &str, year: Option<i32>) -> Option<String> {
    let cut = [
        series_marker().find(name).map(|match_| match_.start()),
        year.and_then(|value| name.find(&value.to_string())),
    ]
    .into_iter()
    .flatten()
    .min()
    .unwrap_or(name.len());
    normalize_display_title(&name[..cut])
}

fn normalize_display_title(value: &str) -> Option<String> {
    let title = value
        .replace(['.', '_'], " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    let title = title.trim_matches(|character: char| {
        matches!(character, '-' | ':' | '[' | ']' | '(' | ')' | ' ')
    });
    (normalized_title(title).chars().count() >= 3).then(|| title.to_owned())
}

fn release_year(value: &str) -> Option<i32> {
    year_regex()
        .find_iter(value)
        .filter_map(|match_| match_.as_str().parse::<i32>().ok())
        .find(|year| (1900..=2100).contains(year))
}

fn first_noise_token(value: &str) -> Option<usize> {
    noise_token().find(value).map(|match_| match_.start())
}

fn year_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| Regex::new(r"\b(?:19|20)\d{2}\b").expect("valid year regex"))
}

fn series_marker() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(r"(?i)\bS\d{1,2}E\d{1,3}\b|\bSeason\s+\d{1,2}\b|\s-\s\d{1,3}\b")
            .expect("valid series regex")
    })
}

fn noise_token() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(r"(?i)\b(?:2160p|1080p|720p|480p|web-?dl|webrip|bluray|brrip|x264|x265|h\.?264|h\.?265|hevc)\b")
            .expect("valid noise regex")
    })
}

#[cfg(test)]
mod tests {
    use super::{ParsedKind, normalized_title, parse_torrent_name};

    #[test]
    fn parses_movie_title_and_year() {
        let parsed = parse_torrent_name("Tuner.2025.1080p.WEB-DL.mkv", None).expect("parsed");
        assert_eq!(parsed.kind, ParsedKind::Movie);
        assert_eq!(parsed.title, "Tuner");
        assert_eq!(parsed.year, Some(2025));
    }

    #[test]
    fn parses_show_title_from_episode_marker() {
        let parsed =
            parse_torrent_name("[SubsPlease] Rurouni Kenshin (2023) - 43 (1080p).mkv", None)
                .expect("parsed");
        assert_eq!(parsed.kind, ParsedKind::Series);
        assert_eq!(parsed.title, "Rurouni Kenshin");
        assert_eq!(parsed.year, Some(2023));
    }

    #[test]
    fn normalized_title_keeps_non_latin_letters() {
        assert_eq!(normalized_title("かぐや様 2026!"), "かぐや様2026");
    }
}
