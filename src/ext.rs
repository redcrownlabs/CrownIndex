//! Imports EXT pages only when the operator has lawful access and robots permits it.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use regex::Regex;
use reqwest::Client;
use scraper::{Html, Selector};
use tokio::fs;
use tokio::time::sleep;
use url::Url;

use crate::record::{TorrentRecord, info_hash_from_magnet};

const MINIMUM_CRAWL_DELAY: Duration = Duration::from_secs(3);
const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug)]
struct RobotsRule {
    allow: bool,
    source_length: usize,
    matcher: Regex,
}

#[derive(Debug)]
struct RobotsPolicy {
    rules: Vec<RobotsRule>,
    crawl_delay: Duration,
}

pub(crate) async fn load_snapshots(directory: &Path) -> Result<Vec<TorrentRecord>> {
    let metadata = fs::metadata(directory).await.with_context(|| {
        format!(
            "failed to inspect snapshot directory {}",
            directory.display()
        )
    })?;
    if !metadata.is_dir() {
        anyhow::bail!("snapshot path is not a directory: {}", directory.display());
    }

    let mut pending = vec![directory.to_path_buf()];
    let mut files = Vec::new();
    while let Some(current) = pending.pop() {
        let mut entries = fs::read_dir(&current)
            .await
            .with_context(|| format!("failed to read {}", current.display()))?;
        while let Some(entry) = entries
            .next_entry()
            .await
            .context("failed to read directory entry")?
        {
            let file_type = entry
                .file_type()
                .await
                .context("failed to inspect snapshot entry")?;
            if file_type.is_symlink() {
                continue;
            }
            if file_type.is_dir() {
                pending.push(entry.path());
            } else if file_type.is_file()
                && entry
                    .path()
                    .extension()
                    .is_some_and(|extension| extension.eq_ignore_ascii_case("html"))
            {
                files.push(entry.path());
            }
        }
    }
    files.sort();

    let mut records = Vec::new();
    for file in files {
        let html = fs::read_to_string(&file)
            .await
            .with_context(|| format!("failed to read snapshot {}", file.display()))?;
        records.extend(
            parse_detail_page(&html)
                .with_context(|| format!("invalid snapshot {}", file.display()))?,
        );
    }
    Ok(deduplicate(records))
}

pub(crate) async fn crawl_live(
    base_url: &Url,
    user_agent: &str,
    page_count: u16,
) -> Result<Vec<TorrentRecord>> {
    let client = Client::builder()
        .user_agent(user_agent)
        .timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::limited(5))
        .build()
        .context("failed to create EXT HTTP client")?;
    let robots_url = base_url
        .join("robots.txt")
        .context("failed to construct robots URL")?;
    let robots = fetch_text(&client, robots_url.clone())
        .await
        .with_context(|| format!("cannot retrieve {robots_url}; live import fails closed"))?;
    let policy = parse_robots(&robots, "CrownIndex")?;
    let delay = policy.crawl_delay.max(MINIMUM_CRAWL_DELAY);

    let mut detail_urls = Vec::new();
    for page in 1..=page_count {
        let mut browse_url = base_url
            .join("browse/")
            .context("failed to construct browse URL")?;
        browse_url
            .query_pairs_mut()
            .append_pair("sort", "age")
            .append_pair("order", "desc")
            .append_pair("page", &page.to_string());
        ensure_allowed(&policy, &browse_url)?;
        if page > 1 {
            sleep(delay).await;
        }
        let html = fetch_text(&client, browse_url).await?;
        detail_urls.extend(parse_detail_links(base_url, &html)?);
    }
    detail_urls.sort_by(|left, right| left.as_str().cmp(right.as_str()));
    detail_urls.dedup();

    let mut records = Vec::new();
    for detail_url in detail_urls {
        ensure_allowed(&policy, &detail_url)?;
        sleep(delay).await;
        let html = fetch_text(&client, detail_url).await?;
        records.extend(parse_detail_page(&html)?);
    }
    Ok(deduplicate(records))
}

async fn fetch_text(client: &Client, url: Url) -> Result<String> {
    let response = client
        .get(url.clone())
        .send()
        .await
        .with_context(|| format!("request failed for {url}"))?
        .error_for_status()
        .with_context(|| format!("server rejected request for {url}"))?;
    if response
        .content_length()
        .is_some_and(|length| length > MAX_RESPONSE_BYTES as u64)
    {
        anyhow::bail!("response exceeded {MAX_RESPONSE_BYTES} bytes: {url}");
    }
    let bytes = response
        .bytes()
        .await
        .context("failed to read HTTP response")?;
    if bytes.len() > MAX_RESPONSE_BYTES {
        anyhow::bail!("response exceeded {MAX_RESPONSE_BYTES} bytes: {url}");
    }
    String::from_utf8(bytes.to_vec()).context("response was not valid UTF-8")
}

fn parse_detail_links(base_url: &Url, html: &str) -> Result<Vec<Url>> {
    let document = Html::parse_document(html);
    let selector = Selector::parse("a[href]")
        .map_err(|error| anyhow::anyhow!("invalid selector: {error:?}"))?;
    let mut result = Vec::new();
    for anchor in document.select(&selector) {
        let Some(href) = anchor.value().attr("href") else {
            continue;
        };
        let Ok(url) = base_url.join(href) else {
            continue;
        };
        let same_origin = url.scheme() == base_url.scheme()
            && url.host_str() == base_url.host_str()
            && url.port_or_known_default() == base_url.port_or_known_default();
        if same_origin
            && (url.path().starts_with("/torrent/") || url.path().starts_with("/torrents/"))
        {
            result.push(url);
        }
    }
    Ok(result)
}

pub(crate) fn parse_detail_page(html: &str) -> Result<Vec<TorrentRecord>> {
    let document = Html::parse_document(html);
    let magnet_selector = Selector::parse("a[href^='magnet:']")
        .map_err(|error| anyhow::anyhow!("invalid magnet selector: {error:?}"))?;
    let title_selector = Selector::parse("h1, title")
        .map_err(|error| anyhow::anyhow!("invalid title selector: {error:?}"))?;
    let page_title = document
        .select(&title_selector)
        .map(|node| node.text().collect::<String>().trim().to_owned())
        .find(|title| !title.is_empty());
    let visible_text = document.root_element().text().collect::<Vec<_>>().join(" ");

    let mut records = Vec::new();
    for anchor in document.select(&magnet_selector) {
        let Some(href) = anchor.value().attr("href") else {
            continue;
        };
        let magnet = Url::parse(href).context("invalid magnet URI")?;
        let info_hash = info_hash_from_magnet(&magnet)?;
        let query = magnet.query_pairs().collect::<HashMap<_, _>>();
        let name = anchor
            .value()
            .attr("data-name")
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(str::to_owned)
            .or_else(|| query.get("dn").map(|name| name.trim().to_string()))
            .filter(|name| !name.is_empty())
            .or_else(|| page_title.clone())
            .context("magnet has no usable name")?;
        let size = anchor
            .value()
            .attr("data-size")
            .and_then(|value| value.parse::<u64>().ok())
            .or_else(|| parse_size(&visible_text))
            .context("torrent page has no recognizable size")?;
        records.push(TorrentRecord::new("ext", info_hash, name, size)?);
    }
    if records.is_empty() {
        anyhow::bail!("torrent page contains no magnet links");
    }
    Ok(records)
}

fn parse_size(text: &str) -> Option<u64> {
    let pattern =
        Regex::new(r"(?i)\b(?<value>\d+(?:[.,]\d+)?)\s*(?<unit>bytes?|[kmgt]i?b)\b").ok()?;
    pattern.captures_iter(text).find_map(|captures| {
        let multiplier = match captures
            .name("unit")?
            .as_str()
            .to_ascii_lowercase()
            .as_str()
        {
            "byte" | "bytes" => 1_u64,
            "kb" => 1_000,
            "kib" => 1_024,
            "mb" => 1_000_000,
            "mib" => 1_048_576,
            "gb" => 1_000_000_000,
            "gib" => 1_073_741_824,
            "tb" => 1_000_000_000_000,
            "tib" => 1_099_511_627_776,
            _ => return None,
        };
        decimal_bytes(captures.name("value")?.as_str(), multiplier)
    })
}

fn decimal_bytes(value: &str, multiplier: u64) -> Option<u64> {
    let normalized = value.replace(',', ".");
    let (whole, fraction) = normalized
        .split_once('.')
        .map_or((normalized.as_str(), ""), |parts| parts);
    let whole_bytes = whole.parse::<u64>().ok()?.checked_mul(multiplier)?;
    if fraction.is_empty() {
        return Some(whole_bytes);
    }
    let denominator = 10_u64.checked_pow(u32::try_from(fraction.len()).ok()?)?;
    let fractional_bytes = fraction
        .parse::<u64>()
        .ok()?
        .checked_mul(multiplier)?
        .checked_add(denominator / 2)?
        .checked_div(denominator)?;
    whole_bytes.checked_add(fractional_bytes)
}

fn deduplicate(records: Vec<TorrentRecord>) -> Vec<TorrentRecord> {
    let mut hashes = HashSet::new();
    records
        .into_iter()
        .filter(|record| hashes.insert(record.info_hash.clone()))
        .collect()
}

#[allow(
    clippy::too_many_lines,
    reason = "robots group state and precedence are kept in one parser to preserve rule ordering semantics"
)]
fn parse_robots(text: &str, product_token: &str) -> Result<RobotsPolicy> {
    let mut groups = Vec::<(Vec<String>, Vec<(bool, String)>, Option<Duration>)>::new();
    let mut agents = Vec::new();
    let mut rules = Vec::new();
    let mut delay = None;
    let mut saw_rule = false;

    for raw_line in text.lines().chain(std::iter::once("")) {
        let line = raw_line.split('#').next().unwrap_or_default().trim();
        if line.is_empty() {
            if !agents.is_empty() {
                groups.push((
                    std::mem::take(&mut agents),
                    std::mem::take(&mut rules),
                    delay.take(),
                ));
            }
            saw_rule = false;
            continue;
        }
        let Some((field, value)) = line.split_once(':') else {
            continue;
        };
        let field = field.trim().to_ascii_lowercase();
        let value = value.trim();
        if field == "user-agent" {
            if saw_rule && !agents.is_empty() {
                groups.push((
                    std::mem::take(&mut agents),
                    std::mem::take(&mut rules),
                    delay.take(),
                ));
                saw_rule = false;
            }
            agents.push(value.to_ascii_lowercase());
        } else if !agents.is_empty() {
            match field.as_str() {
                "allow" => {
                    rules.push((true, value.to_owned()));
                    saw_rule = true;
                }
                "disallow" => {
                    if !value.is_empty() {
                        rules.push((false, value.to_owned()));
                    }
                    saw_rule = true;
                }
                "crawl-delay" => {
                    let seconds = value
                        .parse::<f64>()
                        .context("robots.txt has invalid Crawl-delay")?;
                    if !seconds.is_finite() || !(0.0..=86_400.0).contains(&seconds) {
                        anyhow::bail!("robots.txt Crawl-delay is outside the accepted range");
                    }
                    delay = Some(Duration::from_secs_f64(seconds));
                    saw_rule = true;
                }
                _ => {}
            }
        }
    }

    let token = product_token.to_ascii_lowercase();
    let exact: Vec<_> = groups
        .iter()
        .filter(|(agents, _, _)| agents.iter().any(|agent| agent == &token))
        .collect();
    let selected: Vec<_> = if exact.is_empty() {
        groups
            .iter()
            .filter(|(agents, _, _)| agents.iter().any(|agent| agent == "*"))
            .collect()
    } else {
        exact
    };
    if selected.is_empty() {
        return Ok(RobotsPolicy {
            rules: Vec::new(),
            crawl_delay: MINIMUM_CRAWL_DELAY,
        });
    }
    let crawl_delay = selected
        .iter()
        .filter_map(|(_, _, delay)| *delay)
        .max()
        .unwrap_or(MINIMUM_CRAWL_DELAY);
    let mut compiled = Vec::new();
    for (_, rules, _) in selected {
        for (allow, source) in rules {
            let (source, end_anchored) = source
                .strip_suffix('$')
                .map_or((source.as_str(), false), |value| (value, true));
            let body = source
                .split('*')
                .map(regex::escape)
                .collect::<Vec<_>>()
                .join(".*");
            let suffix = if end_anchored { "$" } else { "" };
            compiled.push(RobotsRule {
                allow: *allow,
                source_length: source.replace('*', "").len(),
                matcher: Regex::new(&format!("^{body}{suffix}"))
                    .context("failed to compile robots rule")?,
            });
        }
    }
    Ok(RobotsPolicy {
        rules: compiled,
        crawl_delay,
    })
}

fn ensure_allowed(policy: &RobotsPolicy, url: &Url) -> Result<()> {
    let mut target = url.path().to_owned();
    if let Some(query) = url.query() {
        target.push('?');
        target.push_str(query);
    }
    let decision = policy
        .rules
        .iter()
        .filter(|rule| rule.matcher.is_match(&target))
        .max_by_key(|rule| (rule.source_length, rule.allow))
        .is_none_or(|rule| rule.allow);
    if !decision {
        anyhow::bail!("robots.txt disallows {target}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{ensure_allowed, parse_detail_page, parse_robots};
    use url::Url;

    #[test]
    fn parses_fixture_record() {
        let html = include_str!("../fixtures/ext/detail.html");
        let records = parse_detail_page(html).expect("fixture should parse");
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].info_hash,
            "0123456789abcdef0123456789abcdef01234567"
        );
        assert_eq!(records[0].size, 2_684_354_560);
        assert_eq!(records[0].name, "Example Linux ISO 2026 x64");
    }

    #[test]
    fn robots_uses_longest_rule_and_exact_agent() {
        let robots = "User-agent: *\nDisallow: /\n\nUser-agent: CrownIndex\nDisallow: /browse/private\nAllow: /browse/private/public\nCrawl-delay: 4\n";
        let policy = parse_robots(robots, "CrownIndex").expect("valid robots");
        assert!(
            ensure_allowed(
                &policy,
                &Url::parse("https://ext.to/browse/").expect("valid URL")
            )
            .is_ok()
        );
        assert!(
            ensure_allowed(
                &policy,
                &Url::parse("https://ext.to/browse/private/item").expect("valid URL")
            )
            .is_err()
        );
        assert!(
            ensure_allowed(
                &policy,
                &Url::parse("https://ext.to/browse/private/public/item").expect("valid URL")
            )
            .is_ok()
        );
    }
}
