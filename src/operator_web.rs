//! Exposes the local operator portal and its authenticated JSON boundary.

use axum::extract::{Path, State};
use axum::http::header::{AUTHORIZATION, CONTENT_SECURITY_POLICY, CONTENT_TYPE};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tracing::{Level, event};

use crate::config::Secret;
use crate::store::CatalogStore;

const PORTAL_HTML: &str = include_str!("operator/index.html");
const PORTAL_CSS: &str = include_str!("operator/app.css");
const PORTAL_JS: &str = include_str!("operator/app.js");
const MAX_VISIBLE_JOBS: i64 = 50;

#[derive(Debug, Clone)]
struct PortalState {
    store: CatalogStore,
    indexers: Vec<String>,
    token: Option<Secret>,
}

#[derive(Debug, Serialize)]
struct PortalConfig {
    available: bool,
    authentication_required: bool,
    indexers: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct CreateSync {
    query: String,
    #[serde(default)]
    indexers: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: String,
}

pub(crate) fn router(store: CatalogStore, indexers: Vec<String>, token: Option<Secret>) -> Router {
    Router::new()
        .route("/operator", get(portal))
        .route("/operator/", get(portal))
        .route("/operator/app.css", get(styles))
        .route("/operator/app.js", get(script))
        .route("/operator/api/config", get(config))
        .route("/operator/api/jobs", get(jobs).post(create_job))
        .route("/operator/api/jobs/{id}", get(job))
        .with_state(PortalState {
            store,
            indexers,
            token,
        })
}

async fn portal() -> Response {
    asset_response("text/html; charset=utf-8", PORTAL_HTML, true)
}

async fn styles() -> Response {
    asset_response("text/css; charset=utf-8", PORTAL_CSS, false)
}

async fn script() -> Response {
    asset_response("text/javascript; charset=utf-8", PORTAL_JS, false)
}

async fn config(State(state): State<PortalState>, headers: HeaderMap) -> Response {
    if !authorized(&state, &headers) {
        return unauthorized();
    }
    Json(PortalConfig {
        available: !state.indexers.is_empty(),
        authentication_required: state.token.is_some(),
        indexers: state.indexers,
    })
    .into_response()
}

async fn jobs(State(state): State<PortalState>, headers: HeaderMap) -> Response {
    if !authorized(&state, &headers) {
        return unauthorized();
    }
    match state.store.operator_sync_jobs(MAX_VISIBLE_JOBS).await {
        Ok(jobs) => Json(jobs).into_response(),
        Err(error) => internal_error(&error),
    }
}

async fn job(
    State(state): State<PortalState>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> Response {
    if !authorized(&state, &headers) {
        return unauthorized();
    }
    match state.store.operator_sync_job(id).await {
        Ok(Some(job)) => Json(job).into_response(),
        Ok(None) => error_response(StatusCode::NOT_FOUND, "sync job not found"),
        Err(error) => internal_error(&error),
    }
}

async fn create_job(
    State(state): State<PortalState>,
    headers: HeaderMap,
    Json(request): Json<CreateSync>,
) -> Response {
    if !authorized(&state, &headers) {
        return unauthorized();
    }
    if headers
        .get("x-crown-index-intent")
        .and_then(|value| value.to_str().ok())
        != Some("operator-sync")
    {
        return error_response(
            StatusCode::BAD_REQUEST,
            "missing operator sync intent header",
        );
    }
    if state.indexers.is_empty() {
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "Jackett is not configured; set JACKETT_API_KEY first",
        );
    }
    let query = match validate_query(&request.query) {
        Ok(query) => query,
        Err(message) => return error_response(StatusCode::BAD_REQUEST, message),
    };
    let indexers = match select_indexers(&state.indexers, request.indexers) {
        Ok(indexers) => indexers,
        Err(message) => return error_response(StatusCode::BAD_REQUEST, &message),
    };
    match state.store.enqueue_operator_sync(&query, &indexers).await {
        Ok(job) => (StatusCode::ACCEPTED, Json(job)).into_response(),
        Err(error) => internal_error(&error),
    }
}

fn validate_query(value: &str) -> Result<String, &'static str> {
    let query = value.split_whitespace().collect::<Vec<_>>().join(" ");
    let length = query.chars().count();
    if !(3..=200).contains(&length) {
        return Err("query must contain between 3 and 200 characters");
    }
    if query.chars().any(char::is_control) {
        return Err("query cannot contain control characters");
    }
    Ok(query)
}

fn select_indexers(configured: &[String], requested: Vec<String>) -> Result<Vec<String>, String> {
    if requested.is_empty() {
        return Ok(configured.to_vec());
    }
    let mut selected = Vec::new();
    for indexer in requested {
        if !configured.contains(&indexer) {
            return Err(format!("indexer is not configured: {indexer}"));
        }
        if !selected.contains(&indexer) {
            selected.push(indexer);
        }
    }
    Ok(selected)
}

fn authorized(state: &PortalState, headers: &HeaderMap) -> bool {
    let Some(expected) = state.token.as_ref() else {
        return true;
    };
    let supplied = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    supplied.is_some_and(|value| secret_eq(expected.expose(), value))
}

fn unauthorized() -> Response {
    error_response(StatusCode::UNAUTHORIZED, "operator token required")
}

fn secret_eq(expected: &str, supplied: &str) -> bool {
    let expected = Sha256::digest(expected.as_bytes());
    let supplied = Sha256::digest(supplied.as_bytes());
    expected
        .iter()
        .zip(supplied)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

fn asset_response(content_type: &'static str, body: &'static str, document: bool) -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
    headers.insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    if document {
        headers.insert(
            CONTENT_SECURITY_POLICY,
            HeaderValue::from_static(
                "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'",
            ),
        );
    }
    (headers, body).into_response()
}

fn error_response(status: StatusCode, message: &str) -> Response {
    (
        status,
        Json(ErrorBody {
            error: message.to_owned(),
        }),
    )
        .into_response()
}

fn internal_error(error: &anyhow::Error) -> Response {
    event!(
        name: "operator.api.failed",
        Level::ERROR,
        error.message = %error,
        "operator API request failed"
    );
    error_response(StatusCode::INTERNAL_SERVER_ERROR, "operator request failed")
}

#[cfg(test)]
mod tests {
    use super::{secret_eq, select_indexers, validate_query};

    #[test]
    fn query_is_trimmed_and_internal_whitespace_is_normalized() {
        assert_eq!(
            validate_query("  Dark   Matter S02  ").expect("valid query"),
            "Dark Matter S02"
        );
        assert!(validate_query("a").is_err());
        assert!(validate_query(&"x".repeat(201)).is_err());
    }

    #[test]
    fn only_configured_indexers_can_be_selected() {
        let configured = vec!["yts".to_owned(), "eztv".to_owned()];
        assert_eq!(
            select_indexers(&configured, vec!["eztv".to_owned(), "eztv".to_owned()])
                .expect("selection"),
            vec!["eztv"]
        );
        assert!(select_indexers(&configured, vec!["unknown".to_owned()]).is_err());
        assert_eq!(
            select_indexers(&configured, Vec::new()).expect("all"),
            configured
        );
    }

    #[test]
    fn operator_tokens_are_compared_by_digest() {
        assert!(secret_eq("secret", "secret"));
        assert!(!secret_eq("secret", "other"));
    }
}
