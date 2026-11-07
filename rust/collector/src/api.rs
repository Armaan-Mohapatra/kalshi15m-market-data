use crate::event::utc_now;
use anyhow::{anyhow, bail, Context, Result};
use reqwest::{Client, StatusCode};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use uuid::Uuid;

pub const DEFAULT_BASE: &str = "https://external-api.kalshi.com/trade-api/v2";

#[derive(Debug)]
pub struct Response {
    pub body: Value,
    pub metadata: Value,
}

pub struct PublicRest {
    base: String,
    client: Client,
    last_at: Mutex<Option<Instant>>,
    min_spacing: Duration,
}

impl PublicRest {
    pub fn new(base: &str, min_spacing: Duration) -> Result<Self> {
        let clean = base.trim_end_matches('/');
        let official = clean == DEFAULT_BASE;
        let loopback =
            clean.starts_with("http://127.0.0.1:") || clean.starts_with("http://localhost:");
        if !(official || loopback) {
            bail!("REST base URL restricted to Kalshi official HTTPS or localhost test fixture")
        }
        let client = Client::builder()
            .user_agent("phoenix-kalshi15m-rust-staging/0.1 read-only")
            .timeout(Duration::from_secs(25))
            .build()?;
        Ok(Self {
            base: clean.to_owned(),
            client,
            last_at: Mutex::new(None),
            min_spacing,
        })
    }

    pub async fn get(&self, path: &str, params: &[(String, String)]) -> Result<Response> {
        if !matches!(
            path,
            "/markets" | "/markets/trades" | "/markets/orderbooks" | "/series"
        ) {
            bail!("route is not in the read-only GET allowlist: {path}")
        }
        let mut last = self.last_at.lock().await;
        if let Some(earlier) = *last {
            let due = earlier + self.min_spacing;
            if let Some(wait) = due.checked_duration_since(Instant::now()) {
                tokio::time::sleep(wait).await;
            }
        }
        // Serializing the GET budget through this mutex ensures no other
        // caller can bypass the configured minimum spacing.
        let mut last_error = anyhow!("not attempted");
        for retry in 0..4u32 {
            *last = Some(Instant::now());
            let started = Instant::now();
            let requested = utc_now();
            let request_id = Uuid::new_v4().simple().to_string();
            let result = self
                .client
                .get(format!("{}{}", self.base, path))
                .query(params)
                .send()
                .await;
            match result {
                Ok(resp) => {
                    let status = resp.status();
                    if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
                        last_error = anyhow!("Kalshi REST {status} on {path}");
                        if retry < 3 {
                            tokio::time::sleep(Duration::from_millis(250 * 2u64.pow(retry))).await;
                            continue;
                        }
                        break;
                    }
                    if !status.is_success() {
                        bail!("unretryable REST status {status} on {path}")
                    }
                    let server_date = resp
                        .headers()
                        .get(reqwest::header::DATE)
                        .and_then(|v| v.to_str().ok())
                        .map(str::to_owned);
                    let bytes = resp.bytes().await?;
                    if bytes.len() > 64 * 1024 * 1024 {
                        bail!("REST response exceeds 64 MiB limit")
                    }
                    let body: Value = serde_json::from_slice(&bytes)
                        .with_context(|| format!("invalid JSON from {path}"))?;
                    if !body.is_object() {
                        bail!("REST payload not an object")
                    }
                    return Ok(Response {
                        body,
                        metadata: json!({
                            "request_id":request_id,"path":path,
                            "request_started_utc":requested,
                            "response_observed_utc":utc_now(),
                            "duration_ms":started.elapsed().as_secs_f64()*1000.0,
                            "http_status":status.as_u16(),
                            "server_date_header":server_date,
                            "server_clock_offset_state":"NOT_MEASURED_COARSE_DATE_ONLY",
                        }),
                    });
                }
                Err(err) => {
                    last_error = anyhow!("REST network timeout/transport: {err}");
                    if retry < 3 {
                        tokio::time::sleep(Duration::from_millis(250 * 2u64.pow(retry))).await;
                    }
                }
            }
        }
        Err(last_error)
    }

    pub async fn pages(
        &self,
        path: &str,
        item_field: &str,
        params: &[(String, String)],
        max_pages: usize,
    ) -> Result<(Vec<Value>, Vec<Value>)> {
        if max_pages == 0 || max_pages > 20 {
            bail!("page limit outside 1..=20")
        }
        let mut all = Vec::new();
        let mut metadata = Vec::new();
        let mut cursors = BTreeSet::new();
        let mut cursor: Option<String> = None;
        for _ in 0..max_pages {
            let mut query = params.to_vec();
            if let Some(next) = &cursor {
                query.push(("cursor".into(), next.clone()));
            }
            let response = self.get(path, &query).await?;
            let items = response
                .body
                .get(item_field)
                .and_then(Value::as_array)
                .ok_or_else(|| anyhow!("missing {item_field} array"))?;
            all.extend(items.iter().cloned());
            metadata.push(response.metadata);
            let next = response
                .body
                .get("cursor")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_owned();
            if next.is_empty() {
                return Ok((all, metadata));
            }
            if !cursors.insert(next.clone()) {
                bail!("pagination cursor repeated on {path}")
            }
            cursor = Some(next);
        }
        bail!("page limit reached on {path}; data cannot be certified complete")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_nonread_routes_and_unapproved_endpoint() {
        assert!(PublicRest::new("https://evil.invalid/api", Duration::ZERO).is_err());
        let client = PublicRest::new(DEFAULT_BASE, Duration::ZERO).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        assert!(runtime
            .block_on(client.get("/portfolio/orders", &[]))
            .is_err());
    }
}
