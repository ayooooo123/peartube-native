//! A search across providers. Report each source when it finishes, but keep its own ranking and
//! the caller's source order in the final answer. A deadline ends the wait, not the provider's work.

use std::borrow::Cow;
use std::collections::HashSet;
use std::time::Duration;

use futures_util::stream::{FuturesUnordered, StreamExt};
use serde::Serialize;
use serde_json::{Map, Value};

use super::{Ctx, MirrorError, MirrorHost, Node, Plugin};
use crate::app_settings::{js_number, js_string, js_string_or_empty, js_truthy};
use crate::identify::js_num;
use crate::protocol::{js_trim, truncate_utf16};

#[derive(Serialize)]
pub struct SourceError {
    pub source: String,
    pub status: u16,
    pub message: String,
}

/// One `source` event. An unsuccessful source has no rows and does not suppress the other events.
#[derive(Serialize)]
pub struct SourcePart {
    pub source: String,
    pub results: Vec<Value>,
    pub truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<SourceError>,
}

pub type OnSource<'a> = &'a mut (dyn FnMut(&SourcePart) + Send + 'a);

/// The `done` event. It does not repeat rows that the client has already received.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchSummary {
    pub query: String,
    pub sources: Vec<String>,
    pub by_source: Map<String, Value>,
    pub truncated: Map<String, Value>,
    pub errors: Vec<SourceError>,
}

#[derive(Serialize)]
pub struct SearchReply {
    #[serde(flatten)]
    pub summary: SearchSummary,
    pub results: Vec<Value>,
}

type Selected<N> = Vec<(String, Option<Plugin<N>>)>;

impl<N: Node + 'static> MirrorHost<N> {
    /// The names and plugin handles at the start of the call. Do not hold the registry while a
    /// remote peer searches. With no list, select every provider in registration order.
    fn selected_sources(&self, sources: &Value) -> Result<Selected<N>, MirrorError> {
        let plugins = self.plugins.lock().expect("mirror plugins");
        let known: Vec<_> = plugins
            .iter()
            .filter(|(_, p)| p.provider().is_some())
            .collect();
        let known_names = || {
            known
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        };
        if sources.is_null() {
            return Ok(known
                .into_iter()
                .map(|(n, p)| (n.clone(), Some(p.clone())))
                .collect());
        }
        let list = sources
            .as_array()
            .filter(|a| !a.is_empty() && a.iter().all(Value::is_string))
            .ok_or_else(|| {
                MirrorError::with_status(
                    format!(
                        "sources must be a list of source names (known: {})",
                        known_names()
                    ),
                    400,
                )
            })?;
        let mut seen = HashSet::new();
        let mut selected = Vec::new();
        let mut first_unknown = None;
        for name in list
            .iter()
            .filter_map(Value::as_str)
            .filter(|&name| seen.insert(name))
        {
            let plugin = plugins.iter().find(|(n, _)| n == name).map(|(_, p)| p);
            if first_unknown.is_none() && plugin.is_none_or(|p| p.provider().is_none()) {
                first_unknown = Some(name);
            }
            selected.push((name.to_owned(), plugin.cloned()));
        }
        // The app tests the truth of the first unknown name. The empty string fails in the search,
        // not in this validation step. No other unknown name is accepted on its own.
        if let Some(name) = first_unknown.filter(|name| !name.is_empty()) {
            return Err(MirrorError::with_status(
                format!("Unknown source: {name} (known: {})", known_names()),
                400,
            ));
        }
        Ok(selected)
    }

    pub async fn search_sources(
        &self,
        payload: Value,
        mut on_source: Option<OnSource<'_>>,
    ) -> Result<SearchReply, MirrorError> {
        let query = js_string_or_empty(payload.get("query"));
        let q = js_trim(&query);
        if q.is_empty() {
            return Err(MirrorError::with_status("query is required", 400));
        }
        let selected = self.selected_sources(&payload["sources"])?;
        let number = match payload.get("timeoutMs") {
            Some(v @ Value::Array(_)) => js_number(&js_string(v)),
            given => js_num(given),
        };
        let ms = if number == 0.0 || number.is_nan() {
            30_000.0
        } else {
            number.clamp(1000.0, 45_000.0)
        };
        let limit = payload.get("limit").filter(|v| js_truthy(v));
        let mut pending = FuturesUnordered::new();
        let mut ordered: Vec<_> = (0..selected.len()).map(|_| None).collect();
        for (index, (name, plugin)) in selected.into_iter().enumerate() {
            let mut args = serde_json::json!({ "query": q });
            if let Some(limit) = limit {
                args["limit"] = limit.clone();
            }
            let ctx = self.ctx.clone();
            pending.push(async move {
                let answer = search_one(ctx, plugin, args, ms)
                    .await
                    .and_then(|reply| rows(reply, &name));
                let (results, truncated, error) = match answer {
                    Ok((rows, truncated)) => (rows, truncated, None),
                    Err(e) => (
                        Vec::new(),
                        false,
                        Some(SourceError {
                            source: name.clone(),
                            status: e.status.filter(|n| *n != 0).unwrap_or(502),
                            message: if e.message.is_empty() {
                                "Error".into()
                            } else {
                                truncate_utf16(&e.message, 300)
                            },
                        }),
                    ),
                };
                (
                    index,
                    SourcePart {
                        source: name,
                        results,
                        truncated,
                        error,
                    },
                )
            });
        }
        while let Some((index, part)) = pending.next().await {
            if let Some(emit) = on_source.as_mut() {
                emit(&part);
            }
            ordered[index] = Some(part);
        }
        let mut results =
            Vec::with_capacity(ordered.iter().flatten().map(|p| p.results.len()).sum());
        let mut summary = SearchSummary {
            query: q.to_owned(),
            sources: Vec::with_capacity(ordered.len()),
            by_source: Map::new(),
            truncated: Map::new(),
            errors: Vec::new(),
        };
        for part in ordered.into_iter().flatten() {
            summary
                .by_source
                .insert(part.source.clone(), part.results.len().into());
            summary
                .truncated
                .insert(part.source.clone(), part.truncated.into());
            summary.sources.push(part.source);
            results.extend(part.results);
            summary.errors.extend(part.error);
        }
        Ok(SearchReply { summary, results })
    }
}

async fn search_one<N: Node + 'static>(
    ctx: Ctx<N>,
    plugin: Option<Plugin<N>>,
    args: Value,
    ms: f64,
) -> Result<Value, MirrorError> {
    let mut search = Box::pin(async move {
        let p = plugin.ok_or_else(|| {
            MirrorError::plain("Cannot read properties of undefined (reading 'search')")
        })?;
        let provider = p
            .provider()
            .ok_or_else(|| MirrorError::plain("plugins.get(...).search is not a function"))?;
        provider.search(args, &ctx).await
    });
    match tokio::time::timeout(Duration::from_millis(ms as u64), &mut search).await {
        Ok(answer) => answer,
        Err(_) => {
            // Like Promise.race: a late answer cannot change the event, but the provider still
            // finishes its search and updates its own state. Fast answers need no separate task.
            tokio::spawn(search);
            Err(MirrorError::with_status(
                format!("No answer in {} s", (ms / 1000.0).round()),
                504,
            ))
        }
    }
}

fn rows(mut reply: Value, name: &str) -> Result<(Vec<Value>, bool), MirrorError> {
    let truncated = reply.get("truncated").is_some_and(js_truthy);
    let Some(candidates) = reply.get_mut("candidates").filter(|v| js_truthy(v)) else {
        return Ok((Vec::new(), truncated));
    };
    let Value::Array(mut rows) = candidates.take() else {
        return Err(MirrorError::plain(
            "((intermediate value) || []).map is not a function",
        ));
    };
    for row in &mut rows {
        if row.is_null() {
            return Err(MirrorError::plain(
                "Cannot read properties of null (reading 'title')",
            ));
        }
        let mut fields = match row.take() {
            Value::Object(fields) => fields,
            Value::Array(items) => items
                .into_iter()
                .enumerate()
                .map(|(i, v)| (i.to_string(), v))
                .collect(),
            Value::String(text) => text
                .encode_utf16()
                .enumerate()
                .map(|(i, c)| (i.to_string(), String::from_utf16_lossy(&[c]).into()))
                .collect(),
            _ => Map::new(),
        };
        let title = fields.shift_remove("title");
        let lossy = fields.shift_remove("lossy");
        let file = match fields.get("filename") {
            Some(Value::String(s)) => Cow::Borrowed(s.as_str()),
            v => Cow::Owned(js_string_or_empty(v)),
        };
        let mut normal = Map::with_capacity(fields.len() + 3);
        normal.insert(
            "title".into(),
            title.unwrap_or_else(|| file.rsplit(['/', '\\']).next().unwrap_or("").into()),
        );
        normal.insert(
            "lossy".into(),
            lossy.unwrap_or_else(|| {
                if file.rsplit_once('.').is_some_and(|(_, ext)| {
                    ["mp3", "aac", "opus", "wma"]
                        .iter()
                        .any(|s| ext.eq_ignore_ascii_case(s))
                }) {
                    Value::Bool(true)
                } else {
                    Value::Null
                }
            }),
        );
        normal.extend(fields);
        normal.insert("source".into(), name.into());
        *row = Value::Object(normal);
    }
    Ok((rows, truncated))
}
