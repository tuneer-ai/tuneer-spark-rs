use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::RecordBatch;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};
use serde::{Deserialize, Serialize};

use crate::adbc;
use crate::federation::FederationManager;

type AppState = Arc<FederationManager>;

pub fn build_router(federation: Arc<FederationManager>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/mount-warehouses", post(mount_warehouses))
        .route("/query/execute", post(execute_query))
        .with_state(federation)
}

async fn health() -> impl IntoResponse {
    Json(serde_json::json!({"status": "ok"}))
}

async fn ready(State(fm): State<AppState>) -> impl IntoResponse {
    if !fm.is_ready() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"status": "initializing"})),
        );
    }
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "status": "ready",
            "idle_seconds": fm.idle_seconds(),
            "query_count": fm.query_count(),
            "mounted_warehouses": fm.mounted_warehouses(),
        })),
    )
}

#[derive(Deserialize)]
struct WarehouseMount {
    name: String,
    warehouse_type: String,
    uri: Option<String>,
    params: Option<HashMap<String, String>>,
}

#[derive(Deserialize)]
struct MountWarehousesRequest {
    warehouses: Vec<WarehouseMount>,
}

#[derive(Serialize)]
struct MountResult {
    name: String,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    detail: Option<String>,
}

async fn mount_warehouses(
    State(fm): State<AppState>,
    Json(body): Json<MountWarehousesRequest>,
) -> impl IntoResponse {
    if !fm.is_ready() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "engine not ready"})),
        );
    }
    let mut results = Vec::with_capacity(body.warehouses.len());
    for wh in body.warehouses {
        match fm.mount_warehouse(&wh.name, &wh.warehouse_type, wh.uri, wh.params) {
            Ok(status) => results.push(MountResult {
                name: wh.name,
                status,
                detail: None,
            }),
            Err(e) => results.push(MountResult {
                name: wh.name,
                status: "error".to_string(),
                detail: Some(e.to_string()),
            }),
        }
    }
    let total = fm.mounted_warehouses().len();
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "mounted": results,
            "total_warehouses": total,
        })),
    )
}

#[derive(Deserialize)]
struct QueryRequest {
    sql: String,
    #[serde(default = "default_limit")]
    limit: usize,
    #[serde(default)]
    mode: Option<String>,
}

fn default_limit() -> usize {
    1000
}

#[derive(Serialize)]
struct QueryResponse {
    columns: Vec<String>,
    rows: Vec<Vec<serde_json::Value>>,
    row_count: usize,
    truncated: bool,
    elapsed_ms: f64,
}

async fn execute_query(
    State(fm): State<AppState>,
    Json(body): Json<QueryRequest>,
) -> impl IntoResponse {
    if !fm.is_ready() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "engine not ready"})),
        );
    }
    fm.touch_activity();
    fm.increment_query_count();

    let start = std::time::Instant::now();
    let is_federated = body.mode.as_deref() == Some("federated");
    let limit = body.limit.min(10_000).max(1);

    let sql = if is_federated {
        match prepare_federated(&fm, &body.sql) {
            Ok(rewritten) => rewritten,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"error": e.to_string()})),
                );
            }
        }
    } else {
        body.sql.clone()
    };

    let result = tokio::task::spawn_blocking(move || execute_on_flight(&sql, limit)).await;

    let elapsed = start.elapsed().as_secs_f64() * 1000.0;

    match result {
        Ok(Ok(resp)) => {
            let response = QueryResponse {
                row_count: resp.rows.len().min(limit),
                truncated: resp.rows.len() > limit,
                columns: resp.columns,
                rows: resp.rows.into_iter().take(limit).collect(),
                elapsed_ms: elapsed,
            };
            (StatusCode::OK, Json(serde_json::to_value(response).unwrap_or_default()))
        }
        Ok(Err(e)) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": format!("task join error: {e}")})),
        ),
    }
}

fn prepare_federated(
    fm: &FederationManager,
    sql: &str,
) -> Result<String, crate::federation::FederationError> {
    let referenced = fm.get_referenced_sources(sql);
    if referenced.is_empty() {
        return Ok(sql.to_string());
    }

    let mut rewritten = sql.to_string();

    for (source_name, source) in &referenced {
        let table_refs = FederationManager::extract_table_refs(sql, source_name);
        for table_ref in &table_refs {
            let batches = adbc::fetch_arrow_via_adbc(
                &source.warehouse_type,
                source.uri.as_deref(),
                source.params.as_ref(),
                table_ref,
            )?;

            let view_name = format!(
                "__{}_{}", source_name, table_ref.replace('.', "_")
            );

            register_arrow_on_flight(&view_name, &batches)
                .map_err(crate::federation::FederationError::QueryExecution)?;

            let original_ref = format!("{source_name}.{table_ref}");
            rewritten = FederationManager::rewrite_sql(&rewritten, &original_ref, &view_name);
        }
    }

    Ok(rewritten)
}

struct RawResult {
    columns: Vec<String>,
    rows: Vec<Vec<serde_json::Value>>,
}

fn execute_on_flight(sql: &str, limit: usize) -> Result<RawResult, String> {
    Python::attach(|py| {
        execute_on_flight_impl(py, sql, limit)
    })
    .map_err(|e| format!("Flight SQL execution failed: {e}"))
}

fn execute_on_flight_impl(
    py: Python<'_>,
    sql: &str,
    limit: usize,
) -> PyResult<RawResult> {
    let flight_sql = PyModule::import(py, "adbc_driver_flightsql")?.getattr("dbapi")?;
    let conn = flight_sql.call_method1("connect", ("grpc://127.0.0.1:32010",))?;
    let cursor = conn.call_method0("cursor")?;

    let limited_sql = format!(
        "SELECT * FROM ({}) AS __q__ LIMIT {}",
        sql.trim_end_matches(';'),
        limit + 1
    );
    cursor.call_method1("execute", (limited_sql,))?;

    let description = cursor.getattr("description")?;
    let columns: Vec<String> = if description.is_none() {
        Vec::new()
    } else {
        let mut cols = Vec::new();
        let iter = description.try_iter()?;
        for item_result in iter {
            if let Ok(item) = item_result {
                if let Ok(name_obj) = item.get_item(0) {
                    if let Ok(name) = name_obj.extract::<String>() {
                        cols.push(name);
                    }
                }
            }
        }
        cols
    };

    let py_rows = cursor.call_method1("fetchmany", (limit + 1,))?;
    let rows_list = py_rows.cast::<PyList>()?;

    let mut rows = Vec::with_capacity(rows_list.len());
    for row_item in rows_list {
        let row_tuple = row_item.cast::<PyTuple>()?;
        let mut row = Vec::with_capacity(row_tuple.len());
        for cell in row_tuple {
            let val = py_cell_to_json(&cell);
            row.push(val);
        }
        rows.push(row);
    }

    cursor.call_method0("close")?;
    conn.call_method0("close")?;

    Ok(RawResult { columns, rows })
}

fn register_arrow_on_flight(view_name: &str, batches: &[RecordBatch]) -> Result<(), String> {
    if batches.is_empty() {
        return Ok(());
    }
    Python::attach(|py| {
        register_arrow_impl(py, view_name, batches)
    })
    .map_err(|e| format!("Failed to register Arrow on Flight: {e}"))
}

fn register_arrow_impl(
    py: Python<'_>,
    view_name: &str,
    batches: &[RecordBatch],
) -> PyResult<()> {
    let pa = PyModule::import(py, "pyarrow")?;

    let py_batches: Vec<_> = batches
        .iter()
        .map(|b| arrow_pyarrow::ToPyArrow::to_pyarrow(b, py))
        .collect::<PyResult<Vec<_>>>()?;

    let py_list = PyList::new(py, py_batches)?;
    let table_cls = pa.getattr("Table")?;
    let table = table_cls.call_method1("from_batches", (py_list,))?;

    let pq = PyModule::import(py, "pyarrow.parquet")?;
    let fs_mod = PyModule::import(py, "pyarrow.fs")?;
    let s3fs_cls = fs_mod.getattr("S3FileSystem")?;
    let s3fs = s3fs_cls.call0()?;

    let s3_base = std::env::var("S3_BASE").unwrap_or_default();
    let path = format!(
        "{}/_sail_staging/{}.parquet",
        s3_base.trim_start_matches("s3://"),
        view_name
    );
    let write_kwargs = PyDict::new(py);
    write_kwargs.set_item("filesystem", s3fs)?;
    pq.call_method("write_table", (table, &path), Some(&write_kwargs))?;

    let flight_sql = PyModule::import(py, "adbc_driver_flightsql")?.getattr("dbapi")?;
    let conn = flight_sql.call_method1("connect", ("grpc://127.0.0.1:32010",))?;
    let cursor = conn.call_method0("cursor")?;
    let ddl = format!(
        "CREATE OR REPLACE TEMPORARY VIEW {view_name} USING parquet OPTIONS (path \"s3://{path}\")"
    );
    cursor.call_method1("execute", (ddl,))?;
    cursor.call_method0("close")?;
    conn.call_method0("close")?;

    Ok(())
}

fn py_cell_to_json(cell: &Bound<'_, PyAny>) -> serde_json::Value {
    if cell.is_none() {
        return serde_json::Value::Null;
    }
    if let Ok(v) = cell.extract::<i64>() {
        return serde_json::Value::Number(v.into());
    }
    if let Ok(v) = cell.extract::<f64>() {
        return serde_json::Number::from_f64(v)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null);
    }
    if let Ok(v) = cell.extract::<bool>() {
        return serde_json::Value::Bool(v);
    }
    if let Ok(v) = cell.extract::<String>() {
        return serde_json::Value::String(v);
    }
    cell.str()
        .map(|s| serde_json::Value::String(s.to_string()))
        .unwrap_or(serde_json::Value::Null)
}
