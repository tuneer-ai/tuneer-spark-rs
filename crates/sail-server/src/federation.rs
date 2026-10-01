use std::collections::HashMap;
use std::sync::RwLock;
use std::time::Instant;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdbcSource {
    pub warehouse_type: String,
    pub uri: Option<String>,
    pub params: Option<HashMap<String, String>>,
}

pub struct FederationManager {
    adbc_sources: RwLock<HashMap<String, AdbcSource>>,
    ready: RwLock<bool>,
    last_activity: RwLock<Instant>,
    query_count: RwLock<u64>,
}

impl FederationManager {
    pub fn new() -> Self {
        Self {
            adbc_sources: RwLock::new(HashMap::new()),
            ready: RwLock::new(false),
            last_activity: RwLock::new(Instant::now()),
            query_count: RwLock::new(0),
        }
    }

    pub fn set_ready(&self, val: bool) {
        if let Ok(mut r) = self.ready.write() {
            *r = val;
        }
    }

    pub fn is_ready(&self) -> bool {
        self.ready.read().map(|r| *r).unwrap_or(false)
    }

    pub fn mount_warehouse(
        &self,
        name: &str,
        warehouse_type: &str,
        uri: Option<String>,
        params: Option<HashMap<String, String>>,
    ) -> Result<String, FederationError> {
        let wt = warehouse_type.to_lowercase();
        if !matches!(wt.as_str(), "redshift" | "snowflake" | "bigquery") {
            return Err(FederationError::UnsupportedWarehouse(
                warehouse_type.to_string(),
            ));
        }
        let safe_name = name
            .replace('-', "_")
            .replace(' ', "_")
            .chars()
            .take(32)
            .collect::<String>();
        let source = AdbcSource {
            warehouse_type: wt,
            uri,
            params,
        };
        if let Ok(mut sources) = self.adbc_sources.write() {
            sources.insert(safe_name, source);
        }
        Ok("registered".to_string())
    }

    pub fn mounted_warehouses(&self) -> Vec<String> {
        self.adbc_sources
            .read()
            .map(|s| s.keys().cloned().collect())
            .unwrap_or_default()
    }

    pub fn get_source(&self, name: &str) -> Option<AdbcSource> {
        self.adbc_sources
            .read()
            .ok()
            .and_then(|s| s.get(name).cloned())
    }

    pub fn get_referenced_sources(&self, sql: &str) -> Vec<(String, AdbcSource)> {
        let sql_lower = sql.to_lowercase();
        self.adbc_sources
            .read()
            .map(|sources| {
                sources
                    .iter()
                    .filter(|(name, _)| sql_lower.contains(&name.to_lowercase()))
                    .map(|(name, src)| (name.clone(), src.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn touch_activity(&self) {
        if let Ok(mut t) = self.last_activity.write() {
            *t = Instant::now();
        }
    }

    pub fn idle_seconds(&self) -> u64 {
        self.last_activity
            .read()
            .map(|t| t.elapsed().as_secs())
            .unwrap_or(0)
    }

    pub fn increment_query_count(&self) {
        if let Ok(mut c) = self.query_count.write() {
            *c += 1;
        }
    }

    pub fn query_count(&self) -> u64 {
        self.query_count.read().map(|c| *c).unwrap_or(0)
    }

    pub fn extract_table_refs(sql: &str, source_name: &str) -> Vec<String> {
        let pattern = format!(
            r"(?i)\b{}\.([\w]+(?:\.[\w]+)?)\b",
            regex::escape(source_name)
        );
        if let Ok(re) = regex::Regex::new(&pattern) {
            re.captures_iter(sql)
                .filter_map(|c| c.get(1).map(|m| m.as_str().to_string()))
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .collect()
        } else {
            Vec::new()
        }
    }

    pub fn rewrite_sql(sql: &str, original_ref: &str, view_name: &str) -> String {
        let pattern = format!(r"(?i)\b{}\b", regex::escape(original_ref));
        regex::Regex::new(&pattern)
            .map(|re| re.replace_all(sql, view_name).to_string())
            .unwrap_or_else(|_| sql.to_string())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FederationError {
    #[error("unsupported warehouse type: {0}")]
    UnsupportedWarehouse(String),
    #[error("ADBC fetch failed: {0}")]
    AdbcFetch(String),
    #[error("query execution failed: {0}")]
    QueryExecution(String),
}
