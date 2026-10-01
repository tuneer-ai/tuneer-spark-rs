use std::collections::HashMap;

use arrow::array::RecordBatch;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use crate::federation::FederationError;

pub fn fetch_arrow_via_adbc(
    warehouse_type: &str,
    uri: Option<&str>,
    params: Option<&HashMap<String, String>>,
    table_ref: &str,
) -> Result<Vec<RecordBatch>, FederationError> {
    Python::attach(|py| {
        fetch_impl(py, warehouse_type, uri, params, table_ref)
            .map_err(|e| FederationError::AdbcFetch(format!("{e}")))
    })
}

fn fetch_impl(
    py: Python<'_>,
    warehouse_type: &str,
    uri: Option<&str>,
    params: Option<&HashMap<String, String>>,
    table_ref: &str,
) -> PyResult<Vec<RecordBatch>> {
    let conn = open_adbc_connection(py, warehouse_type, uri, params)?;

    let cursor = conn.call_method0("cursor")?;
    let sql = format!("SELECT * FROM {table_ref}");
    cursor.call_method1("execute", (sql,))?;
    let py_arrow_table = cursor.call_method0("fetch_arrow_table")?;
    cursor.call_method0("close")?;
    conn.call_method0("close")?;

    let batches_py = py_arrow_table.call_method0("to_batches")?;
    let batches_list = batches_py.cast::<PyList>()?;

    let mut result = Vec::with_capacity(batches_list.len());
    for batch_py in batches_list {
        let batch = arrow_pyarrow::FromPyArrow::from_pyarrow_bound(&batch_py)?;
        result.push(batch);
    }

    Ok(result)
}

fn open_adbc_connection<'py>(
    py: Python<'py>,
    warehouse_type: &str,
    uri: Option<&str>,
    params: Option<&HashMap<String, String>>,
) -> PyResult<Bound<'py, PyAny>> {
    match warehouse_type {
        "redshift" => {
            let driver = PyModule::import(py, "adbc_driver_manager")?.getattr("dbapi")?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("driver", "redshift")?;
            let db_kwargs = PyDict::new(py);
            if let Some(u) = uri {
                db_kwargs.set_item("uri", u)?;
            }
            kwargs.set_item("db_kwargs", db_kwargs)?;
            driver.call_method("connect", (), Some(&kwargs))
        }
        "snowflake" => {
            let driver = PyModule::import(py, "adbc_driver_snowflake")?.getattr("dbapi")?;
            let kwargs = PyDict::new(py);
            let db_kwargs = PyDict::new(py);
            if let Some(p) = params {
                for (k, v) in p {
                    db_kwargs.set_item(k, v)?;
                }
            }
            kwargs.set_item("db_kwargs", db_kwargs)?;
            driver.call_method("connect", (), Some(&kwargs))
        }
        "bigquery" => {
            let driver = PyModule::import(py, "adbc_driver_bigquery")?.getattr("dbapi")?;
            let kwargs = PyDict::new(py);
            let db_kwargs = PyDict::new(py);
            if let Some(p) = params {
                for (k, v) in p {
                    db_kwargs.set_item(k, v)?;
                }
            }
            kwargs.set_item("db_kwargs", db_kwargs)?;
            driver.call_method("connect", (), Some(&kwargs))
        }
        _ => Err(pyo3::exceptions::PyValueError::new_err(format!(
            "unsupported warehouse type: {warehouse_type}"
        ))),
    }
}
