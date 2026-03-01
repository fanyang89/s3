//! Raw FFI bindings to libchdb.
//!
//! Mirrors the new (non-deprecated) API from chdb.h.

use std::ffi::{c_char, c_double, c_int};
use std::os::raw::c_void;

// ---------------------------------------------------------------------------
// Opaque handle types
// ---------------------------------------------------------------------------

/// Opaque query result handle.
#[repr(C)]
pub struct ChdbResult {
    internal_data: *mut c_void,
}

/// Opaque connection handle (the header typedef'd this as a *pointer*).
/// chdb_connection == *chdb_connection_
#[repr(C)]
pub struct ChdbConnectionInner {
    internal_data: *mut c_void,
}

pub type ChdbConnection = *mut ChdbConnectionInner;

// ---------------------------------------------------------------------------
// extern "C" declarations
// ---------------------------------------------------------------------------

#[link(name = "chdb")]
unsafe extern "C" {
    /// Open a chDB connection.
    /// argv may include `--path=<db_path>` to persist to disk; omit for `:memory:`.
    pub fn chdb_connect(argc: c_int, argv: *const *const c_char) -> *mut ChdbConnection;

    /// Close a connection obtained from `chdb_connect`.
    pub fn chdb_close_conn(conn: *mut ChdbConnection);

    /// Execute a query and return a result handle.
    /// `format` is e.g. `"JSONEachRow"`, `"CSV"`, `"TabSeparated"` …
    pub fn chdb_query(
        conn: ChdbConnection,
        query: *const c_char,
        format: *const c_char,
    ) -> *mut ChdbResult;

    /// Binary-safe variant of `chdb_query`.
    pub fn chdb_query_n(
        conn: ChdbConnection,
        query: *const c_char,
        query_len: usize,
        format: *const c_char,
        format_len: usize,
    ) -> *mut ChdbResult;

    /// Free a result handle.
    pub fn chdb_destroy_query_result(result: *mut ChdbResult);

    // --- Accessors on ChdbResult ---

    pub fn chdb_result_buffer(result: *mut ChdbResult) -> *const c_char;
    pub fn chdb_result_length(result: *mut ChdbResult) -> usize;
    pub fn chdb_result_elapsed(result: *mut ChdbResult) -> c_double;
    pub fn chdb_result_rows_read(result: *mut ChdbResult) -> u64;
    pub fn chdb_result_error(result: *mut ChdbResult) -> *const c_char;
}
