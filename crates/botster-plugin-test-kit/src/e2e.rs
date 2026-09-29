//! The real-daemon end-to-end mode (`botster-plugin-test --e2e`).
//!
//! Each test starts a real `botster-hub` daemon process in an isolated data
//! directory (`IsolatedHub`) and drives it only through its socket protocol,
//! as a client does. Every wait is a daemon response; nothing sleeps. The
//! Hub binary comes from `BOTSTER_HUB_BIN`, `BOTSTER_SESSION_WORKER_BIN`, and
//! `BOTSTER_CANDIDATE_MANIFEST`, the same variables the Hub's own gate sets.
//!
//! The driver has only the verbs that a client can serve: `load`,
//! `try_load`, `request`, `call_tool`, `tools`, `logs`, and the assertions.
//! Every other verb raises `unsupported_by_kit`; use the in-process kit for
//! the logical clock, session input, and plugin_db reads.

use std::path::{Path, PathBuf};

use botster_hub_client::{DaemonRequest, DaemonResponse, request};
use botster_hub_test_support::{
    IsolatedHub, IsolatedHubBuilder, copy_plugin_contract_matrix_fixture,
    run_plugin_contract_matrix_conformance,
};
use mlua::{Function, Lua, LuaSerdeExt, MultiValue, Scope, Table, Value};

use crate::spec::{
    install_assertions, json_of, package_name, response_table, serialize, unsupported,
};

/// A builder with a short data root and name. A daemon socket path must fit
/// in `SUN_LEN` (104 bytes on macOS), and a checkout path plus a test name
/// can exceed it.
fn short_hub_builder(kind: &str) -> IsolatedHubBuilder {
    static NEXT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let index = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    IsolatedHubBuilder::new()
        .root(std::env::temp_dir().join("bpt"))
        .name(format!("{kind}{}-{index}", std::process::id() % 100_000))
}

fn runtime_error(message: impl Into<String>) -> mlua::Error {
    mlua::Error::runtime(message.into())
}

fn send(hub: &IsolatedHub, daemon_request: DaemonRequest) -> mlua::Result<DaemonResponse> {
    request(hub.endpoint(), daemon_request)
        .map_err(|error| runtime_error(format!("daemon request failed: {error:?}")))
}

/// Run one test against a fresh isolated Hub.
pub(crate) fn run_test(
    lua: &Lua,
    plugin_directory: &Path,
    label: &str,
    body: &Function,
) -> mlua::Result<()> {
    let _ = label;
    let hub = short_hub_builder("t")
        .start()
        .map_err(|error| runtime_error(format!("cannot start the isolated hub: {error}")))?;
    let outcome = lua.scope(|scope| {
        let driver = build_driver(lua, scope, &hub, plugin_directory)?;
        body.call::<()>(driver)
    });
    let shutdown = hub.shutdown();
    outcome?;
    shutdown.map_err(|error| runtime_error(format!("cannot shut the isolated hub down: {error}")))
}

/// Refuse every verb that the mode does not serve, by name.
fn refuse_unknown_verbs<'scope, 'env>(
    lua: &Lua,
    scope: &'scope Scope<'scope, 'env>,
    table: &Table,
    owner: &'static str,
) -> mlua::Result<()> {
    let metatable = lua.create_table()?;
    metatable.set(
        "__index",
        scope.create_function(
            move |_, (_, verb): (Value, String)| -> mlua::Result<Value> {
                Err(runtime_error(format!(
                    "unsupported_by_kit: {owner}:{verb} is not available in --e2e mode; \
                 use the in-process kit"
                )))
            },
        )?,
    )?;
    table.set_metatable(Some(metatable))?;
    Ok(())
}

fn build_driver<'scope, 'env>(
    lua: &Lua,
    scope: &'scope Scope<'scope, 'env>,
    hub: &'env IsolatedHub,
    plugin_directory: &'env Path,
) -> mlua::Result<Table> {
    let t = lua.create_table()?;

    let enable = move |lua: &Lua, path: &str| -> mlua::Result<Result<String, Table>> {
        let directory: PathBuf = plugin_directory.join(path);
        let response = send(
            hub,
            DaemonRequest::EnablePackageLocalPath {
                path: directory.clone(),
            },
        )?;
        if let Some(error) = &response.error {
            let detail = lua.create_table()?;
            detail.set("kind", error.code.clone())?;
            detail.set("message", error.message.clone())?;
            return Ok(Err(detail));
        }
        Ok(Ok(package_name(&directory)?))
    };
    let enable = std::sync::Arc::new(enable);

    let strict = std::sync::Arc::clone(&enable);
    t.set(
        "load",
        scope.create_function(move |lua, (_, path): (Value, String)| {
            match strict(lua, &path)? {
                Ok(name) => plugin_table(lua, scope, hub, name),
                Err(error) => {
                    let kind: String = error.get("kind")?;
                    let message: String = error.get("message")?;
                    Err(runtime_error(format!(
                        "the plugin at {path:?} failed to load: {kind}: {message}"
                    )))
                }
            }
        })?,
    )?;
    let lenient = std::sync::Arc::clone(&enable);
    t.set(
        "try_load",
        scope.create_function(move |lua, (_, path): (Value, String)| {
            match lenient(lua, &path)? {
                Ok(name) => Ok((
                    Value::Table(plugin_table(lua, scope, hub, name)?),
                    Value::Nil,
                )),
                Err(error) => Ok((Value::Nil, Value::Table(error))),
            }
        })?,
    )?;
    t.set(
        "request",
        scope.create_function(move |lua, (_, daemon_request): (Value, Value)| {
            let daemon_request: DaemonRequest =
                lua.from_value(daemon_request).map_err(|error| {
                    runtime_error(format!(
                        "t:request takes a DaemonRequest table with a type tag: {error}"
                    ))
                })?;
            let response = send(hub, daemon_request)?;
            response_table(lua, &response)
        })?,
    )?;
    install_assertions(scope, &t)?;
    refuse_unknown_verbs(lua, scope, &t, "t")?;
    Ok(t)
}

fn plugin_table<'scope, 'env>(
    lua: &Lua,
    scope: &'scope Scope<'scope, 'env>,
    hub: &'env IsolatedHub,
    name: String,
) -> mlua::Result<Table> {
    let p = lua.create_table()?;
    p.set("name", name.clone())?;
    p.set(
        "call_tool",
        scope.create_function(
            move |lua, (_, tool, arguments, options): (Value, String, Value, Option<Table>)| {
                if let Some(options) = options {
                    for feature in ["caller", "token"] {
                        if !matches!(options.get::<Value>(feature)?, Value::Nil) {
                            return unsupported(lua, feature, "G1");
                        }
                    }
                }
                let arguments = match arguments {
                    Value::Nil => serde_json::json!({}),
                    other => json_of(lua, other)?,
                };
                let response = send(
                    hub,
                    DaemonRequest::PluginMcpCallTool {
                        name: tool,
                        arguments,
                    },
                )?;
                response_table(lua, &response)
            },
        )?,
    )?;
    p.set(
        "tools",
        scope.create_function(move |lua, _: Value| {
            let response = send(hub, DaemonRequest::PluginMcpListTools)?;
            serialize(lua, &response.plugin_tools)
        })?,
    )?;
    p.set(
        "logs",
        scope.create_function(move |lua, _: Value| {
            let response = send(
                hub,
                DaemonRequest::ReadPluginLogs {
                    package_name: name.clone(),
                    after_seq: 0,
                },
            )?;
            if let Some(error) = response.error {
                return Err(runtime_error(format!(
                    "read_plugin_logs: {}",
                    error.message
                )));
            }
            let logs = response
                .plugin_logs
                .ok_or_else(|| runtime_error("read_plugin_logs returned no logs"))?;
            serialize(lua, &logs.records)
        })?,
    )?;
    refuse_unknown_verbs(lua, scope, &p, "p")?;
    Ok(p)
}

/// Run the Hub's plugin contract matrix conformance against a real daemon
/// and return its report. The helper checks the bundled matrix package, so
/// this is a check of the Hub, not of the plugin under test.
pub fn run_conformance() -> Result<String, String> {
    let hub = short_hub_builder("c")
        .start()
        .map_err(|error| format!("cannot start the isolated hub: {error}"))?;
    let directory =
        std::env::temp_dir().join(format!("botster-plugin-conformance-{}", std::process::id()));
    let package = copy_plugin_contract_matrix_fixture(&directory)
        .map_err(|error| format!("cannot copy the matrix fixture: {error}"))?;
    let report = run_plugin_contract_matrix_conformance(&hub, package)
        .map(|report| format!("{report:#?}"))
        .map_err(|error| format!("contract matrix conformance failed: {error}"));
    let _ = std::fs::remove_dir_all(&directory);
    hub.shutdown()
        .map_err(|error| format!("cannot shut the isolated hub down: {error}"))?;
    report
}
