const SANDBOX_PROBE_PLUGIN: &str = r#"
local function from_hex(hex)
  return (hex:gsub("..", function(pair) return string.char(tonumber(pair, 16)) end))
end

return botster.register({
  tools = {{
    name = "sandbox.probe",
    description = "Report which base-library escapes the plugin sandbox allows.",
    handler = "probe",
    call = function(args)
      local dofile_ok, dofile_value = pcall(function() return dofile(args.host_file) end)
      local loadfile_ok, loadfile_value = pcall(function() return loadfile(args.host_file)() end)
      local binary_chunk, binary_error = load(from_hex(args.bytecode_hex))
      local collect_ok = pcall(collectgarbage, "collect")
      return {
        dofile = { ok = dofile_ok, value = tostring(dofile_value) },
        loadfile = { ok = loadfile_ok, value = tostring(loadfile_value) },
        binary_load = { loaded = binary_chunk ~= nil, error = tostring(binary_error) },
        string_dump = string.dump ~= nil or ("").dump ~= nil,
        print = print ~= nil,
        collect = collect_ok,
        count = math.type(collectgarbage("count")),
        text_load = load("return botster ~= nil and 40 + 2")(),
      }
    end,
  }, {
    name = "sandbox.absorb",
    description = "Try to outlive the instruction budget inside protected calls.",
    handler = "absorb",
    call = function()
      local function spin() for _ = 1, 2000 do end end
      for _ = 1, 100000 do pcall(spin) end
      return { survived = true }
    end,
  }},
})
"#;

fn write_sandbox_probe_package(root: &Path) {
    fs::create_dir_all(root).expect("create sandbox probe package root");
    fs::write(root.join("plugin.lua"), SANDBOX_PROBE_PLUGIN).expect("write sandbox probe plugin");
    fs::write(
        root.join("botster-package.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "name": "sandbox.probe",
            "version": "1.0.0",
            "kind": "plugin",
            "botster": ">=0.1.0",
            "source": { "type": "path", "path": "." },
            "capabilities": [{ "surface": "mcp" }],
            "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }]
        }))
        .expect("serialize sandbox probe manifest"),
    )
    .expect("write sandbox probe manifest");
}

/// A daemon-loaded plugin cannot read host files, run bytecode, dump bytecode,
/// write daemon stdio, drive the collector, or absorb the instruction budget in
/// protected calls, while text `load` still works.
#[test]
fn live_daemon_plugin_sandbox_blocks_host_files_and_bytecode() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("sandbox-data");
    let package_dir = unique_short_test_dir("sandbox-package");
    let host_dir = unique_short_test_dir("sandbox-host");
    write_sandbox_probe_package(&package_dir);
    fs::create_dir_all(&host_dir).expect("create host file directory");
    let host_file = host_dir.join("host.lua");
    fs::write(&host_file, "return 'host file ran'").expect("write host file");
    let bytecode = mlua::Lua::new()
        .load("return 'bytecode ran'")
        .into_function()
        .expect("compile bytecode fixture")
        .dump(false);
    let bytecode_hex: String = bytecode.iter().map(|byte| format!("{byte:02x}")).collect();

    let daemon = PanicSafeCliDaemon::start(&data_dir, "plugin sandbox daemon cleanup");
    let enabled = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::EnablePackageLocalPath { path: package_dir.clone() },
    )
    .expect("enable sandbox probe package");
    assert_eq!(enabled.kind, botster_hub::DaemonResponseKind::PackageDecision, "{enabled:?}");
    let probe = call_plugin_tool(
        &data_dir,
        "sandbox.probe",
        serde_json::json!({
            "host_file": host_file.to_str().expect("UTF-8 host file path"),
            "bytecode_hex": bytecode_hex,
        }),
    );

    for escape in ["dofile", "loadfile"] {
        assert_eq!(probe[escape]["ok"], false, "{escape} must fail: {probe}");
        assert!(
            !probe[escape]["value"].as_str().unwrap_or_default().contains("host file ran"),
            "{escape} must not run the host file: {probe}"
        );
    }
    assert_eq!(probe["binary_load"]["loaded"], false, "{probe}");
    assert!(
        probe["binary_load"]["error"]
            .as_str()
            .unwrap_or_default()
            .contains("attempt to load a binary chunk"),
        "a bytecode chunk must be refused in text mode: {probe}"
    );
    assert_eq!(probe["string_dump"], false, "{probe}");
    assert_eq!(probe["print"], false, "{probe}");
    assert_eq!(probe["collect"], false, "{probe}");
    assert_eq!(probe["count"], "float", "{probe}");
    assert_eq!(probe["text_load"], 42, "{probe}");

    let absorbed = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::PluginMcpCallTool {
            name: "sandbox.absorb".to_string(),
            arguments: serde_json::json!({}),
        },
    )
    .expect("call budget absorption probe");
    assert_eq!(absorbed.kind, botster_hub::DaemonResponseKind::OperatorError, "{absorbed:?}");
    let message = &absorbed.error.as_ref().expect("budget failure has an operator error").message;
    assert!(message.contains("instruction budget"), "{message}");
    daemon.shutdown();
}
