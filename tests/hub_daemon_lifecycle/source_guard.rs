//! Checked source selection for the terminal Lua boundary guard.

/// Replace comments and literals with spaces while keeping byte offsets.
fn code_mask(source: &str) -> Result<Vec<u8>, String> {
    let bytes = source.as_bytes();
    let mut mask = bytes.to_vec();
    let mut index = 0;
    while index < bytes.len() {
        let start = index;
        if bytes[index..].starts_with(b"//") {
            index += 2;
            while index < bytes.len() && bytes[index] != b'\n' {
                index += 1;
            }
        } else if bytes[index..].starts_with(b"/*") {
            index += 2;
            let mut depth = 1usize;
            while index < bytes.len() && depth != 0 {
                if bytes[index..].starts_with(b"/*") {
                    depth += 1;
                    index += 2;
                } else if bytes[index..].starts_with(b"*/") {
                    depth -= 1;
                    index += 2;
                } else {
                    index += 1;
                }
            }
            if depth != 0 {
                return Err("unterminated block comment".into());
            }
        } else if let Some((opening, hashes)) = raw_string_open(bytes, index) {
            index += opening;
            loop {
                if index >= bytes.len() {
                    return Err("unterminated raw string".into());
                }
                if bytes[index] == b'"'
                    && bytes.get(index + 1..index + 1 + hashes) == Some(&vec![b'#'; hashes][..])
                {
                    index += 1 + hashes;
                    break;
                }
                index += 1;
            }
        } else if bytes[index] == b'"' {
            index += 1;
            loop {
                if index >= bytes.len() {
                    return Err("unterminated string".into());
                }
                if bytes[index] == b'\\' {
                    index = (index + 2).min(bytes.len());
                } else if bytes[index] == b'"' {
                    index += 1;
                    break;
                } else {
                    index += 1;
                }
            }
        } else if bytes[index] == b'\'' && char_literal_end(source, index).is_some() {
            index = char_literal_end(source, index).expect("checked char literal");
        } else {
            index += 1;
            continue;
        }
        for byte in &mut mask[start..index] {
            if *byte != b'\n' {
                *byte = b' ';
            }
        }
    }
    Ok(mask)
}

fn raw_string_open(bytes: &[u8], start: usize) -> Option<(usize, usize)> {
    let raw = if bytes[start..].starts_with(b"br") || bytes[start..].starts_with(b"cr") {
        start + 2
    } else if bytes[start] == b'r' {
        start + 1
    } else {
        return None;
    };
    let mut end = raw;
    while bytes.get(end) == Some(&b'#') {
        end += 1;
    }
    (bytes.get(end) == Some(&b'"')).then_some((end + 1 - start, end - raw))
}

fn char_literal_end(source: &str, start: usize) -> Option<usize> {
    let rest = source.get(start + 1..)?;
    if rest.starts_with('\\') {
        let mut escaped = false;
        for (offset, ch) in rest.char_indices() {
            if ch == '\n' {
                return None;
            }
            if ch == '\'' && !escaped {
                return Some(start + 1 + offset + 1);
            }
            escaped = ch == '\\' && !escaped;
        }
        return None;
    }
    let character = rest.chars().next()?;
    let end = start + 1 + character.len_utf8();
    (source.as_bytes().get(end) == Some(&b'\'')).then_some(end + 1)
}

/// Remove only reviewed cfg(test) items. A missing or malformed item is an error.
pub(crate) fn without_test_items(source: &str, items: &[&str]) -> Result<String, String> {
    let mask = code_mask(source)?;
    let mut ranges = Vec::new();
    for item in items {
        let matches = mask
            .windows(item.len())
            .enumerate()
            .filter_map(|(at, window)| (window == item.as_bytes()).then_some(at))
            .collect::<Vec<_>>();
        if matches.len() != 1 {
            return Err(format!(
                "expected one cfg(test) item `{item}`, found {}",
                matches.len()
            ));
        }
        let start = matches[0];
        let open = if item.ends_with('{') {
            start + item.len() - 1
        } else {
            let tail = &mask[start + item.len()..];
            let brace = tail.iter().position(|byte| *byte == b'{');
            let semicolon = tail.iter().position(|byte| *byte == b';');
            match (brace, semicolon) {
                (Some(brace), Some(semicolon)) if semicolon < brace => {
                    return Err(format!("cfg(test) item `{item}` has unsupported syntax"));
                }
                (Some(brace), _) => start + item.len() + brace,
                _ => return Err(format!("cfg(test) item `{item}` has no body")),
            }
        };
        let mut depth = 0usize;
        let mut end = None;
        for (at, byte) in mask.iter().enumerate().skip(open) {
            match byte {
                b'{' => depth += 1,
                b'}' => {
                    depth -= 1;
                    if depth == 0 {
                        end = Some(at + 1);
                        break;
                    }
                }
                _ => {}
            }
        }
        ranges.push((
            start,
            end.ok_or_else(|| format!("cfg(test) item `{item}` is unterminated"))?,
        ));
    }
    ranges.sort_unstable();
    if ranges.windows(2).any(|pair| pair[0].1 > pair[1].0) {
        return Err("cfg(test) item ranges overlap".into());
    }
    let mut out = String::new();
    let mut cursor = 0;
    for (start, end) in ranges {
        out.push_str(&source[cursor..start]);
        cursor = end;
    }
    out.push_str(&source[cursor..]);
    Ok(out)
}

fn test_items(path: &str) -> &'static [&'static str] {
    const MODULE: &str = "#[cfg(test)]\nmod tests {";
    match path {
        "src/daemon/owner_loop.rs"
        | "src/daemon_maintenance.rs"
        | "src/plugin_entity.rs"
        | "src/package_entity_fanout.rs" => &[MODULE],
        "src/runtime/entity_model.rs" => &[
            MODULE,
            "#[cfg(test)]\n    pub(crate) fn test_pending_publication(",
        ],
        _ => &[],
    }
}

fn runtime_owner(path: &str) -> bool {
    matches!(
        path,
        "src/lib.rs" | "src/runtime.rs" | "src/runtime/package_effect.rs" | "src/lua_runtime.rs"
    ) || path.starts_with("src/lua_runtime/")
}

fn feature_only(path: &str, root: &std::path::Path) -> Result<bool, String> {
    let (owner, gate) = match path {
        "src/test_internals.rs" => (
            "src/lib.rs",
            "#[cfg(feature = \"test-internals\")]\npub mod test_internals;",
        ),
        "src/data_plane/driver/allocation_oracle.rs" => (
            "src/data_plane/driver.rs",
            "#[cfg(feature = \"allocation-oracle\")]\npub(crate) mod allocation_oracle;",
        ),
        _ => return Ok(false),
    };
    let declaration = std::fs::read_to_string(root.join(owner))
        .map_err(|error| format!("read {owner}: {error}"))?;
    if !feature_gate_in_code(&declaration, gate)? {
        return Err(format!("{path} lacks its feature gate in {owner}"));
    }
    Ok(true)
}

fn feature_gate_in_code(declaration: &str, gate: &str) -> Result<bool, String> {
    let declaration_mask = code_mask(&declaration)?;
    let gate_mask = code_mask(gate)?;
    Ok(declaration
        .match_indices(gate)
        .any(|(at, _)| declaration_mask[at..at + gate.len()] == gate_mask))
}

fn publication_permit_path(path: &str) -> bool {
    matches!(
        path,
        "src/daemon/control/entities.rs"
            | "src/package_entity_fanout.rs"
            | "src/package_event_router.rs"
            | "src/plugin_entity.rs"
            | "src/runtime/entities.rs"
            | "src/runtime/entity_model.rs"
            | "src/runtime/family_cleanup.rs"
            | "src/runtime/resync.rs"
            | "src/subscription/entity_resync.rs"
    )
}

fn carrier_line(path: &str, line: &str) -> bool {
    const PERMIT: &str = "crate::lua_runtime::EntityPublishPermit";
    if publication_permit_path(path) && line.contains(PERMIT) {
        return matches!(
            line,
            "Option<crate::lua_runtime::EntityPublishPermit>,"
                | "admission: Option<crate::lua_runtime::EntityPublishPermit>,"
                | "pub admission: Option<crate::lua_runtime::EntityPublishPermit>,"
                | "pub(crate) obligation: Option<(u64, Option<crate::lua_runtime::EntityPublishPermit>)>,"
                | "pub(crate) resync_lease: Option<(u64, Option<crate::lua_runtime::EntityPublishPermit>)>,"
                | "pub leases: BTreeMap<u64, Option<crate::lua_runtime::EntityPublishPermit>>,"
                | "permit: Option<crate::lua_runtime::EntityPublishPermit>,"
                | "identities: BTreeMap<LeaseIdentity, Option<crate::lua_runtime::EntityPublishPermit>>,"
                | "Resync((u64, Option<crate::lua_runtime::EntityPublishPermit>)),"
                | "_resync: Option<(u64, Option<crate::lua_runtime::EntityPublishPermit>)>,"
                | "retained: &mut Option<(u64, Option<crate::lua_runtime::EntityPublishPermit>)>,"
                | "resync_lease: Option<(u64, Option<crate::lua_runtime::EntityPublishPermit>)>,"
                | "release: Option<(u64, Option<crate::lua_runtime::EntityPublishPermit>)>,"
                | "terminal_admission: Option<crate::lua_runtime::EntityPublishPermit>,"
                | "pub(crate) fn admission(&self) -> Option<&crate::lua_runtime::EntityPublishPermit> {"
                | "pub(crate) fn terminal_admission(&self) -> Option<crate::lua_runtime::EntityPublishPermit> {"
                | ") -> Option<(u64, Option<crate::lua_runtime::EntityPublishPermit>)> {"
                | "admission: Option<&crate::lua_runtime::EntityPublishPermit>,"
        );
    }
    match path {
        "src/host_executor.rs" => matches!(
            line,
            "response: crate::lua_runtime::CoordinationReplySender,"
                | "result: crate::lua_runtime::CoordinationDelivery,"
        ),
        "src/runtime/publication.rs" => {
            line == "use crate::lua_runtime::PendingEntityPublishRequest;"
        }
        "src/daemon/control/coordination.rs" => matches!(
            line,
            "crate::lua_runtime::CoordinationIngressPoll::Ready(pending) => pending,"
                | "if matches!(poll, crate::lua_runtime::CoordinationIngressPoll::Poisoned) {"
        ),
        _ => false,
    }
}

/// Keep the recursive import inventory while allowing only reviewed Rust owners.
pub(crate) fn check_lua_boundary(
    root: &std::path::Path,
    path: &str,
    source: &str,
) -> Result<(), String> {
    let mut production = without_test_items(source, test_items(path))?;
    if runtime_owner(path) || feature_only(path, root)? {
        return Ok(());
    }
    if path == "src/daemon/control/coordination.rs" {
        const IMPORT: &str = "use crate::lua_runtime::{\n    CoordinationDelivery, CoordinationRefusal, CoordinationReply, CoordinationReplySender,\n};";
        if production.matches(IMPORT).count() != 1 {
            return Err("coordination bridge import changed".into());
        }
        production = production.replacen(IMPORT, "", 1);
    }
    for line in production.lines() {
        if line.contains("lua_runtime") && !carrier_line(path, line.trim()) {
            return Err(format!(
                "{path} has an unreviewed Lua reference: {}",
                line.trim()
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{check_lua_boundary, feature_gate_in_code, without_test_items};
    use std::path::Path;

    const ITEM: &str = "#[cfg(test)]\nmod tests {";

    #[test]
    fn keeps_production_after_test_module_with_literal_braces() {
        let source = "#[cfg(test)]\nmod tests {\n let _ = r#\"{ }\"#; // {\n let _ = \"{\"; /* } */\n let _ = '{';\n}\nfn production() { lua_runtime::forbidden(); }\n";
        let production = without_test_items(source, &[ITEM]).unwrap();
        assert!(production.contains("lua_runtime::forbidden"));
        assert!(!production.contains("let _"));
    }

    #[test]
    fn rejects_unterminated_test_item_and_literal() {
        assert!(without_test_items("#[cfg(test)]\nmod tests {", &[ITEM]).is_err());
        assert!(without_test_items("#[cfg(test)]\nmod tests { let _ = r#\"open", &[ITEM]).is_err());
        assert!(without_test_items("#[cfg(test)]\nmod tests { /* open", &[ITEM]).is_err());
        assert!(without_test_items("#[cfg(test)]\nmod tests;", &[ITEM]).is_err());
    }

    #[test]
    fn rejects_forbidden_production_after_test_module() {
        let source = "#[cfg(test)]\nmod tests { let _ = \"{\"; }\nfn production() { crate::lua_runtime::LuaPluginRuntime::load(); }\n";
        assert!(check_lua_boundary(Path::new("."), "src/daemon/owner_loop.rs", source).is_err());
    }

    #[test]
    fn rejects_forbidden_reference_beside_carrier_and_in_transport() {
        let carrier = "Option<crate::lua_runtime::EntityPublishPermit>, crate::lua_runtime::LuaPluginRuntime::load();";
        assert!(
            check_lua_boundary(Path::new("."), "src/daemon/control/entities.rs", carrier).is_err()
        );
        let transport = "crate::lua_runtime::LuaPluginRuntime::load();";
        assert!(check_lua_boundary(Path::new("."), "src/transport/unix.rs", transport).is_err());
        let permit_call = "crate::lua_runtime::EntityPublishPermit::acquire();";
        assert!(
            check_lua_boundary(
                Path::new("."),
                "src/daemon/control/entities.rs",
                permit_call
            )
            .is_err()
        );
        let permit_constructor = "let permit = crate::lua_runtime::EntityPublishPermit {};";
        assert!(
            check_lua_boundary(
                Path::new("."),
                "src/daemon/control/entities.rs",
                permit_constructor
            )
            .is_err()
        );
    }

    #[test]
    fn accepts_exact_reviewed_carrier() {
        let source = "permit: Option<crate::lua_runtime::EntityPublishPermit>,";
        assert!(
            check_lua_boundary(Path::new("."), "src/daemon/control/entities.rs", source).is_ok()
        );
    }

    #[test]
    fn rejects_feature_gate_in_comment_or_string() {
        let gate = "#[cfg(feature = \"test-internals\")]\npub mod test_internals;";
        let commented = format!("/* {gate} */\npub mod test_internals;");
        assert!(!feature_gate_in_code(&commented, gate).unwrap());
        let quoted = format!("const NOTE: &str = r#\"{gate}\"#;\npub mod test_internals;");
        assert!(!feature_gate_in_code(&quoted, gate).unwrap());
        assert!(feature_gate_in_code(gate, gate).unwrap());
    }
}
