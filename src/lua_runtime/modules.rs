//! `require` over the package's own Lua modules.
//!
//! At load the Hub stages every `.lua` file below `<package>/lua/` into the
//! new VM as text. `require("lib.store")` serves `lua/lib/store.lua` from that
//! in-memory set only, so the VM never touches the filesystem after load.
//! See docs/plans/plugin-platform.md section 7.4.

use std::fs;
use std::io::Read;
use std::path::Path;
use std::sync::{Arc, Mutex};

use mlua::{Function, Lua, Table};

use crate::lua_memory::{LuaCallbackCharge, LuaMemoryAccount};

/// The one global staging permit: package loads stage one at a time, and
/// their staged text never exceeds this many bytes (a user-approved number).
const STAGING_PERMIT_BYTES: usize = 16 * 1024 * 1024;
static STAGING_PERMIT: Mutex<()> = Mutex::new(());

/// The package directory that holds Lua modules.
const MODULE_ROOT: &str = "lua";

#[derive(Debug)]
pub(super) struct StagedModule {
    /// Module name, for example `lib.store`.
    pub(super) name: String,
    pub(super) source: String,
}

/// Staged module text plus the charge that funds it until the VM holds it.
pub(super) struct StagedModules {
    pub(super) modules: Vec<StagedModule>,
    _charge: Option<LuaCallbackCharge>,
}

/// Read every module below `<package_root>/lua/` under the staging permit.
///
/// The walk never follows a symlink, refuses non-UTF-8 names, and funds each
/// file's bytes before it reads them. A file that grows past its funded size
/// fails the load.
pub(super) fn stage(
    package_root: &Path,
    memory: &Arc<LuaMemoryAccount>,
) -> Result<StagedModules, String> {
    let root = package_root.join(MODULE_ROOT);
    let metadata = match fs::symlink_metadata(&root) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(StagedModules {
                modules: Vec::new(),
                _charge: None,
            });
        }
        Err(error) => return Err(format!("cannot read the lua module directory: {error}")),
    };
    if !metadata.is_dir() {
        return Err("the package's lua path must be a real directory".to_string());
    }
    let _permit = STAGING_PERMIT
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut charge = memory
        .reserve_callback_total(0)
        .map_err(|_| "the Lua callback memory capacity is exhausted".to_string())?;
    let mut modules = Vec::new();
    let mut staged_bytes = 0_usize;
    let per_file_limit = memory.limits().per_callback_bytes;
    let mut pending = vec![(root, String::new())];
    while let Some((directory, prefix)) = pending.pop() {
        let mut entries = fs::read_dir(&directory)
            .map_err(|error| format!("cannot list {}: {error}", directory.display()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| format!("cannot list {}: {error}", directory.display()))?;
        entries.sort_by_key(fs::DirEntry::file_name);
        for entry in entries {
            let file_name = entry.file_name();
            let Some(name) = file_name.to_str() else {
                return Err("module file names must be UTF-8".to_string());
            };
            let kind = entry
                .file_type()
                .map_err(|error| format!("cannot inspect {name}: {error}"))?;
            if kind.is_symlink() {
                return Err(format!(
                    "symlinks are not allowed below lua/: {prefix}{name}"
                ));
            }
            if kind.is_dir() {
                if name.contains('.') {
                    return Err(format!(
                        "module directory names cannot contain '.': {prefix}{name}"
                    ));
                }
                pending.push((entry.path(), format!("{prefix}{name}.")));
                continue;
            }
            let Some(stem) = name.strip_suffix(".lua") else {
                continue;
            };
            if stem.is_empty() || stem.contains('.') {
                return Err(format!(
                    "module file names need one '.lua' suffix: {prefix}{name}"
                ));
            }
            let size = entry
                .metadata()
                .map_err(|error| format!("cannot inspect {name}: {error}"))?
                .len();
            let size = usize::try_from(size).unwrap_or(usize::MAX);
            if size > per_file_limit {
                return Err(format!(
                    "module {prefix}{stem} exceeds {per_file_limit} bytes"
                ));
            }
            staged_bytes = staged_bytes
                .checked_add(size)
                .filter(|total| *total <= STAGING_PERMIT_BYTES)
                .ok_or_else(|| {
                    format!("the package's modules exceed the {STAGING_PERMIT_BYTES} byte staging permit")
                })?;
            charge
                .grow(size)
                .map_err(|_| "the Lua callback memory capacity is exhausted".to_string())?;
            let source = read_exact_size(&entry.path(), size)
                .map_err(|error| format!("cannot read module {prefix}{stem}: {error}"))?;
            modules.push(StagedModule {
                name: format!("{prefix}{stem}"),
                source,
            });
        }
    }
    Ok(StagedModules {
        modules,
        _charge: Some(charge),
    })
}

/// Read at most `size` bytes into a buffer allocated once at that size; a file
/// that grew after it was measured fails instead of allocating more.
fn read_exact_size(path: &Path, size: usize) -> Result<String, String> {
    let mut file = fs::File::open(path).map_err(|error| error.to_string())?;
    let mut bytes = vec![0_u8; size];
    let mut filled = 0;
    while filled < size {
        let read = file
            .read(&mut bytes[filled..])
            .map_err(|error| error.to_string())?;
        if read == 0 {
            break;
        }
        filled += read;
    }
    let mut extra = [0_u8; 1];
    if file.read(&mut extra).map_err(|error| error.to_string())? != 0 {
        return Err("the file changed while it was staged".to_string());
    }
    bytes.truncate(filled);
    String::from_utf8(bytes).map_err(|_| "module source is not UTF-8".to_string())
}

/// Install `require` over the staged modules. The VM then owns the text, so
/// the caller drops the staging charge afterwards.
pub(super) fn install(lua: &Lua, staged: &StagedModules) -> mlua::Result<()> {
    let sources = lua.create_table()?;
    for module in &staged.modules {
        sources.raw_set(module.name.as_str(), module.source.as_str())?;
    }
    let require: Function = lua
        .load(
            r#"
            local sources, load, error, type = ...
            local loaded, loading = {}, {}
            return function(name)
                if type(name) ~= "string" or name == "" then
                    error("require takes a module name such as 'lib.store'", 2)
                end
                local cached = loaded[name]
                if cached ~= nil then return cached end
                if loading[name] then
                    error("circular require of module '" .. name .. "'", 2)
                end
                local source = sources[name]
                if source == nil then
                    error("module '" .. name .. "' is not in the package's lua/ directory", 2)
                end
                local chunk, message = load(source, "@lua/" .. name:gsub("%.", "/") .. ".lua", "t")
                if chunk == nil then error(message, 2) end
                loading[name] = true
                local ok, value = pcall(chunk, name)
                loading[name] = nil
                if not ok then error(value, 0) end
                if value == nil then value = true end
                loaded[name] = value
                return value
            end
            "#,
        )
        .set_name("@hub/require")
        .call((
            sources,
            lua.globals().raw_get::<Function>("load")?,
            lua.globals().raw_get::<Function>("error")?,
            lua.globals().raw_get::<Function>("type")?,
        ))?;
    lua.globals().raw_set("require", require)
}

/// `Component` check used by tests to keep module paths inside `lua/`.
#[cfg(test)]
fn is_plain_relative(path: &Path) -> bool {
    use std::path::Component;
    path.components()
        .all(|part| matches!(part, Component::Normal(_)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lua_memory::LuaMemoryLimits;
    use mlua::{LuaOptions, StdLib};

    struct Package(std::path::PathBuf);

    impl Package {
        fn new(files: &[(&str, &str)]) -> Self {
            let mut random = [0_u8; 16];
            getrandom::fill(&mut random).unwrap();
            let root = std::env::temp_dir().join(format!(
                "botster-modules-{:032x}",
                u128::from_le_bytes(random)
            ));
            for (path, source) in files {
                assert!(is_plain_relative(Path::new(path)));
                let file = root.join(path);
                fs::create_dir_all(file.parent().unwrap()).unwrap();
                fs::write(file, source).unwrap();
            }
            fs::create_dir_all(&root).unwrap();
            Self(root)
        }
    }

    impl Drop for Package {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn memory(callback: usize) -> Arc<LuaMemoryAccount> {
        LuaMemoryAccount::new(LuaMemoryLimits {
            per_vm_bytes: 1024 * 1024,
            total_vm_bytes: 1024 * 1024,
            per_callback_bytes: callback,
            total_callback_bytes: callback,
        })
        .unwrap()
    }

    fn vm_with(package: &Package, memory: &Arc<LuaMemoryAccount>) -> Result<Lua, String> {
        let lua = Lua::new_with(
            StdLib::TABLE | StdLib::STRING | StdLib::MATH | StdLib::UTF8,
            LuaOptions::default(),
        )
        .unwrap();
        super::super::sandbox::install(
            &lua,
            Arc::new(std::sync::atomic::AtomicU64::new(u64::MAX)),
            Arc::new(super::super::InstructionBudgetExceeded),
        )
        .unwrap();
        let staged = stage(&package.0, memory)?;
        install(&lua, &staged).unwrap();
        drop(staged);
        Ok(lua)
    }

    #[test]
    fn require_serves_staged_modules_once_and_releases_staging() {
        let package = Package::new(&[
            (
                "lua/lib/store.lua",
                "counter = (counter or 0) + 1\nreturn { name = 'store' }",
            ),
            (
                "lua/lib/util.lua",
                "local store = require('lib.store') return { store = store }",
            ),
            ("lua/top.lua", "return nil"),
        ]);
        let memory = memory(64 * 1024);
        let lua = vm_with(&package, &memory).unwrap();
        assert_eq!(
            memory.usage().1,
            0,
            "staging is released once the VM holds the text"
        );
        lua.load(
            r#"
            local util = require("lib.util")
            assert(util.store.name == "store")
            assert(require("lib.store") == util.store)
            assert(counter == 1, "a module runs once")
            assert(require("top") == true)
            local ok, message = pcall(require, "lib.missing")
            assert(not ok and message:find("not in the package's lua/ directory", 1, true))
            local ok2 = pcall(require, "")
            assert(not ok2)
            "#,
        )
        .exec()
        .unwrap();
    }

    #[test]
    fn require_reports_circular_modules() {
        let package = Package::new(&[
            ("lua/a.lua", "return require('b')"),
            ("lua/b.lua", "return require('a')"),
        ]);
        let lua = vm_with(&package, &memory(64 * 1024)).unwrap();
        let error = lua.load("require('a')").exec().unwrap_err();
        assert!(error.to_string().contains("circular require"), "{error}");
    }

    #[test]
    fn staging_refuses_symlinks_and_unfunded_modules() {
        let package = Package::new(&[("lua/real.lua", "return 1")]);
        std::os::unix::fs::symlink(
            package.0.join("lua/real.lua"),
            package.0.join("lua/link.lua"),
        )
        .unwrap();
        let error = vm_with(&package, &memory(64 * 1024))
            .err()
            .expect("symlink refused");
        assert!(error.contains("symlinks are not allowed"), "{error}");

        let large = Package::new(&[("lua/big.lua", &"x".repeat(4096))]);
        let error = vm_with(&large, &memory(1024))
            .err()
            .expect("oversized module refused");
        assert!(error.contains("exceeds"), "{error}");
    }

    #[test]
    fn staging_charges_each_module_against_the_callback_account() {
        // Each file fits the per-file ceiling; together they exceed the account.
        let body = format!("return '{}'", "y".repeat(40 * 1024));
        let package = Package::new(&[("lua/one.lua", &body), ("lua/two.lua", &body)]);
        let memory = LuaMemoryAccount::new(LuaMemoryLimits {
            per_vm_bytes: 1024 * 1024,
            total_vm_bytes: 1024 * 1024,
            per_callback_bytes: 64 * 1024,
            total_callback_bytes: 64 * 1024,
        })
        .unwrap();
        let error = vm_with(&package, &memory)
            .err()
            .expect("unfunded staging refused");
        assert!(error.contains("capacity is exhausted"), "{error}");
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn packages_without_a_lua_directory_stage_nothing() {
        let package = Package::new(&[]);
        let lua = vm_with(&package, &memory(1024)).unwrap();
        let ok: bool = lua
            .load("return pcall(require, 'x') == false")
            .eval()
            .unwrap();
        assert!(ok);
    }
}
