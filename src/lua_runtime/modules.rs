//! `require` over the package's own Lua modules.
//!
//! At load the Hub stages every `.lua` file below `<package>/lua/` into the
//! new VM as text. `require("lib.store")` serves `lua/lib/store.lua` from that
//! in-memory set only, so the VM never touches the filesystem after load.
//! See docs/plans/plugin-platform.md section 7.4.

use std::fs::File;
use std::io::Read;
use std::os::fd::OwnedFd;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use mlua::{Function, Lua};
use rustix::fs::{Dir, Mode, OFlags, openat};
use rustix::io::Errno;

use crate::lua_memory::{LuaCallbackCharge, LuaMemoryAccount};

/// The one global staging permit: package loads stage one at a time, and
/// their staged bytes never exceed this many (a user-approved number).
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

/// Staged module text plus the charge that funds it and the staging permit.
/// Both last until the VM holds the text and the caller drops this value.
pub(super) struct StagedModules {
    pub(super) modules: Vec<StagedModule>,
    _charge: Option<LuaCallbackCharge>,
    _permit: Option<StagingPermit>,
}

/// Holds the global staging permit for as long as the staged text exists.
struct StagingPermit {
    _guard: MutexGuard<'static, ()>,
}

impl StagingPermit {
    fn acquire() -> Self {
        let guard = STAGING_PERMIT
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        #[cfg(test)]
        PERMIT_HELD_HERE.with(|held| held.set(true));
        Self { _guard: guard }
    }
}

#[cfg(test)]
impl Drop for StagingPermit {
    fn drop(&mut self) {
        PERMIT_HELD_HERE.with(|held| held.set(false));
    }
}

#[cfg(test)]
thread_local! {
    /// Whether this thread holds the staging permit.
    static PERMIT_HELD_HERE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// A directory still to walk: its descriptor and its module name prefix.
struct PendingDirectory {
    descriptor: OwnedFd,
    prefix: String,
}

/// Funds staging: every retained byte is charged to the callback account
/// before it is allocated, and counted against the staging permit.
struct StagingFunds {
    charge: LuaCallbackCharge,
    staged_bytes: usize,
}

impl StagingFunds {
    fn admit(&mut self, bytes: usize) -> Result<(), String> {
        self.staged_bytes = self
            .staged_bytes
            .checked_add(bytes)
            .filter(|total| *total <= STAGING_PERMIT_BYTES)
            .ok_or_else(|| {
                format!(
                    "the package's modules exceed the {STAGING_PERMIT_BYTES} byte staging permit"
                )
            })?;
        self.charge
            .grow(bytes)
            .map_err(|_| "the Lua callback memory capacity is exhausted".to_string())
    }

    /// Push onto `items`, funding any growth of its allocation first.
    fn push<T>(&mut self, items: &mut Vec<T>, item: T) -> Result<(), String> {
        if items.len() == items.capacity() {
            let target = items.capacity().max(4).saturating_mul(2);
            let additional = target - items.len();
            self.admit(additional.saturating_mul(std::mem::size_of::<T>()))?;
            items.reserve_exact(additional);
        }
        items.push(item);
        Ok(())
    }
}

#[cfg(test)]
thread_local! {
    /// Test seam: runs after an entry is listed and before it is opened.
    static BEFORE_OPEN: std::cell::RefCell<Option<Box<dyn FnMut(&str)>>> =
        const { std::cell::RefCell::new(None) };
}

/// Read every module below `<package_root>/lua/` under the staging permit.
///
/// The walk is descriptor-relative: each entry is opened once with
/// `O_NOFOLLOW | O_NONBLOCK` below its parent's descriptor and classified by
/// `fstat` on that descriptor, so a symlink or a swapped entry is refused and
/// a FIFO or device never blocks. Only one directory stream is open at a
/// time. Every retained byte (names, directory records, module text, and the
/// vectors that hold them) is funded before it is allocated.
pub(super) fn stage(
    package_root: &Path,
    memory: &Arc<LuaMemoryAccount>,
) -> Result<StagedModules, String> {
    let package = File::open(package_root)
        .map_err(|error| format!("cannot open the package directory: {error}"))?;
    let root = match openat(
        &package,
        MODULE_ROOT,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    ) {
        Ok(root) => root,
        Err(Errno::NOENT) => {
            return Ok(StagedModules {
                modules: Vec::new(),
                _charge: None,
                _permit: None,
            });
        }
        Err(Errno::LOOP | Errno::NOTDIR) => {
            return Err("the package's lua path must be a real directory".to_string());
        }
        Err(error) => return Err(format!("cannot open the lua module directory: {error}")),
    };
    let permit = StagingPermit::acquire();
    let mut funds = StagingFunds {
        charge: memory
            .reserve_callback_total(0)
            .map_err(|_| "the Lua callback memory capacity is exhausted".to_string())?,
        staged_bytes: 0,
    };
    let per_file_limit = memory.limits().per_callback_bytes;
    let mut modules: Vec<StagedModule> = Vec::new();
    let mut pending: Vec<PendingDirectory> = Vec::new();
    funds.push(
        &mut pending,
        PendingDirectory {
            descriptor: root,
            prefix: String::new(),
        },
    )?;
    while let Some(PendingDirectory { descriptor, prefix }) = pending.pop() {
        // The stream reads a duplicate descriptor; entries open below the
        // original, so no path is resolved again.
        let directory = Dir::read_from(&descriptor)
            .map_err(|error| format!("cannot list a module directory: {error}"))?;
        for entry in directory {
            let entry =
                entry.map_err(|error| format!("cannot list a module directory: {error}"))?;
            let raw = entry.file_name().to_bytes();
            if raw == b"." || raw == b".." {
                continue;
            }
            let Ok(name) = std::str::from_utf8(raw) else {
                return Err("module file names must be UTF-8".to_string());
            };
            #[cfg(test)]
            BEFORE_OPEN.with(|hook| {
                if let Some(hook) = hook.borrow_mut().as_mut() {
                    hook(name);
                }
            });
            let opened = match openat(
                &descriptor,
                name,
                OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
                Mode::empty(),
            ) {
                Ok(opened) => File::from(opened),
                Err(Errno::LOOP) => {
                    return Err(format!(
                        "symlinks are not allowed below lua/: {prefix}{name}"
                    ));
                }
                Err(error) => return Err(format!("cannot open {prefix}{name}: {error}")),
            };
            let metadata = opened
                .metadata()
                .map_err(|error| format!("cannot inspect {prefix}{name}: {error}"))?;
            if metadata.is_dir() {
                if name.contains('.') {
                    return Err(format!(
                        "module directory names cannot contain '.': {prefix}{name}"
                    ));
                }
                let child_prefix_len = prefix.len() + name.len() + 1;
                funds.admit(child_prefix_len)?;
                funds.push(
                    &mut pending,
                    PendingDirectory {
                        descriptor: OwnedFd::from(opened),
                        prefix: format!("{prefix}{name}."),
                    },
                )?;
                continue;
            }
            let Some(stem) = name.strip_suffix(".lua") else {
                continue;
            };
            if !metadata.is_file() {
                return Err(format!("modules must be regular files: {prefix}{name}"));
            }
            if stem.is_empty() || stem.contains('.') {
                return Err(format!(
                    "module file names need one '.lua' suffix: {prefix}{name}"
                ));
            }
            let size = usize::try_from(metadata.len()).unwrap_or(usize::MAX);
            if size > per_file_limit {
                return Err(format!(
                    "module {prefix}{stem} exceeds {per_file_limit} bytes"
                ));
            }
            let module_name_len = prefix.len() + stem.len();
            funds.admit(size.saturating_add(module_name_len))?;
            let source = read_exact_size(opened, size)
                .map_err(|error| format!("cannot read module {prefix}{stem}: {error}"))?;
            funds.push(
                &mut modules,
                StagedModule {
                    name: format!("{prefix}{stem}"),
                    source,
                },
            )?;
        }
    }
    Ok(StagedModules {
        modules,
        _charge: Some(funds.charge),
        _permit: Some(permit),
    })
}
/// Read at most `size` bytes into a buffer allocated once at that size; a file
/// that grew after it was measured fails instead of allocating more.
fn read_exact_size(mut file: File, size: usize) -> Result<String, String> {
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
    use std::fs;

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
    fn many_empty_modules_are_funded_like_any_other() {
        // Empty modules carry no text, but their names and records are
        // retained; 2000 of them exceed a 64 KiB callback account.
        let names: Vec<String> = (0..2000).map(|index| format!("lua/m{index}.lua")).collect();
        let files: Vec<(&str, &str)> = names.iter().map(|name| (name.as_str(), "")).collect();
        let package = Package::new(&files);
        let memory = memory(64 * 1024);
        let error = vm_with(&package, &memory)
            .err()
            .expect("unfunded module metadata refused");
        assert!(error.contains("capacity is exhausted"), "{error}");
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn an_entry_swapped_for_a_symlink_after_listing_is_refused() {
        let package = Package::new(&[("lua/a.lua", "return 'real'")]);
        let target = package.0.join("outside.lua");
        fs::write(&target, "return 'outside'").unwrap();
        let module = package.0.join("lua/a.lua");
        BEFORE_OPEN.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move |name: &str| {
                if name == "a.lua" {
                    fs::remove_file(&module).unwrap();
                    std::os::unix::fs::symlink(&target, &module).unwrap();
                }
            }));
        });
        let result = vm_with(&package, &memory(64 * 1024));
        BEFORE_OPEN.with(|hook| hook.borrow_mut().take());
        let error = result.err().expect("the swapped symlink is refused");
        assert!(error.contains("symlinks are not allowed"), "{error}");
    }

    #[test]
    fn a_fifo_module_is_refused_without_blocking() {
        let package = Package::new(&[("lua/real.lua", "return 1")]);
        let fifo = std::ffi::CString::new(
            package
                .0
                .join("lua/pipe.lua")
                .into_os_string()
                .into_encoded_bytes(),
        )
        .unwrap();
        // SAFETY: `fifo` is a valid NUL-terminated path.
        assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
        let error = vm_with(&package, &memory(64 * 1024))
            .err()
            .expect("a FIFO module is refused");
        assert!(error.contains("must be regular files"), "{error}");
    }

    #[test]
    fn the_staging_permit_lasts_as_long_as_the_staged_text() {
        let package = Package::new(&[("lua/a.lua", "return 1")]);
        let memory = memory(64 * 1024);
        let staged = stage(&package.0, &memory).unwrap();
        assert!(
            PERMIT_HELD_HERE.with(std::cell::Cell::get),
            "the permit is held while the staged text exists"
        );
        drop(staged);
        assert!(!PERMIT_HELD_HERE.with(std::cell::Cell::get));
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
