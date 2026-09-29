//! A static check for global names that a plugin uses but the sandbox does not
//! define.
//!
//! A plugin runtime spec only reaches the paths that a test drives. A call to
//! an undefined global in an error path that no test reaches (`log.warn(...)`
//! in a plugin whose sandbox has no `log`) passes every runtime spec and
//! fails in production. This check parses the plugin's Lua and reports every
//! name that is read or written as a global, is not bound by a local,
//! parameter, or loop variable in scope, and is not in the sandbox's real set
//! of global names (`KitHub::sandbox_globals`, read from the running runtime).
//!
//! Limits, kept on purpose: scopes follow blocks, functions, and `for` loops.
//! A local declared in a `repeat` block is not visible in its `until`
//! condition, so it can be reported. Dynamic access (`_G[name]`, `load`) is
//! not seen.

use std::collections::{BTreeSet, HashSet};
use std::path::{Path, PathBuf};

use full_moon::ast::{
    Assignment, Block, FunctionBody, FunctionDeclaration, GenericFor, LocalAssignment,
    LocalFunction, NumericFor, Parameter, Prefix, Var,
};
use full_moon::visitors::Visitor;

/// One use of a global name that the sandbox does not define.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalUse {
    pub name: String,
    pub line: usize,
    pub column: usize,
    /// `true` for an assignment or a `function name()` declaration.
    pub write: bool,
}

/// A use, with the file that holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileGlobalUse {
    pub file: PathBuf,
    pub usage: GlobalUse,
}

/// Every undefined global use in `source`.
///
/// # Errors
/// Returns the parser's message when `source` is not valid Lua 5.4.
pub fn undefined_globals(
    source: &str,
    defined: &BTreeSet<String>,
) -> Result<Vec<GlobalUse>, String> {
    let ast = full_moon::parse(source).map_err(|errors| {
        errors
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("; ")
    })?;
    let mut scan = Scan {
        defined,
        scopes: Vec::new(),
        writes: HashSet::new(),
        found: Vec::new(),
    };
    scan.visit_ast(&ast);
    Ok(scan.found)
}

/// Every undefined global use in the Lua files of a plugin directory: all
/// `.lua` files below it except those under `test`, `tests`, `spec`, `specs`,
/// and directories that start with a dot.
///
/// # Errors
/// Returns a message naming the file when a file cannot be read or parsed.
pub fn undefined_globals_in_plugin(
    directory: &Path,
    defined: &BTreeSet<String>,
) -> Result<Vec<FileGlobalUse>, String> {
    let mut files = Vec::new();
    collect_lua_files(directory, &mut files)?;
    files.sort();
    let mut found = Vec::new();
    for file in files {
        let source = std::fs::read_to_string(&file)
            .map_err(|error| format!("{}: {error}", file.display()))?;
        let relative = file.strip_prefix(directory).unwrap_or(&file).to_path_buf();
        for usage in undefined_globals(&source, defined)
            .map_err(|error| format!("{}: {error}", relative.display()))?
        {
            found.push(FileGlobalUse {
                file: relative.clone(),
                usage,
            });
        }
    }
    Ok(found)
}

fn collect_lua_files(directory: &Path, files: &mut Vec<PathBuf>) -> Result<(), String> {
    let entries = std::fs::read_dir(directory)
        .map_err(|error| format!("{}: {error}", directory.display()))?;
    for entry in entries {
        let path = entry
            .map_err(|error| format!("{}: {error}", directory.display()))?
            .path();
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default()
            .to_string();
        if path.is_dir() {
            if name.starts_with('.') || matches!(name.as_str(), "test" | "tests" | "spec" | "specs")
            {
                continue;
            }
            collect_lua_files(&path, files)?;
        } else if path.extension().and_then(|extension| extension.to_str()) == Some("lua") {
            files.push(path);
        }
    }
    Ok(())
}

struct Scan<'a> {
    defined: &'a BTreeSet<String>,
    scopes: Vec<HashSet<String>>,
    /// Positions of names that an assignment writes, so `visit_var` does not
    /// also report them as reads.
    writes: HashSet<(usize, usize)>,
    found: Vec<GlobalUse>,
}

impl Scan<'_> {
    fn is_bound(&self, name: &str) -> bool {
        self.scopes.iter().any(|scope| scope.contains(name)) || self.defined.contains(name)
    }

    fn declare(&mut self, name: String) {
        if let Some(scope) = self.scopes.last_mut() {
            scope.insert(name);
        }
    }

    fn check(&mut self, token: &full_moon::tokenizer::TokenReference, write: bool) {
        let name = token.token().to_string();
        if self.is_bound(&name) {
            return;
        }
        let position = token.start_position();
        self.found.push(GlobalUse {
            name,
            line: position.line(),
            column: position.character(),
            write,
        });
    }
}

impl Visitor for Scan<'_> {
    fn visit_block(&mut self, _block: &Block) {
        self.scopes.push(HashSet::new());
    }

    fn visit_block_end(&mut self, _block: &Block) {
        self.scopes.pop();
    }

    fn visit_function_body(&mut self, body: &FunctionBody) {
        let mut scope = HashSet::new();
        for parameter in body.parameters() {
            if let Parameter::Name(name) = parameter {
                scope.insert(name.token().to_string());
            }
        }
        self.scopes.push(scope);
    }

    fn visit_function_body_end(&mut self, _body: &FunctionBody) {
        self.scopes.pop();
    }

    // A local is in scope for the statements after its declaration, not for
    // its own right-hand side (`local x = x` reads the outer `x`).
    fn visit_local_assignment_end(&mut self, assignment: &LocalAssignment) {
        for name in assignment.names() {
            self.declare(name.token().to_string());
        }
    }

    // A local function is in scope inside its own body (recursion).
    fn visit_local_function(&mut self, function: &LocalFunction) {
        self.declare(function.name().token().to_string());
    }

    fn visit_numeric_for(&mut self, numeric: &NumericFor) {
        self.scopes.push(HashSet::from([numeric
            .index_variable()
            .token()
            .to_string()]));
    }

    fn visit_numeric_for_end(&mut self, _numeric: &NumericFor) {
        self.scopes.pop();
    }

    fn visit_generic_for(&mut self, generic: &GenericFor) {
        self.scopes.push(
            generic
                .names()
                .iter()
                .map(|name| name.token().to_string())
                .collect(),
        );
    }

    fn visit_generic_for_end(&mut self, _generic: &GenericFor) {
        self.scopes.pop();
    }

    fn visit_assignment(&mut self, assignment: &Assignment) {
        for variable in assignment.variables() {
            if let Var::Name(token) = variable {
                let position = token.start_position();
                self.writes.insert((position.line(), position.character()));
                self.check(token, true);
            }
        }
    }

    // `function name()` defines a global; `function base.field()` and
    // `function base:method()` read the global `base`.
    fn visit_function_declaration(&mut self, declaration: &FunctionDeclaration) {
        let name = declaration.name();
        let Some(first) = name.names().iter().next() else {
            return;
        };
        let single = name.names().len() == 1 && name.method_name().is_none();
        self.check(first, single);
    }

    fn visit_var(&mut self, variable: &Var) {
        if let Var::Name(token) = variable {
            let position = token.start_position();
            if !self
                .writes
                .contains(&(position.line(), position.character()))
            {
                self.check(token, false);
            }
        }
    }

    fn visit_prefix(&mut self, prefix: &Prefix) {
        if let Prefix::Name(token) = prefix {
            self.check(token, false);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn defined() -> BTreeSet<String> {
        [
            "botster", "string", "table", "pairs", "ipairs", "pcall", "type", "tostring",
        ]
        .into_iter()
        .map(str::to_string)
        .collect()
    }

    fn names(source: &str) -> Vec<(String, bool)> {
        undefined_globals(source, &defined())
            .expect("valid Lua")
            .into_iter()
            .map(|usage| (usage.name, usage.write))
            .collect()
    }

    #[test]
    fn an_undefined_global_call_in_an_unreached_branch_is_reported() {
        let source = "local function prune(id)\n  if false then\n    log.warn({ message = id })\n  end\nend\nreturn botster.register({})\n";
        assert_eq!(names(source), [("log".to_string(), false)]);
        let usage = &undefined_globals(source, &defined()).expect("valid Lua")[0];
        assert_eq!((usage.line, usage.column), (3, 5));
    }

    #[test]
    fn a_global_in_the_sandbox_set_is_not_reported() {
        assert_eq!(
            names("botster.log.info({}); return string.format('x') .. tostring(type(1))"),
            []
        );
    }

    #[test]
    fn locals_parameters_and_loop_variables_are_bound() {
        let source = "local a = 1\nlocal function f(b, ...)\n  for i = 1, 3 do local c = a + b + i end\n  for k, v in pairs({}) do local d = k .. v end\n  return f\nend\nreturn a\n";
        assert_eq!(names(source), []);
    }

    #[test]
    fn a_local_is_not_visible_in_its_own_right_hand_side_or_after_its_block() {
        assert_eq!(
            names("local x = x\ndo local inner = 1 end\nreturn inner"),
            [("x".to_string(), false), ("inner".to_string(), false)]
        );
    }

    #[test]
    fn assignments_and_function_declarations_to_globals_are_writes() {
        assert_eq!(
            names(
                "counter = 1\nfunction helper() end\nlocal t = {}\nfunction t.method() end\nfunction other:call() end"
            ),
            [
                ("counter".to_string(), true),
                ("helper".to_string(), true),
                ("other".to_string(), false)
            ]
        );
    }

    #[test]
    fn field_and_method_names_are_not_globals() {
        assert_eq!(
            names("local s = 'x'\nreturn s:upper() .. botster.clock.now().value"),
            []
        );
    }

    #[test]
    fn a_parse_error_is_reported_not_swallowed() {
        assert!(undefined_globals("local = 1", &defined()).is_err());
    }
}
