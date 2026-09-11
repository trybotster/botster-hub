//! Host callbacks return errors as Lua strings before the Lua wrapper raises them.

use std::sync::Arc;

use mlua::{FromLua, Function, Lua, LuaSerdeExt, Table, Value};

/// Raw arguments cannot create a Rust conversion error before the callback body.
pub(super) trait Arguments: Sized {
    type Raw: mlua::FromLuaMulti;
    const COUNT: usize;

    fn convert(raw: Self::Raw, lua: &Lua) -> mlua::Result<Self>;
}

fn argument<T: FromLua>(value: Value, position: usize, lua: &Lua) -> mlua::Result<T> {
    T::from_lua(value, lua).map_err(|cause| mlua::Error::BadArgument {
        to: None,
        pos: position,
        name: None,
        cause: Arc::new(cause),
    })
}

macro_rules! scalar_arguments {
    ($($kind:ty),+ $(,)?) => {
        $(impl Arguments for $kind {
            type Raw = Value;
            const COUNT: usize = 1;

            fn convert(raw: Value, lua: &Lua) -> mlua::Result<Self> {
                argument(raw, 1, lua)
            }
        })+
    };
}

scalar_arguments!(Value, Table, u64, super::EventName);

impl Arguments for () {
    type Raw = ();
    const COUNT: usize = 0;

    fn convert((): (), _: &Lua) -> mlua::Result<Self> {
        Ok(())
    }
}

impl<A: FromLua, B: FromLua> Arguments for (A, B) {
    type Raw = (Value, Value);
    const COUNT: usize = 2;

    fn convert((first, second): Self::Raw, lua: &Lua) -> mlua::Result<Self> {
        Ok((argument(first, 1, lua)?, argument(second, 2, lua)?))
    }
}

impl<A: FromLua, B: FromLua, C: FromLua> Arguments for (A, B, C) {
    type Raw = (Value, Value, Value);
    const COUNT: usize = 3;

    fn convert((first, second, third): Self::Raw, lua: &Lua) -> mlua::Result<Self> {
        Ok((
            argument(first, 1, lua)?,
            argument(second, 2, lua)?,
            argument(third, 3, lua)?,
        ))
    }
}

pub(super) trait ReturnValue {
    const HAS_VALUE: bool;

    fn into_value(self) -> Value;
}

impl ReturnValue for Value {
    const HAS_VALUE: bool = true;

    fn into_value(self) -> Value {
        self
    }
}

impl ReturnValue for Table {
    const HAS_VALUE: bool = true;

    fn into_value(self) -> Value {
        Value::Table(self)
    }
}

impl ReturnValue for () {
    const HAS_VALUE: bool = false;

    fn into_value(self) -> Value {
        Value::Nil
    }
}

/// Keep argument and body errors out of mlua's retained Rust error userdata.
pub(super) fn create<A, R, F>(lua: &Lua, function: F) -> mlua::Result<Function>
where
    A: Arguments,
    R: ReturnValue,
    F: Fn(&Lua, A) -> mlua::Result<R> + Send + 'static,
{
    let fallback = lua.create_string("Lua callback could not allocate its error")?;
    let callback = lua.create_function(move |lua, raw: A::Raw| {
        let result = A::convert(raw, lua)
            .and_then(|args| function(lua, args))
            .and_then(|value| match value.into_value() {
                Value::Error(error) => Err(*error),
                value => Ok(value),
            });
        let reply = match result {
            Ok(value) => (true, value),
            Err(error) => {
                let message = match error {
                    mlua::Error::RuntimeError(message) => message,
                    error => error.to_string(),
                };
                let value = lua
                    .create_string(&message)
                    .unwrap_or_else(|_| fallback.clone());
                (false, Value::String(value))
            }
        };
        Ok(reply)
    })?;
    lua.load(
        r#"
        local callback, has_value, count, null = ...
        local raise, value_type, same = error, type, rawequal
        local function argument(value)
            if value_type(value) == 'userdata' and not same(value, null) then
                raise('unsupported Lua userdata', 0)
            end
            return value
        end
        return function(...)
            local first, second, third = ...
            if count >= 1 then first = argument(first) end
            if count >= 2 then second = argument(second) end
            if count >= 3 then third = argument(third) end
            local ok, value = callback(first, second, third)
            if not ok then raise(value, 0) end
            if has_value then return value end
        end
        "#,
    )
    .call((callback, R::HAS_VALUE, A::COUNT, lua.null()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn argument_and_body_errors_reach_lua_as_strings() {
        let lua = Lua::new();
        let callback = create(&lua, |_, _: Table| -> mlua::Result<Value> {
            Err(mlua::Error::RuntimeError(
                "host callback failure".to_owned(),
            ))
        })
        .unwrap();
        lua.globals().set("callback", callback).unwrap();
        lua.load(
            r#"
            local ok, message = pcall(callback, false)
            assert(not ok and type(message) == 'string')
            assert(message:find('bad argument #1', 1, true))
            local ok, message = pcall(callback, {})
            assert(not ok and type(message) == 'string')
            assert(message:find('host callback failure', 1, true))
            for i = 1, 100 do
                local ok, message = pcall(callback, {})
                assert(not ok and type(message) == 'string')
            end
        "#,
        )
        .exec()
        .unwrap();
    }

    #[test]
    fn callback_results_preserve_arity_and_values() {
        let lua = Lua::new();
        lua.globals()
            .set("empty", create(&lua, |_, ()| Ok(())).unwrap())
            .unwrap();
        lua.globals()
            .set(
                "identity",
                create(&lua, |_, value: Value| Ok(value)).unwrap(),
            )
            .unwrap();
        lua.load(
            r#"
            assert(select('#', empty()) == 0)
            assert(select('#', identity(nil)) == 1)
            local value = {}
            assert(identity(value, false, {}) == value)
            assert(identity('text') == 'text')
            assert(identity(12) == 12)
        "#,
        )
        .exec()
        .unwrap();
    }

    #[test]
    fn wrapper_uses_the_original_error_function() {
        let lua = Lua::new();
        lua.globals()
            .set("callback", create(&lua, |_, _: Table| Ok(())).unwrap())
            .unwrap();
        lua.load(
            r#"
            error = function() return 'replaced' end
            local ok, message = pcall(callback, false)
            assert(not ok and type(message) == 'string')
            assert(message:find('bad argument #1', 1, true))
        "#,
        )
        .exec()
        .unwrap();
    }

    #[test]
    fn wrapper_rejects_error_userdata_before_rust_and_preserves_null() {
        let lua = Lua::new();
        let reached = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let called = Arc::clone(&reached);
        let callback = create(&lua, move |_, value: Value| {
            called.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(value)
        })
        .unwrap();
        lua.globals().set("callback", callback).unwrap();
        lua.globals().set("null", lua.null()).unwrap();
        lua.globals()
            .set(
                "failure",
                lua.create_function(|_, ()| -> mlua::Result<()> {
                    Err(mlua::Error::RuntimeError("injected Rust error".to_owned()))
                })
                .unwrap(),
            )
            .unwrap();
        lua.load(
            r#"
            local ok, failure = pcall(failure)
            assert(not ok and type(failure) == 'userdata')
            local ok, message = pcall(callback, failure)
            assert(not ok and type(message) == 'string')
            assert(message == 'unsupported Lua userdata')
            assert(callback(null) == null)
        "#,
        )
        .exec()
        .unwrap();
        assert_eq!(reached.load(std::sync::atomic::Ordering::Relaxed), 1);
    }
}
