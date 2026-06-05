-- Project Pipelines local plugin entrypoint.
--
-- The current botster-hub scaffold loads this package through the host-supplied
-- Project Pipelines runtime bundle while Lua entrypoint execution is still
-- absent from the reduced hub crate. Keep workflow policy documented here and
-- in README.md so the future Lua adapter can replace the host bundle directly.
return {
  name = "project-pipelines",
}
