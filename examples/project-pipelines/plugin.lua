-- Project Pipelines local plugin entrypoint.
--
-- The current botster-hub scaffold loads this package through the host-supplied
-- Project Pipelines runtime bundle because this stub does not yet register MCP
-- descriptors or workflow handlers through the Lua ABI. Keep workflow policy
-- documented here and in README.md so the Lua implementation can replace the
-- host bundle directly.
return {
  name = "project-pipelines",
}
