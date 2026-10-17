// No build: `zig build --fetch` only downloads the packages of build.zig.zon (ci/remote/fetch-public.sh).
const std = @import("std");

pub fn build(b: *std.Build) void {
    _ = b;
}
