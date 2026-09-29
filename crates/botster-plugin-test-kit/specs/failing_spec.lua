-- Expected to fail: the runner must report each failure and exit non-zero.
local kit = require("botster.test")
kit.test("an equality that does not hold", function(t)
  t:eq(1, 2)
end)
kit.test("a load that does not resolve", function(t)
  t:load("no-such-plugin")
end)
