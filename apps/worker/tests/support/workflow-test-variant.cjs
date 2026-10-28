// Only applied to the production workflows.ts by the test bundler. The legacy
// bundle records histories without the patch marker; today's unmodified code
// must subsequently replay those histories. Never edit a recorded history.
module.exports = function (source) {
  const { variant } = this.getOptions();
  const replaceOnce = (before, after) => {
    if (source.split(before).length !== 2) throw new Error(`Expected one test variant anchor: ${before}`);
    source = source.replace(before, after);
  };
  if (variant === "legacy") {
    replaceOnce('!patched("room-background-continuation-v1")', "true");
    replaceOnce('patched("room-background-action-failure-v1")', "false");
  } else if (variant === "extra-timer") {
    replaceOnce("  condition,", "  condition,\n  sleep,");
    replaceOnce("  let woken = false;", '  await sleep("1 second");\n  let woken = false;');
  } else throw new Error(`Unknown test variant: ${variant}`);
  return source;
};
