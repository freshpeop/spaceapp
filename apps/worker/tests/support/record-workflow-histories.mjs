import { spawnSync } from "node:child_process";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";

const version = process.argv[2];
if (process.argv.length !== 3 || !/^[a-z0-9][a-z0-9-]{0,40}$/.test(version ?? "")) {
  console.error("Usage: npm run temporal:record-histories -w @space/worker -- <new-version>");
  console.error("Existing versions are immutable. Add histories only with a reviewed patched() change.");
  process.exit(1);
}
const require = createRequire(import.meta.url);
const vitest = join(dirname(require.resolve("vitest/package.json")), "vitest.mjs");
const result = spawnSync(process.execPath, [vitest, "run", "tests/workflow-sandbox.test.ts"], {
  stdio: "inherit", env: { ...process.env, SPACE_RECORD_WORKFLOW_HISTORIES: version }
});
if (result.error) console.error(result.error);
process.exit(result.status ?? 1);
