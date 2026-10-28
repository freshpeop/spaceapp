import { mkdtemp, mkdir, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, expect, it } from "vitest";
import { historyFiles, recordHistories, readHistoryRecords } from "./support/workflow-history-store.js";

const directories: string[] = [];
async function directory() { const dir = await mkdtemp(join(tmpdir(), "history-store-")); directories.push(dir); return dir; }
afterEach(async () => { await Promise.all(directories.splice(0).map(dir => rm(dir, { recursive: true, force: true }))); });
const old = new Map([["synthetic.json", { workflowId: "synthetic", history: { events: ["old-history"] } }]]);

it("refuses to overwrite a version and preserves its exact bytes", async () => {
  const dir = await directory(); await recordHistories(dir, "v1", old);
  const before = await readFile(join(dir, "v1/synthetic.json"));
  await expect(recordHistories(dir, "v1", new Map([["synthetic.json", "replacement"]]))).rejects.toMatchObject({ code: "EEXIST" });
  expect(await readFile(join(dir, "v1/synthetic.json"))).toEqual(before);
});
it("adds a version without losing older histories from the replay corpus", async () => {
  const dir = await directory(); await recordHistories(dir, "v1", old);
  await recordHistories(dir, "patch-v2", new Map([["new.json", { workflowId: "new", history: {} }]]));
  expect(await historyFiles(dir)).toEqual(["patch-v2/new.json", "v1/synthetic.json"]);
  expect((await readHistoryRecords(dir)).map(h => h.workflowId)).toEqual(["new", "synthetic"]);
});
it("rejects path escapes, invalid filenames and empty recordings before writing", async () => {
  const dir = await directory();
  await expect(recordHistories(dir, "../escape", old)).rejects.toThrow("version");
  await expect(recordHistories(dir, "v1", new Map([["../escape.json", {}]]))).rejects.toThrow("filename");
  await expect(recordHistories(dir, "v1", new Map())).rejects.toThrow("empty");
  expect(await historyFiles(dir)).toEqual([]);
});
it("does not reuse an empty directory left by an interrupted recording", async () => {
  const dir = await directory(); await mkdir(join(dir, "v1"));
  await expect(recordHistories(dir, "v1", old)).rejects.toMatchObject({ code: "EEXIST" });
});
