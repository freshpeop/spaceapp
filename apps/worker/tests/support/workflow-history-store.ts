import { mkdir, readFile, readdir, writeFile } from "node:fs/promises";
import { join } from "node:path";

export async function historyFiles(directory: string): Promise<string[]> {
  const entries = await readdir(directory, { withFileTypes: true }).catch((error: NodeJS.ErrnoException) => {
    if (error.code === "ENOENT") return [];
    throw error;
  });
  const files = await Promise.all(entries.map(async entry => {
    if (entry.isDirectory()) return (await historyFiles(join(directory, entry.name))).map(name => join(entry.name, name));
    return entry.isFile() && entry.name.endsWith(".json") ? [entry.name] : [];
  }));
  return files.flat().sort();
}

export async function recordHistories(directory: string, version: string, records: Map<string, unknown>) {
  if (!/^[a-z0-9][a-z0-9-]{0,40}$/.test(version)) throw new Error("Use a new, simple history version name");
  if (!records.size) throw new Error("Refusing an empty history corpus");
  const encoded = [...records].map(([name, record]) => {
    if (!/^[a-zA-Z0-9-]+\.json$/.test(name)) throw new Error(`Invalid history filename: ${name}`);
    return [name, JSON.stringify(record, null, 2) + "\n"] as const;
  });
  await mkdir(directory, { recursive: true });
  const destination = join(directory, version);
  // No recursive mkdir here: an existing version, even empty, is an error.
  // Never replace or delete previously committed execution histories.
  await mkdir(destination);
  for (const [name, text] of encoded) await writeFile(join(destination, name), text, { flag: "wx" });
  return encoded.length;
}

export async function readHistoryRecords(directory: string) {
  return Promise.all((await historyFiles(directory)).map(async name => ({
    name, ...JSON.parse(await readFile(join(directory, name), "utf8"))
  })));
}
