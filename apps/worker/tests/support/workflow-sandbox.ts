import { randomUUID } from "node:crypto";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { TestWorkflowEnvironment } from "@temporalio/testing";
import { temporal } from "@temporalio/proto";
import { bundleWorkflowCode, Runtime, DefaultLogger, Worker, type WorkflowBundle } from "@temporalio/worker";
import type { WorkflowHandle } from "@temporalio/client";
import { expect } from "vitest";
import { readHistoryRecords } from "./workflow-history-store.js";

const here = dirname(fileURLToPath(import.meta.url));
export const historiesDir = resolve(here, "../fixtures/workflow-histories");
export const mainQueue = "sandbox-current";
export const legacyQueue = "sandbox-legacy";
export type History = Awaited<ReturnType<WorkflowHandle["fetchHistory"]>>;
export type Activities = Record<string, any>;
export type RecordedHistory = { workflowId: string; history: History };

export async function bundle(variant?: "legacy" | "extra-timer") {
  return bundleWorkflowCode({
    workflowsPath: resolve(here, "sandbox-workflows.ts"),
    webpackConfigHook(config) {
      if (variant) config.module!.rules!.unshift({
        test: /[/\\]src[/\\]workflows\.ts$/,
        enforce: "pre",
        use: [{ loader: resolve(here, "workflow-test-variant.cjs"), options: { variant } }]
      });
      return config;
    }
  });
}

export async function readHistories() {
  const records = await readHistoryRecords(historiesDir);
  // fetchHistory().toJSON() uses protobufjs JSON (seconds/nanos, base64 payloads),
  // not Temporal CLI's proto-JSON format. Decode using the very same SDK proto
  // type before replay; passing string event IDs to Worker invokes a different
  // JSON importer and can fail with mixed protobufjs versions in this lockfile.
  return records.map(record => ({ ...record,
    history: temporal.api.history.v1.History.fromObject(record.history)
  })) as (RecordedHistory & { name: string })[];
}

export async function replay(bundle: WorkflowBundle, histories: (RecordedHistory & { name?: string })[]) {
  const results = [];
  for await (const result of Worker.runReplayHistories({ workflowBundle: bundle }, histories)) {
    // The diagnostic includes the filename, not an opaque synthetic UUID.
    const name = histories[results.length]?.name ?? result.workflowId;
    expect(result.error, `Replay failed: ${name}: ${result.error}`).toBeUndefined();
    results.push(result);
  }
  expect(results).toHaveLength(histories.length);
}

export class WorkflowSandbox {
  env!: TestWorkflowEnvironment;
  current!: WorkflowBundle;
  legacy!: WorkflowBundle;
  activities: Activities = {};
  histories = new Map<string, RecordedHistory>();
  private workers: Worker[] = [];
  private running: Promise<void>[] = [];
  private failure: unknown;
  active = false;
  private clocks = new Map<string, { stop: boolean; done: Promise<void>; error?: unknown }>();

  async start() {
    if (!this.current) {
      Runtime.install({ logger: new DefaultLogger("ERROR") });
      this.current = await bundle();
      this.legacy = await bundle("legacy");
    }
    this.workers = []; this.running = []; this.failure = undefined;
    this.env = await TestWorkflowEnvironment.createTimeSkipping({ client: { identity: "synthetic-test-client" } });
    this.active = true;
    // Register only activity stubs. The production workflow modules are bundled
    // verbatim and the actual SDK/server handle timers, signals and children.
    const names = ["recordDummyTurnStarted", "recordDummyTurnCompleted", "dotGoalStep", "dotGoalObserve",
      "runOpenCodeAgentTurn", "runCliAgentTurn", "runCodexAppServerTurn", "runRoomAgentBackgroundAction",
      "failRoomAgentBackgroundAction", "markRoomAgentMissionStarted", "markRoomAgentMissionFinished",
      "markRoomAgentMissionContinued", "settleRoomAgentBackgroundMission", "prepareRoomAgentBackgroundContinuation",
      "checkRoomAgentSupervisorTurn", "discardRoomAgentSupervisorTurn", "finishRoomAgentSupervisorTurn",
      "refreshMemoryGraphSnapshot", "refreshAllMonthsMemoryGraphSnapshot", "reconcileStaleSpaceAgentSessions",
      "createScheduledMemoryAudit", "prepareMemoryConsolidation", "executePersistedMemoryChangeSet", "finalizeMemoryRepair"];
    const dispatch = Object.fromEntries(names.map(name => [name, (...args: any[]) => {
      if (!this.activities[name]) throw new Error(`Missing activity stub: ${name}`);
      return this.activities[name]!(...args);
    }]));
    for (const taskQueue of [mainQueue, legacyQueue, "space-memory-maintenance", "space-memory-mutations"]) {
      const worker = await Worker.create({
        connection: this.env.nativeConnection, namespace: this.env.namespace, taskQueue, identity: "synthetic-test-worker",
        workflowBundle: taskQueue === legacyQueue ? this.legacy : this.current,
        activities: dispatch, shutdownGraceTime: "0 seconds", shutdownForceTime: "5 seconds",
        // Deliver stub heartbeats promptly; do not wait for the SDK's default
        // 80%-of-heartbeat-timeout batching in cancellation tests.
        maxHeartbeatThrottleInterval: "10 milliseconds", defaultHeartbeatThrottleInterval: "10 milliseconds"
      });
      this.workers.push(worker);
      this.running.push(worker.run().catch(error => { this.failure = error; }));
    }
  }

  async stop() {
    if (!this.active) return;
    for (const clock of this.clocks.values()) clock.stop = true;
    await Promise.all([...this.clocks.values()].map(clock => clock.done));
    this.clocks.clear();
    for (const worker of this.workers) if (worker.getState() === "RUNNING") worker.shutdown();
    try { await Promise.all(this.running); }
    finally { await this.env?.teardown(); this.active = false; }
    if (this.failure) throw this.failure;
  }

  async startWorkflow(type: string, arg: unknown, taskQueue = mainQueue) {
    if (this.failure) throw this.failure;
    return this.env.client.workflow.start(type, {
      workflowId: `synthetic-${type}-${randomUUID()}`, taskQueue, args: [arg]
    });
  }

  advanceUntilClosed(handle: WorkflowHandle) {
    // Awaiting result() on an endless supervisor lets the test server jump ten
    // years to its default execution timeout once it becomes idle. Advance in
    // bounded steps instead; each sleep relocks the test server's clock.
    const clock: { stop: boolean; done: Promise<void>; error?: unknown } = { stop: false, done: Promise.resolve() };
    clock.done = (async () => {
      while (!clock.stop) await this.env.sleep("1 second");
    })().catch(error => { clock.error = error; this.failure = error; });
    this.clocks.set(handle.workflowId, clock);
    return clock.done;
  }

  // Read genuine server histories, including continue-as-new runs and children.
  // Tests close idle supervisors explicitly before capturing their histories.
  async capture(label: string, handle: WorkflowHandle) {
    const seen = new Set<string>();
    const visit = async (id: string, runId?: string) => {
      if (seen.has(`${id}/${runId}`)) return;
      seen.add(`${id}/${runId}`);
      const history = await this.env.client.workflow.getHandle(id, runId).fetchHistory();
      const type = history.events![0]!.workflowExecutionStartedEventAttributes!.workflowType!.name!;
      this.histories.set(`${label.replace(/[^a-z0-9-]/g, "-")}--${seen.size}--${type}.json`, { workflowId: id, history });
      for (const event of history.events ?? []) {
        const continued = event.workflowExecutionContinuedAsNewEventAttributes?.newExecutionRunId;
        if (continued) await visit(id, continued);
        const child = event.childWorkflowExecutionStartedEventAttributes?.workflowExecution;
        if (child?.workflowId) await visit(child.workflowId, child.runId!);
      }
    };
    await visit(handle.workflowId, (handle as any).firstExecutionRunId);
  }

  async close(label: string, handle: WorkflowHandle) {
    const clock = this.clocks.get(handle.workflowId);
    if (clock) {
      clock.stop = true; await clock.done; this.clocks.delete(handle.workflowId);
      if (clock.error) throw clock.error;
    }
    await handle.terminate("Synthetic test finished");
    await this.capture(label, handle);
  }
}

export function deferred<T = void>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(r => { resolve = r; });
  return { promise, resolve, async wait(timeout = 10_000) {
    let timer: ReturnType<typeof setTimeout>;
    try {
      return await Promise.race([promise, new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error("Synthetic activity barrier timed out")), timeout);
      })]);
    } finally { clearTimeout(timer!); }
  } };
}

export async function until(predicate: () => boolean, label = "activity observation") {
  await expect.poll(predicate, { timeout: 10_000, interval: 10, message: label }).toBe(true);
}
