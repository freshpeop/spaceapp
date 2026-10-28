import { performance } from "node:perf_hooks";
import { ApplicationFailure, TimeoutFailure, TimeoutType, defaultPayloadConverter } from "@temporalio/common";
import { Context } from "@temporalio/activity";
import { Worker } from "@temporalio/worker";
import { DeterminismViolationError } from "@temporalio/workflow";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { DOT_GOAL_SIGNAL, ROOM_AGENT_ENQUEUE_SIGNAL, ROOM_AGENT_STOP_SIGNAL,
  roomAgentSupervisorInputSchema, roomAgentSupervisorQueueItemSchema } from "@space/contracts";
import { continuationFixture } from "./room-continuation-fixture.js";
import { discardSupervisorTurn, finishSupervisorTurn, prepareBackgroundContinuation, supervisorTurnState } from "../src/room-background-continuation.js";
import { WorkflowSandbox, bundle, deferred, historiesDir, legacyQueue, mainQueue, readHistories, replay, until } from "./support/workflow-sandbox.js";
import { recordHistories } from "./support/workflow-history-store.js";

const sandbox = new WorkflowSandbox();
const recording = process.env.SPACE_RECORD_WORKFLOW_HISTORIES;
let started: number;
let scenarioFailed = false;
let scenarioId = 0;
beforeAll(async () => {
  started = performance.now();
  await sandbox.start();
  const old = await readHistories();
  if (recording) {
    expect(recording).toMatch(/^[a-z0-9][a-z0-9-]{0,40}$/);
    // A recording run must NOT conceal an existing replay failure.
    if (old.length) await replay(sandbox.current, old);
  }
}, 90_000);
beforeEach(async context => {
  scenarioId++; sandbox.activities = {};
  context.onTestFailed(() => { scenarioFailed = true; });
  if (!sandbox.active) await sandbox.start();
}, 30_000);
afterEach(async () => { await sandbox.stop(); }, 15_000);
afterAll(async () => {
  await sandbox.stop();
  const elapsed = performance.now() - started;
  console.info(`Temporal sandbox, capture and replay: ${Math.round(elapsed)} ms`);
  if (process.env.CI) expect(elapsed, "Temporal additions must finish in <180 seconds").toBeLessThan(180_000);
}, 30_000);

function syntheticError(message: string) { const error = new Error(message); error.stack = `Error: ${message}`; return error; }

const completed = { status: "COMPLETED", message: "Synthetic completion", roomAgentOutcome: {
  status: "VERIFIED", executedActionCount: 1, statusReason: "Synthetic verification"
} };
function item(index = 0, missionId = "mission:synthetic") {
  return roomAgentSupervisorQueueItemSchema.parse({ missionId, turn: {
    roomId: "room:synthetic", paneId: "pane:synthetic", prompt: `Synthetic request ${index}`,
    agentRunId: `run:synthetic-${index}`, traceId: `trace:synthetic-${scenarioId}-${index}`
  } });
}
function supervisorInput(extra: Record<string, unknown> = {}) {
  return roomAgentSupervisorInputSchema.parse({ roomId: "room:synthetic", ...extra });
}

describe("real workflow sandbox", () => {
  it("runs dummy turns, retrying activities with the real retry policy", async () => {
    const attempts: number[] = [];
    sandbox.activities.recordDummyTurnStarted = async () => {
      attempts.push(Context.current().info.attempt);
      if (attempts.length < 3) throw syntheticError("Synthetic transient failure");
    };
    sandbox.activities.recordDummyTurnCompleted = vi.fn(async () => completed);
    const handle = await sandbox.startWorkflow("dummyTurnWorkflow", item().turn);
    expect(await handle.result()).toEqual(completed);
    expect(attempts).toEqual([1, 2, 3]);
    expect(sandbox.activities.recordDummyTurnCompleted).toHaveBeenCalledOnce();
    await sandbox.capture("dummy-retries", handle);
  }, 20_000);

  it.each([
    [undefined, "runCodexAppServerTurn", 5],
    ["opencode", "runOpenCodeAgentTurn", 1],
    ["cli:claude", "runCliAgentTurn", 1]
  ])("routes turn provider %s and enforces its retry limit", async (providerId, activity, limit) => {
    const attempts: number[] = [];
    sandbox.activities[activity] = async () => { attempts.push(Context.current().info.attempt); throw syntheticError("Synthetic exhausted failure"); };
    const handle = await sandbox.startWorkflow("codexAppServerTurnWorkflow", { ...item().turn, providerId });
    await expect(handle.result()).rejects.toThrow();
    expect(attempts).toEqual(Array.from({ length: limit }, (_, i) => i + 1));
    await sandbox.capture(`turn-${providerId ?? "native"}-failure`, handle);
  }, 20_000);

  it("wakes a paused goal before its one-hour timeout", async () => {
    const step = vi.fn().mockResolvedValueOnce({ state: "PAUSED", runId: null, goalStatus: "ACTIVE" })
      .mockResolvedValue({ state: "DONE", runId: null, goalStatus: "COMPLETED" });
    sandbox.activities.dotGoalStep = step;
    const handle = await sandbox.startWorkflow("dotGoalWorkflow", { goalId: "goal:wake" });
    await until(() => step.mock.calls.length === 1);
    // Wait until the timer is durably recorded before delivering the signal.
    await expect.poll(async () => (await handle.fetchHistory()).events!.some(e => !!e.timerStartedEventAttributes)).toBe(true);
    await handle.signal(DOT_GOAL_SIGNAL);
    expect(await handle.result()).toEqual({ goalStatus: "COMPLETED" });
    const history = await handle.fetchHistory();
    expect(history.events!.some(e => !!e.timerCanceledEventAttributes)).toBe(true);
    expect(history.events!.some(e => !!e.timerFiredEventAttributes)).toBe(false);
    expect(step).toHaveBeenCalledTimes(2);
    await sandbox.capture("goal-wake", handle);
  });

  it("retains a wake signal received during an activity", async () => {
    const entered = deferred(); const release = deferred(); let steps = 0;
    sandbox.activities.dotGoalStep = async () => {
      if (++steps === 1) { entered.resolve(); await release.promise; return { state: "PAUSED", runId: null, goalStatus: "ACTIVE" }; }
      return { state: "DONE", runId: null, goalStatus: "COMPLETED" };
    };
    const handle = await sandbox.startWorkflow("dotGoalWorkflow", { goalId: "goal:retained-wake" });
    await entered.wait(); await handle.signal(DOT_GOAL_SIGNAL); release.resolve();
    expect(await handle.result()).toEqual({ goalStatus: "COMPLETED" });
    expect((await handle.fetchHistory()).events!.filter(e => e.timerStartedEventAttributes)).toHaveLength(0);
    await sandbox.capture("goal-retained-wake", handle);
  });

  it("fires durable wait and observation timers when no wake arrives", async () => {
    sandbox.activities.dotGoalStep = vi.fn()
      .mockResolvedValueOnce({ state: "WAITING", runId: null, goalStatus: "ACTIVE" })
      .mockResolvedValueOnce({ state: "RUNNING", runId: "run:goal", goalStatus: "ACTIVE" })
      .mockResolvedValue({ state: "DONE", runId: null, goalStatus: "COMPLETED" });
    sandbox.activities.dotGoalObserve = vi.fn().mockResolvedValueOnce({ run: "RUNNING", goalStatus: "ACTIVE" })
      .mockResolvedValue({ run: "COMPLETED", goalStatus: "ACTIVE" });
    const handle = await sandbox.startWorkflow("dotGoalWorkflow", { goalId: "goal:timers" });
    expect(await handle.result()).toEqual({ goalStatus: "COMPLETED" });
    const timers = (await handle.fetchHistory()).events!.filter(e => e.timerStartedEventAttributes)
      .map(e => Number(e.timerStartedEventAttributes!.startToFireTimeout!.seconds));
    expect(timers).toEqual([30, 15, 15]);
    expect(sandbox.activities.dotGoalObserve).toHaveBeenCalledTimes(2);
    await sandbox.capture("goal-timeouts", handle);
  });

  it("continues an unfinished goal as a new execution carrying its id", async () => {
    let steps = 0;
    sandbox.activities.dotGoalStep = vi.fn(async () => ++steps <= 300
      ? { state: "WAITING", runId: null, goalStatus: "ACTIVE" }
      : { state: "DONE", runId: null, goalStatus: "COMPLETED" });
    const handle = await sandbox.startWorkflow("dotGoalWorkflow", { goalId: "goal:continue" });
    expect(await handle.result()).toEqual({ goalStatus: "COMPLETED" });
    expect(steps).toBe(301);
    expect(sandbox.activities.dotGoalStep.mock.calls.every(([id]: [string]) => id === "goal:continue")).toBe(true);
    expect((await sandbox.env.client.workflow.getHandle(handle.workflowId, handle.firstExecutionRunId).fetchHistory())
      .events!.some(e => !!e.workflowExecutionContinuedAsNewEventAttributes)).toBe(true);
    await sandbox.capture("goal-continue", handle);
  }, 30_000);

  it.each([mainQueue, legacyQueue])("persists exhausted background failures with patch semantics on %s", async queue => {
    const attempts: number[] = [];
    sandbox.activities.runRoomAgentBackgroundAction = async () => {
      attempts.push(Context.current().info.attempt); throw syntheticError("Synthetic transport failure");
    };
    const failed = vi.fn(); sandbox.activities.failRoomAgentBackgroundAction = failed;
    const handle = await sandbox.startWorkflow("roomAgentActionWorkflow", {
      bridge: { roomId: "room:proof", missionId: "mission:proof" }, traceId: "trace:proof"
    }, queue);
    await expect(handle.result()).rejects.toThrow();
    expect(attempts).toEqual([1, 2, 3, 4, 5]);
    if (queue === mainQueue) expect(failed).toHaveBeenCalledWith({ roomId: "room:proof", missionId: "mission:proof",
      actionId: handle.workflowId, cancelled: false });
    else expect(failed).not.toHaveBeenCalled();
    await sandbox.capture(`action-${queue}`, handle);
  }, 20_000);

  it("continues memory maintenance after failed optional refreshes and executes a consolidation child", async () => {
    const attempts = { refresh: [] as number[], reconcile: [] as number[], mutation: [] as number[] };
    const result = { sourceHash: "synthetic-hash", nodeCount: 2, edgeCount: 1 };
    sandbox.activities.refreshMemoryGraphSnapshot = vi.fn(async () => result);
    sandbox.activities.refreshAllMonthsMemoryGraphSnapshot = async () => { attempts.refresh.push(Context.current().info.attempt); throw syntheticError("Synthetic optional refresh failure"); };
    sandbox.activities.reconcileStaleSpaceAgentSessions = async () => { attempts.reconcile.push(Context.current().info.attempt); throw syntheticError("Synthetic optional reconcile failure"); };
    sandbox.activities.createScheduledMemoryAudit = vi.fn(async () => ({ id: "audit:synthetic", workflowId: "consolidation:synthetic" }));
    sandbox.activities.prepareMemoryConsolidation = vi.fn(async () => ({ changeSetId: "change:synthetic", run: { id: "audit:synthetic" } }));
    sandbox.activities.executePersistedMemoryChangeSet = async () => {
      attempts.mutation.push(Context.current().info.attempt);
      if (attempts.mutation.length < 3) throw syntheticError("Synthetic mutation retry");
      return { status: "APPLIED" };
    };
    sandbox.activities.finalizeMemoryRepair = vi.fn(async () => ({ id: "audit:synthetic", status: "COMPLETED" }));
    const handle = await sandbox.startWorkflow("memoryMaintenanceWorkflow", { traceId: "trace:memory" }, "space-memory-maintenance");
    expect(await handle.result()).toEqual(result);
    expect(attempts).toEqual({ refresh: [1, 2, 3], reconcile: [1, 2, 3], mutation: [1, 2, 3] });
    expect(sandbox.activities.createScheduledMemoryAudit).toHaveBeenCalledWith({ traceId: "trace:memory", sourceHash: "synthetic-hash" });
    expect(sandbox.activities.finalizeMemoryRepair).toHaveBeenCalledWith({ runId: "audit:synthetic", changeSetId: "change:synthetic", mutationResult: { status: "APPLIED" } });
    expect((await handle.fetchHistory()).events!.filter(e => e.childWorkflowExecutionCompletedEventAttributes)).toHaveLength(1);
    await sandbox.capture("memory-maintenance", handle);
  }, 20_000);

  it("returns a no-change consolidation without running a mutation", async () => {
    sandbox.activities.prepareMemoryConsolidation = async () => ({ run: { id: "audit:no-change" }, changeSetId: null });
    sandbox.activities.executePersistedMemoryChangeSet = vi.fn();
    const handle = await sandbox.startWorkflow("memoryConsolidationWorkflow", { runId: "audit:no-change" });
    expect(await handle.result()).toEqual({ id: "audit:no-change" });
    expect(sandbox.activities.executePersistedMemoryChangeSet).not.toHaveBeenCalled();
    await sandbox.capture("memory-no-change", handle);
  });

  it("exhausts real memory mutation retries", async () => {
    const attempts: number[] = [];
    sandbox.activities.executePersistedMemoryChangeSet = async () => { attempts.push(Context.current().info.attempt); throw syntheticError("Synthetic mutation failure"); };
    const handle = await sandbox.startWorkflow("memoryMutationWorkflow", { changeSetId: "change:failed", traceId: "trace:failed" });
    await expect(handle.result()).rejects.toThrow();
    expect(attempts).toEqual([1, 2, 3]);
    await sandbox.capture("memory-mutation-failure", handle);
  });
});

function continuationDriver() {
  const c = continuationFixture();
  sandbox.activities = {
    prepareRoomAgentBackgroundContinuation: vi.fn(input => prepareBackgroundContinuation(input, c.store)),
    checkRoomAgentSupervisorTurn: vi.fn(input => supervisorTurnState(input, c.store)),
    discardRoomAgentSupervisorTurn: vi.fn(({ item, reason, errorCode }) => discardSupervisorTurn(item, reason, c.store, errorCode)),
    finishRoomAgentSupervisorTurn: vi.fn(input => finishSupervisorTurn(input, c.store)),
    runCodexAppServerTurn: vi.fn(async turn => {
      c.store.updateSpaceAgentRun(turn.agentRunId, { status: "COMPLETED" });
      return completed;
    })
  };
  return { ...c, input: roomAgentSupervisorInputSchema.parse({ roomId: c.room.id, monitoringMissionIds: [c.initial.mission.id] }) };
}
// Supervisors are deliberately endless; use bounded time advancement, then
// assert state and explicitly terminate instead of awaiting a final result.
function skipWhileRunning(handle: Awaited<ReturnType<WorkflowSandbox["startWorkflow"]>>) {
  return sandbox.advanceUntilClosed(handle);
}

describe("real supervisor signals, continuations and child workflows", () => {
  it("executes allocation, send and verification in a single mission", async () => {
    const c = continuationDriver();
    sandbox.activities.runCodexAppServerTurn.mockImplementationOnce(async (turn: any) => {
      expect(turn.prompt).toContain("Open CLI and Chat, then send a story");
      c.store.createRoomAgentAction({ ...c.action, actionId: "action:send", idempotencyKey: "send-once", actionType: "SEND",
        evidence: { cli: "Story complete", chat: "Story complete" }, statusReason: "Both tasks completed." });
      c.store.updateSpaceAgentRun(turn.agentRunId, { status: "COMPLETED" });
      return { ...completed, roomAgentOutcome: { ...completed.roomAgentOutcome, pendingActionIds: ["action:send"] } };
    });
    const handle = await sandbox.startWorkflow("roomAgentSupervisorWorkflow", c.input);
    const running = skipWhileRunning(handle);
    try {
      await until(() => c.store.getRoomAgentMission(c.room.id, c.initial.mission.id)?.status === "COMPLETED");
      expect(sandbox.activities.runCodexAppServerTurn).toHaveBeenCalledTimes(2);
      expect(c.store.listRoomAgentActions(c.initial.mission.id).filter(a => a.actionType === "SEND")).toHaveLength(1);
      expect(sandbox.activities.finishRoomAgentSupervisorTurn).toHaveBeenCalledOnce();
    } finally { await sandbox.close("supervisor-allocation-send-verify", handle); await running; }
  });

  it.each([
    ["ordinary", "ROOM_AGENT_SUPERVISOR_FAILED"],
    ["timeout", "ROOM_AGENT_SUPERVISOR_TIMEOUT"],
    ["application", "CODEX_APP_SERVER_TURN_TIMEOUT"]
  ])("persists the cause of a real %s child failure", async (kind, code) => {
    const c = continuationDriver();
    sandbox.activities.runCodexAppServerTurn = async () => {
      if (kind === "timeout") {
        // Activity code can only throw application failures. Keep a typed
        // timeout as its cause to exercise SDK conversion across the real child.
        const cause = new TimeoutFailure("Synthetic heartbeat timeout", undefined, TimeoutType.HEARTBEAT);
        cause.stack = "TimeoutFailure: Synthetic heartbeat timeout";
        const failure = ApplicationFailure.create({ message: "Synthetic activity timeout", nonRetryable: true, cause });
        failure.stack = "ApplicationFailure: Synthetic activity timeout";
        throw failure;
      }
      const failure = ApplicationFailure.nonRetryable("Synthetic child failure", kind === "application" ? code : "SyntheticError");
      failure.stack = "ApplicationFailure: Synthetic child failure";
      throw failure;
    };
    const handle = await sandbox.startWorkflow("roomAgentSupervisorWorkflow", c.input);
    const running = skipWhileRunning(handle);
    try {
      await until(() => c.store.getRoomAgentMission(c.room.id, c.initial.mission.id)?.status === "FAILED");
      expect(sandbox.activities.finishRoomAgentSupervisorTurn).toHaveBeenCalledWith(expect.objectContaining({ status: "FAILED", errorCode: code }));
      const discarded = sandbox.activities.discardRoomAgentSupervisorTurn.mock.calls[0][0].item;
      expect(c.store.getRoomAgentTurn(c.initial.mission.id, discarded.turn.agentRunId)?.run).toMatchObject({ status: "FAILED", errorCode: code });
    } finally { await sandbox.close(`supervisor-child-${kind}`, handle); await running; }
  }, 20_000);

  it.each(["stop", "follow-up"])("retires prepared work when %s arrives before the activity reply", async kind => {
    const c = continuationDriver(); const prepared = deferred<any>(); const release = deferred();
    sandbox.activities.prepareRoomAgentBackgroundContinuation.mockImplementationOnce(async (input: any) => {
      const result = await prepareBackgroundContinuation(input, c.store);
      prepared.resolve(result); await release.promise; return result;
    });
    const handle = await sandbox.startWorkflow("roomAgentSupervisorWorkflow", c.input);
    const running = skipWhileRunning(handle);
    try {
      const result = await prepared.wait();
      expect(result.state).toBe("READY");
      if (kind === "stop") {
        c.store.updateRoomAgentMission(c.initial.mission.id, { status: "INTERRUPTED" });
        await handle.signal(ROOM_AGENT_STOP_SIGNAL, { missionId: null, reason: "Stop" });
      } else {
        const follow = c.enqueue("follow-up", "Only inspect the room.");
        await handle.signal(ROOM_AGENT_ENQUEUE_SIGNAL, follow.queueItem);
      }
      release.resolve();
      await until(() => c.store.getRoomAgentTurn(c.initial.mission.id, result.item.turn.agentRunId)?.run.status === "INTERRUPTED");
      if (kind === "stop") expect(sandbox.activities.runCodexAppServerTurn).not.toHaveBeenCalled();
      else {
        await until(() => sandbox.activities.runCodexAppServerTurn.mock.calls.length > 0);
        expect(sandbox.activities.runCodexAppServerTurn.mock.calls[0][0].roomAgentOperatorRequest).toBe("Only inspect the room.");
      }
    } finally { release.resolve(); await sandbox.close(`supervisor-prepared-${kind}`, handle); await running; }
  });

  it("carries a prepared continuation through continue-as-new and runs it once", async () => {
    const c = continuationDriver();
    const handle = await sandbox.startWorkflow("roomAgentSupervisorWorkflow", { ...c.input, processedCount: 50 });
    const running = skipWhileRunning(handle);
    try {
      await until(() => c.store.getRoomAgentMission(c.room.id, c.initial.mission.id)?.status === "COMPLETED");
      const first = await sandbox.env.client.workflow.getHandle(handle.workflowId, handle.firstExecutionRunId).fetchHistory();
      const continued = first.events!.find(e => e.workflowExecutionContinuedAsNewEventAttributes)!.workflowExecutionContinuedAsNewEventAttributes!;
      const carried = defaultPayloadConverter.fromPayload(continued.input!.payloads![0]!) as any;
      expect(carried.pending).toHaveLength(1);
      expect(first.events!.filter(e => e.startChildWorkflowExecutionInitiatedEventAttributes)).toHaveLength(0);
      expect(sandbox.activities.runCodexAppServerTurn).toHaveBeenCalledOnce();
    } finally { await sandbox.close("supervisor-prepared-carry", handle); await running; }
  });

  it("drains the overflow before continuing with exactly 100 pending requests", async () => {
    let flooded = false; let handle: any;
    const calls: string[] = [];
    sandbox.activities.checkRoomAgentSupervisorTurn = async (queued: any) => {
      calls.push(queued.turn.agentRunId);
      if (!flooded) {
        flooded = true;
        for (let i = 1; i <= 105; i++) await handle.signal(ROOM_AGENT_ENQUEUE_SIGNAL, item(i));
      }
      return "SKIP";
    };
    sandbox.activities.discardRoomAgentSupervisorTurn = async () => {};
    handle = await sandbox.startWorkflow("roomAgentSupervisorWorkflow", supervisorInput({ pending: [item()], processedCount: 49 }));
    const running = skipWhileRunning(handle);
    try {
      await until(() => calls.length >= 106);
      const first = await sandbox.env.client.workflow.getHandle(handle.workflowId, handle.firstExecutionRunId).fetchHistory();
      const continued = first.events!.find(e => e.workflowExecutionContinuedAsNewEventAttributes)!.workflowExecutionContinuedAsNewEventAttributes!;
      const carried = defaultPayloadConverter.fromPayload(continued.input!.payloads![0]!) as any;
      expect(carried.pending).toHaveLength(100);
      expect(() => roomAgentSupervisorInputSchema.parse(carried)).not.toThrow();
      expect(first.events!.filter(e => e.activityTaskScheduledEventAttributes?.activityType?.name === "checkRoomAgentSupervisorTurn")).toHaveLength(6);
      expect(calls).toEqual(Array.from({ length: 106 }, (_, i) => `run:synthetic-${i}`));
    } finally { await sandbox.close("supervisor-carry-overflow", handle); await running; }
  }, 20_000);

  it.each([mainQueue, legacyQueue])("handles enqueue and cancellation of a running child on %s", async queue => {
    const entered = deferred(); const cancellations: string[] = [];
    sandbox.activities.checkRoomAgentSupervisorTurn = async () => "RUN";
    sandbox.activities.markRoomAgentMissionStarted = async () => {};
    sandbox.activities.markRoomAgentMissionFinished = vi.fn();
    sandbox.activities.discardRoomAgentSupervisorTurn = vi.fn();
    sandbox.activities.finishRoomAgentSupervisorTurn = vi.fn();
    sandbox.activities.runCodexAppServerTurn = async () => {
      entered.resolve();
      const context = Context.current();
      const heartbeats = setInterval(() => context.heartbeat(), 10);
      try { await context.cancelled; }
      catch (error) { cancellations.push("cancelled"); throw error; }
      finally { clearInterval(heartbeats); }
    };
    const handle = await sandbox.startWorkflow("roomAgentSupervisorWorkflow", supervisorInput(), queue);
    const running = skipWhileRunning(handle);
    try {
      await handle.signal(ROOM_AGENT_ENQUEUE_SIGNAL, item());
      await entered.wait();
      await handle.signal(ROOM_AGENT_STOP_SIGNAL, { missionId: null, reason: "Synthetic operator stop" });
      const finish = queue === mainQueue ? sandbox.activities.finishRoomAgentSupervisorTurn : sandbox.activities.markRoomAgentMissionFinished;
      await until(() => finish.mock.calls.length > 0);
      expect(finish.mock.calls[0][0]).toMatchObject({ status: "INTERRUPTED", statusReason: "Synthetic operator stop" });
      await until(() => cancellations.length === 1, "Activity acknowledged child cancellation");
      expect(cancellations).toHaveLength(1);
    } finally { await sandbox.close(`supervisor-active-stop-${queue}`, handle); await running; }
  }, 20_000);

  it.each([mainQueue, legacyQueue])("continues after 50 processed items with the next queued item intact on %s", async queue => {
    let processed = 0; const entered = deferred(); const release = deferred();
    sandbox.activities.checkRoomAgentSupervisorTurn = async () => "RUN";
    sandbox.activities.markRoomAgentMissionStarted = async () => {};
    sandbox.activities.markRoomAgentMissionFinished = async () => {};
    sandbox.activities.markRoomAgentMissionContinued = async () => {};
    sandbox.activities.finishRoomAgentSupervisorTurn = async () => {};
    sandbox.activities.runCodexAppServerTurn = async () => {
      if (++processed === 51) { entered.resolve(); await release.promise; }
      return completed;
    };
    const handle = await sandbox.startWorkflow("roomAgentSupervisorWorkflow", supervisorInput({ pending: Array.from({ length: 51 }, (_, i) => item(i, `mission:${i}`)) }), queue);
    const running = skipWhileRunning(handle);
    try {
      await entered.wait(25_000);
      const first = await sandbox.env.client.workflow.getHandle(handle.workflowId, handle.firstExecutionRunId).fetchHistory();
      const continued = first.events!.find(e => e.workflowExecutionContinuedAsNewEventAttributes)!.workflowExecutionContinuedAsNewEventAttributes!;
      const carried = defaultPayloadConverter.fromPayload(continued.input!.payloads![0]!) as any;
      expect(carried.pending).toEqual([item(50, "mission:50")]);
      expect(carried.processedCount).toBe(0);
      expect(first.events!.filter(e => e.childWorkflowExecutionCompletedEventAttributes)).toHaveLength(50);
      release.resolve();
    } finally { release.resolve(); await sandbox.close(`supervisor-fifty-${queue}`, handle); await running; }
  }, 30_000);

  it.each([mainQueue, legacyQueue])("continues after 250 one-second polls retaining monitoring on %s", async queue => {
    let polls = 0; const resumed = deferred(); const release = deferred();
    const monitor = async () => {
      if (++polls === 251) { resumed.resolve(); await release.promise; }
      return queue === mainQueue ? { state: "WAITING" } : false;
    };
    sandbox.activities.prepareRoomAgentBackgroundContinuation = monitor;
    sandbox.activities.settleRoomAgentBackgroundMission = monitor;
    const handle = await sandbox.startWorkflow("roomAgentSupervisorWorkflow", supervisorInput({ monitoringMissionIds: ["mission:monitor"], processedCount: 7 }), queue);
    const running = skipWhileRunning(handle);
    try {
      await resumed.wait(25_000);
      const first = await sandbox.env.client.workflow.getHandle(handle.workflowId, handle.firstExecutionRunId).fetchHistory();
      const continued = first.events!.find(e => e.workflowExecutionContinuedAsNewEventAttributes)!.workflowExecutionContinuedAsNewEventAttributes!;
      const carried = defaultPayloadConverter.fromPayload(continued.input!.payloads![0]!) as any;
      expect(carried.monitoringMissionIds).toEqual(["mission:monitor"]);
      expect(carried.pending).toEqual([]);
      expect(carried.processedCount).toBe(queue === mainQueue ? 0 : 7);
      const timers = first.events!.filter(e => e.timerStartedEventAttributes);
      expect(timers).toHaveLength(250);
      expect(timers.every(e => Number(e.timerStartedEventAttributes!.startToFireTimeout!.seconds) === 1)).toBe(true);
    } finally { release.resolve(); await sandbox.close(`supervisor-polls-${queue}`, handle); await running; }
  }, 30_000);

  it("keeps the legacy boolean settle activity contract", async () => {
    sandbox.activities.settleRoomAgentBackgroundMission = vi.fn(async () => true);
    sandbox.activities.prepareRoomAgentBackgroundContinuation = vi.fn();
    const handle = await sandbox.startWorkflow("roomAgentSupervisorWorkflow", supervisorInput({ monitoringMissionIds: ["mission:legacy"] }), legacyQueue);
    const running = skipWhileRunning(handle);
    try {
      await until(() => sandbox.activities.settleRoomAgentBackgroundMission.mock.calls.length > 0);
      expect(sandbox.activities.settleRoomAgentBackgroundMission).toHaveBeenCalledOnce();
      expect(sandbox.activities.prepareRoomAgentBackgroundContinuation).not.toHaveBeenCalled();
    } finally { await sandbox.close("supervisor-legacy-settle", handle); await running; }
  });

  it("carries pending, interrupted and monitoring state together after a rollover", async () => {
    const finished: string[] = [];
    sandbox.activities.discardRoomAgentSupervisorTurn = async () => {};
    sandbox.activities.finishRoomAgentSupervisorTurn = async ({ item: queued }: any) => { finished.push(queued.turn.agentRunId); };
    sandbox.activities.checkRoomAgentSupervisorTurn = async () => "SKIP";
    sandbox.activities.prepareRoomAgentBackgroundContinuation = async () => ({ state: "DONE" });
    const stopped = [0, 1].map(i => ({ item: item(i), reason: "Synthetic queued stop" }));
    const handle = await sandbox.startWorkflow("roomAgentSupervisorWorkflow", supervisorInput({
      pending: [item(2)], interrupted: stopped, monitoringMissionIds: ["mission:monitor"], processedCount: 49
    }));
    const running = skipWhileRunning(handle);
    try {
      await until(() => finished.length === 2);
      const first = await sandbox.env.client.workflow.getHandle(handle.workflowId, handle.firstExecutionRunId).fetchHistory();
      const continued = first.events!.find(e => e.workflowExecutionContinuedAsNewEventAttributes)!.workflowExecutionContinuedAsNewEventAttributes!;
      const carried = defaultPayloadConverter.fromPayload(continued.input!.payloads![0]!) as any;
      expect(carried).toEqual(supervisorInput({ pending: [item(2)], interrupted: [stopped[1]], monitoringMissionIds: ["mission:monitor"], processedCount: 0 }));
      expect(finished).toEqual(["run:synthetic-0", "run:synthetic-1"]);
    } finally { await sandbox.close("supervisor-all-state-carry", handle); await running; }
  });
});

describe("immutable execution history replay", () => {
  it("replays every recorded scenario with today's unmodified production workflows", async () => {
    expect(scenarioFailed, "Refusing to certify histories from failed scenarios").toBe(false);
    const fresh = [...sandbox.histories].map(([name, record]) => ({ name, ...record }));
    const types = new Set(fresh.map(r => r.history.events![0]!.workflowExecutionStartedEventAttributes!.workflowType!.name));
    expect([...types].sort()).toEqual(["codexAppServerTurnWorkflow", "dotGoalWorkflow", "dummyTurnWorkflow", "memoryConsolidationWorkflow",
      "memoryMaintenanceWorkflow", "memoryMutationWorkflow", "roomAgentActionWorkflow", "roomAgentSupervisorWorkflow"].sort());
    expect(fresh.some(r => r.name.includes("legacy"))).toBe(true);
    await replay(sandbox.current, fresh);
  }, 30_000);

  it("detects an extra durable timer with DeterminismViolationError", async () => {
    const modified = await bundle("extra-timer");
    const record = [...sandbox.histories].find(([name]) => name.startsWith("goal-timeouts--"))![1];
    const results = [];
    for await (const result of Worker.runReplayHistories({ workflowBundle: modified }, [record])) results.push(result);
    expect(results).toHaveLength(1);
    expect(results[0]!.error).toBeInstanceOf(DeterminismViolationError);
  }, 30_000);

  it("gates committed histories without rewriting them (or explicitly adds a new version)", async () => {
    expect(scenarioFailed, "Refusing to record failed or nondeterministic scenarios").toBe(false);
    if (recording) await recordHistories(historiesDir, recording, sandbox.histories);
    const committed = await readHistories();
    expect(committed.length, "Missing committed corpus; see workflow-histories/README.md").toBeGreaterThan(0);
    await replay(sandbox.current, committed);
  }, 30_000);
});

