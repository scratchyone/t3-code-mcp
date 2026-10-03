// send_message delivery: pick queue vs auto, and read back what T3 actually did. Run: node --test test/
import { test } from "node:test";
import assert from "node:assert/strict";
import { activeRun, shouldQueue, isSteerRejection, deliveryOutcome, pendingRequests, liveState } from "../threads.mjs";

const run = (id, status, userMessageId, extra = {}) => ({ id, status, userMessageId, queuePosition: null, ...extra });

test("a run settling after its turn (status waiting) is queued, not steered", () => {
  const p = { runs: [run("r1", "completed", "m0"), run("r2", "waiting", "m1")] };
  assert.equal(activeRun(p).id, "r2");
  assert.equal(shouldQueue("auto", p), true);
  assert.equal(shouldQueue(undefined, p), true);
});

test("auto stays auto for running or idle threads; queue is always queue", () => {
  assert.equal(shouldQueue("auto", { runs: [run("r1", "running", "m1")] }), false);
  assert.equal(shouldQueue("auto", { runs: [run("r1", "completed", "m1")] }), false);
  assert.equal(shouldQueue("auto", null), false);
  assert.equal(shouldQueue("queue", { runs: [] }), true);
});

test("T3's steer rejections are recognised; unrelated errors are not", () => {
  for (const m of [
    "desktop: Target run run:x is waiting and cannot be steered.",
    "desktop: No running provider turn found for active run run:x.",
    "desktop: No running providerInstanceId turn found for active run run:x.",
    "desktop: Provider thread p has no active provider session for steering.",
    "desktop: claudeAgent cannot satisfy message dispatch mode steer_active for command c.",
  ]) assert.equal(isSteerRejection(new Error(m)), true, m);
  for (const m of ["desktop: couldn't get a WebSocket ticket (HTTP 401)", "desktop: orchestration.dispatchCommand timed out"]) {
    assert.equal(isSteerRejection(new Error(m)), false, m);
  }
});

test("steered: the message is attached to the run another message started", () => {
  const p = {
    runs: [run("r1", "running", "m-original")],
    messages: [{ id: "m-new", runId: "r1", role: "user" }],
    turnItems: [{ type: "user_message", messageId: "m-new", runId: "r1", inputIntent: "steer" }],
  };
  assert.deepEqual(deliveryOutcome(p, "m-new", "r1"), {
    delivery: "steered", detail: "Delivered into the running turn; the agent picks it up at its next step.", run_id: "r1", run_status: "running",
  });
});

test("queued: the message owns a new queued run", () => {
  const p = { runs: [run("r1", "running", "m-original"), run("r2", "queued", "m-new", { queuePosition: 1 })] };
  const o = deliveryOutcome(p, "m-new", "r1");
  assert.equal(o.delivery, "queued");
  assert.equal(o.queue_position, 1);
  assert.equal(o.run_id, "r2");
});

test("started: idle thread, the message starts its own run", () => {
  const o = deliveryOutcome({ runs: [run("r1", "completed", "m0"), run("r2", "starting", "m-new")] }, "m-new", undefined);
  assert.equal(o.delivery, "started");
  assert.equal(o.run_status, "starting");
});

test("restarted: the previously active run was interrupted for this message", () => {
  const o = deliveryOutcome({ runs: [run("r1", "interrupted", "m0"), run("r2", "running", "m-new")] }, "m-new", "r1");
  assert.equal(o.delivery, "restarted");
});

test("not visible yet: null, so the caller polls and then says unconfirmed", () => {
  assert.equal(deliveryOutcome({ runs: [run("r1", "running", "m0")], messages: [] }, "m-new", "r1"), null);
  assert.equal(deliveryOutcome(null, "m-new"), null);
});

test("liveState reads the live snapshot, not the cached shell", () => {
  const stale = { status: "waiting", pendingRuntimeRequest: true };
  assert.equal(liveState({ runs: [run("r1", "completed", "m0")], runtimeRequests: [] }, stale), "completed");
  assert.equal(liveState({ runs: [run("r1", "running", "m0")], runtimeRequests: [{ status: "pending", kind: "user_input" }] }, stale), "waiting_on_you");
  assert.equal(liveState({ runs: [run("r1", "running", "m0")], runtimeRequests: [] }, stale), "running");
  assert.equal(liveState(null, { status: "idle" }), "idle");
  assert.deepEqual(pendingRequests({ runtimeRequests: [{ status: "pending", kind: "approval" }, { status: "cancelled" }] }).map((r) => r.kind), ["approval"]);
});
