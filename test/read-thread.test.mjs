// read_thread paging: long messages are never clipped silently. Run: node --test test/
import { test } from "node:test";
import assert from "node:assert/strict";
import { textChunk, shapeMessages } from "../threads.mjs";

const describe = (m) => ({ role: m.role });
const long = "Build report.\n" + "detail line\n".repeat(2_000) + "\n**Approval needed:** enable the flag and restart.\n**Test:** subscribe, then wait.";

test("short text is returned whole with no truncation flags", () => {
  assert.deepEqual(textChunk("hello"), { text: "hello", text_length: 5 });
});

test("a long message with a late approval section is reassembled exactly by paging", () => {
  let offset = 0, out = "", pages = 0, c;
  do {
    c = textChunk(long, offset, 8_000);
    out += c.text;
    pages++;
    if (c.text_truncated) { assert.equal(c.next_text_offset, offset + c.text.length); offset = c.next_text_offset; }
  } while (c.text_truncated);
  assert.equal(out, long);
  assert.ok(pages > 1);
  assert.match(c.text, /\*\*Approval needed:\*\*/);
  assert.equal(c.text_offset, offset);
  assert.equal(c.next_text_offset, undefined);
});

test("first chunk flags truncation and never hides that text was cut", () => {
  const c = textChunk(long, 0, 8_000);
  assert.equal(c.text_truncated, true);
  assert.equal(c.text.length, 8_000);
  assert.equal(c.text_length, long.length);
  assert.ok(!c.text.endsWith("…"));
});

test("offset past the end returns an empty final chunk", () => {
  const c = textChunk("abc", 99);
  assert.deepEqual(c, { text: "", text_length: 3, text_offset: 3 });
});

test("shapeMessages: newest message gets the budget first; truncated ids listed", () => {
  const msgs = [
    { id: "old1", role: "user", text: "x".repeat(10_000) },
    { id: "old2", role: "assistant", text: "y".repeat(10_000) },
    { id: "final", role: "assistant", text: long },
  ];
  const s = shapeMessages(msgs, 10, { perMessage: 8_000, total: 12_000, describe });
  assert.deepEqual(s.messages.map((m) => [m.message_id, m.text.length, !!m.text_truncated]), [["old1", 0, true], ["old2", 4_000, true], ["final", 8_000, true]]);
  assert.deepEqual(s.truncated_message_ids, ["old1", "old2", "final"]);
  assert.equal(s.messages[0].next_text_offset, 0, "fully budgeted-out message still says where to start");
});

test("shapeMessages keeps short threads unchanged apart from ids and lengths", () => {
  const s = shapeMessages([{ id: "a", role: "user", text: "hi" }], 10, { describe });
  assert.deepEqual(s, { messages: [{ message_id: "a", role: "user", text: "hi", text_length: 2 }], truncated_message_ids: [] });
});
