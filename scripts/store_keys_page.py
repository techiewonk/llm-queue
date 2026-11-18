"""Build a one-file HTML page from `docs/reference/store-keys.json`: every store key, searchable.

The page has the key expiry modes, the store-policy matrix, the keys one call touches, the
settings that change lifetimes, and a filterable catalogue of every key (fields, writers,
readers, lifetime, swept vs native, eviction impact, what deletes it). Open it in a browser or
publish it; it loads only Google Fonts.

    python scripts/store_keys_page.py                     # reports/store-keys.html
    python scripts/store_keys_page.py --out keys.html
"""

# ruff: noqa: E501  # the page is one embedded HTML / CSS / JS template
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

PAGE = r"""<title>llm-queue Store Keys</title>
<meta name="claude:viewport-fit-sides" content="cover">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans:wght@400;500;600&family=Space+Grotesk:wght@500;600&display=swap">
<style>
/* Layout: one reading column for the explanations, a wide filterable catalogue below. */
:root {
  --bg: #f6f7f5; --panel: #ffffff; --fg: #1d2420; --muted: #5b6660; --line: #dde2de;
  --accent: #0f7a64; --accent-soft: #e2f2ec; --warn: #a3590b; --warn-soft: #fbefe1;
  --bad: #b23a3a; --bad-soft: #f8e5e5; --code: #eef1ee;
  --display: "Space Grotesk", "Segoe UI", system-ui, sans-serif;
  --body: "IBM Plex Sans", "Segoe UI", system-ui, sans-serif;
  --mono: "IBM Plex Mono", ui-monospace, Consolas, monospace;
}
@media (prefers-color-scheme: dark) { :root:not([data-theme="light"]) {
  --bg: #121614; --panel: #1a201d; --fg: #e3e8e5; --muted: #98a39d; --line: #2c3430;
  --accent: #4cc7a6; --accent-soft: #173229; --warn: #e3a25b; --warn-soft: #36291a;
  --bad: #ee8282; --bad-soft: #3a2020; --code: #232a26; color-scheme: dark } }
:root[data-theme="dark"] {
  --bg: #121614; --panel: #1a201d; --fg: #e3e8e5; --muted: #98a39d; --line: #2c3430;
  --accent: #4cc7a6; --accent-soft: #173229; --warn: #e3a25b; --warn-soft: #36291a;
  --bad: #ee8282; --bad-soft: #3a2020; --code: #232a26; color-scheme: dark }
* { box-sizing: border-box }
body { background: var(--bg); color: var(--fg); font: 15px/1.6 var(--body);
  padding-left: max(16px, env(safe-area-inset-left, 0px));
  padding-right: max(16px, env(safe-area-inset-right, 0px)); padding-block: 32px 64px }
main { max-width: 1180px; margin: 0 auto; display: grid; gap: 40px }
.prose { max-width: 72ch }
h1, h2, h3 { font-family: var(--display); line-height: 1.2; text-wrap: balance; margin: 0 }
h1 { font-size: 2.1rem } h2 { font-size: 1.45rem } h3 { font-size: 1.05rem }
p { margin: 0 } section { display: grid; gap: 14px }
.eyebrow { font: 500 .75rem var(--mono); letter-spacing: .08em; text-transform: uppercase; color: var(--accent) }
.lede { color: var(--muted); font-size: 1.05rem }
code { font: .86em var(--mono); background: var(--code); padding: .08em .35em; border-radius: 4px; overflow-wrap: anywhere }
.scroll { overflow-x: auto; border: 1px solid var(--line); border-radius: 10px; background: var(--panel) }
table { border-collapse: collapse; width: 100%; font-size: .9rem }
th, td { text-align: left; vertical-align: top; padding: 9px 12px; border-bottom: 1px solid var(--line) }
th { font: 600 .78rem var(--body); letter-spacing: .03em; color: var(--muted); background: var(--bg) }
tr:last-child td { border-bottom: 0 }
.pill { display: inline-block; font: 500 .74rem var(--mono); padding: 2px 8px; border-radius: 999px; white-space: nowrap }
.ok { background: var(--accent-soft); color: var(--accent) }
.warn { background: var(--warn-soft); color: var(--warn) }
.bad { background: var(--bad-soft); color: var(--bad) }
.plain { background: var(--code); color: var(--muted) }
.modes { display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 14px }
.mode { background: var(--panel); border: 1px solid var(--line); border-radius: 10px; padding: 16px; display: grid; gap: 8px; min-width: 0 }
pre.flow { margin: 0; padding: 16px; font: .8rem/1.5 var(--mono); white-space: pre; color: var(--fg) }
.controls { display: flex; flex-wrap: wrap; gap: 8px; align-items: center }
.controls input { flex: 1 1 240px; min-width: 0; font: inherit; padding: 8px 12px; border-radius: 8px;
  border: 1px solid var(--line); background: var(--panel); color: var(--fg) }
.chip { font: 500 .8rem var(--body); padding: 5px 11px; border-radius: 999px; border: 1px solid var(--line);
  background: var(--panel); color: var(--fg); cursor: pointer }
.chip[aria-pressed="true"] { background: var(--accent); border-color: var(--accent); color: var(--bg) }
.chip:focus-visible, input:focus-visible, summary:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px }
.count { color: var(--muted); font-size: .85rem; font-variant-numeric: tabular-nums }
.keys { display: grid; gap: 8px }
details.key { background: var(--panel); border: 1px solid var(--line); border-radius: 10px }
details.key > summary { list-style: none; cursor: pointer; padding: 12px 14px; display: grid;
  grid-template-columns: minmax(0, 2.2fr) 90px minmax(0, 2fr) 150px; gap: 12px; align-items: start }
details.key > summary::-webkit-details-marker { display: none }
.kname { font: 500 .88rem var(--mono); overflow-wrap: anywhere }
.kgroup { display: block; font: .72rem var(--body); color: var(--muted); margin-top: 2px }
.ktype { font: .8rem var(--mono); color: var(--muted) }
.klife { font-size: .84rem; color: var(--muted); min-width: 0 }
.body { border-top: 1px solid var(--line); padding: 14px; display: grid; grid-template-columns: 160px minmax(0, 1fr); gap: 8px 16px; font-size: .9rem }
.body dt { color: var(--muted); font-weight: 600; font-size: .8rem }
.body dd { margin: 0; min-width: 0 }
@media (max-width: 760px) {
  details.key > summary { grid-template-columns: minmax(0, 1fr) auto }
  .klife, .ktype { display: none }
  .body { grid-template-columns: minmax(0, 1fr) }
}
@media (prefers-reduced-motion: no-preference) { details.key[open] .body { animation: in .15s ease-out } }
@keyframes in { from { opacity: .4 } to { opacity: 1 } }
</style>

<main>
  <header class="prose" style="display:grid;gap:10px">
    <div class="eyebrow">llm-queue __VERSION__ &middot; Valkey / Redis data model</div>
    <h1>What llm-queue stores, and for how long</h1>
    <p class="lede">Every key the library writes: its type, its fields, who writes and reads it, when it goes away, and what an evicting store would do to it. <code>P</code> is <code>LLMQ_KEY_PREFIX</code>, <code>QP</code> is <code>LLMQ_QUEUE_PREFIX</code>. Braces such as <code>{m:gpt20b}</code> are hash tags.</p>
  </header>

  <section>
    <div class="eyebrow">Two ways keys expire</div>
    <h2>Key expiry modes</h2>
    <p class="prose">Valkey's <code>volatile-*</code> policies evict only keys that have a TTL. In <b>swept</b> mode no key the library needs has one: its expiry time goes in the sorted set <code>P:ttl</code> and the library's sweeper deletes it when due (at most one sweep, 10 s, late). So the queue's keys can't be evicted, and your app's own cache keys keep being evicted as usual.</p>
    <div class="modes">
      <div class="mode"><h3>native</h3><p>Valkey TTLs, as in 0.1.0b2. Safe only on <code>noeviction</code>.</p><p><span class="pill plain">PEXPIRE key 180000</span></p></div>
      <div class="mode"><h3>swept</h3><p>No TTL; the index holds the expiry. Safe on <code>noeviction</code> and every <code>volatile-*</code> policy.</p><p><span class="pill ok">ZADD P:ttl now+180000 key</span></p></div>
      <div class="mode"><h3>auto (default)</h3><p><code>LLMQ_KEY_EXPIRY_MODE=auto</code> reads the store's policy at start and each minute: <code>noeviction</code> &rarr; native, anything else &rarr; swept. The choice is stored in <code>P:expiry</code>.</p></div>
    </div>
  </section>

  <section>
    <div class="eyebrow">Policy matrix</div>
    <h2>What each store policy means</h2>
    <div class="scroll"><table>
      <thead><tr><th>Store policy</th><th>Mode auto picks</th><th>Can llm-queue keys be evicted?</th><th>When the store is full</th><th>Readiness (strict)</th></tr></thead>
      <tbody>
        <tr><td><code>noeviction</code></td><td>native</td><td><span class="pill ok">no</span></td><td>Writes refused (OOM): calls fail <code>queue_unavailable</code> or wait per <code>store_down</code>, loudly</td><td><span class="pill ok">ready</span></td></tr>
        <tr><td><code>volatile-lru / lfu / ttl / random</code></td><td>swept</td><td><span class="pill ok">no</span> (only the locks, health probe and debug keys, on purpose)</td><td>Your app's TTL keys are evicted; if only TTL-free keys remain, writes are refused as with noeviction</td><td><span class="pill ok">ready</span></td></tr>
        <tr><td><code>volatile-*</code> with <code>LLMQ_KEY_EXPIRY_MODE=native</code></td><td>(forced)</td><td><span class="pill bad">yes</span>: budgets, records, results</td><td>Silent loss</td><td><span class="pill bad">not ready</span></td></tr>
        <tr><td><code>allkeys-*</code></td><td>swept</td><td><span class="pill bad">yes</span>: everything, queues too</td><td>Silent loss</td><td><span class="pill bad">not ready</span></td></tr>
      </tbody>
    </table></div>
  </section>

  <section>
    <div class="eyebrow">Life of one call</div>
    <h2>Which keys a call touches, in order</h2>
    <div class="scroll"><pre class="flow">App pod (caller)                    Valkey                                         Any pod (worker)        Bedrock
llmq.converse("gpt20b", body)
 1 REGISTER  ───────────────────▶  P:{r:ID}:job, :callers, :status=waiting      (record, expiry)
 2 SEND      ───────────────────▶  P:{q:QP llm-gpt20b}:s  + P:bell:...         (stream: no expiry)
 3 CONFIRM   ───────────────────▶  job confirmed=1, lives to deadline + 30 min
 4 wait on pub/sub P:done:&lt;pod&gt;                 ◀── 5 RECEIVE  {q}:vis, :gen, :st, P:rcv:&lt;worker&gt;:gpt20b
                                                ◀── 6 PICK_TAKE  rate:DEP, {a:ACCT}:rate, ac, cb, lease, inprog:ID,
                                                                 health, sticky, wait:p|n (if refused), tf:* (fairness)
                                                                                              7 call ───────▶ model
                                                ◀── 8 SETTLE_DONE  {r:ID}:res (5 min), status=done, refund, free
                                                                   slot, ACK the message, PUBLISH P:done:&lt;pod&gt;
 9 RESULT_TAKE ─────────────────▶  read res (kept 2 s after the first read)
Reply returned to the app</pre></div>
  </section>

  <section>
    <div class="eyebrow">Settings</div>
    <h2>Settings that change lifetimes and clean-up</h2>
    <div class="scroll"><table>
      <thead><tr><th>Setting</th><th>Default</th><th>Controls</th></tr></thead>
      <tbody>
        <tr><td><code>LLMQ_KEY_EXPIRY_MODE</code></td><td>auto</td><td>native TTLs or the swept index</td></tr>
        <tr><td><code>LLMQ_KEY_EXPIRY_SWEEP_BATCH</code> / <code>_ROUNDS</code></td><td>500 / 20</td><td>Keys deleted per sweep run / full batches per sweep</td></tr>
        <tr><td><code>LLMQ_RESULT_TTL_SECS</code></td><td>300</td><td>How long a result waits to be read</td></tr>
        <tr><td><code>LLMQ_RESULT_DEDUPE_SECS</code></td><td>0</td><td>Keep a result after its first read (else 2 s)</td></tr>
        <tr><td><code>LLMQ_STATUS_TTL_SECS</code></td><td>1800</td><td>Request status, cancel flags, finished records</td></tr>
        <tr><td><code>LLMQ_CALLER_TTL_SECS</code></td><td>30 (45 effective)</td><td>Caller process liveness</td></tr>
        <tr><td><code>LLMQ_LEASE_SECS</code></td><td>60</td><td>Running-call leases; renewed every 20 s</td></tr>
        <tr><td><code>LLMQ_AUDIT_RETENTION_DAYS</code></td><td>90</td><td>Registry audit stream (also capped near 10,000)</td></tr>
        <tr><td><code>LLMQ_PURGE_AUTO_ENABLED</code> / <code>LLMQ_PURGE_GRACE_SECS</code></td><td>on / 900</td><td>Delete a removed model's or deployment's keys after the grace</td></tr>
        <tr><td><code>LLMQ_PURGE_WAIT_ENABLED</code> / <code>_WAIT_SECS</code></td><td>true / 300</td><td>A deployment purge waits for its running calls</td></tr>
        <tr><td><code>LLMQ_REQUESTS_INDEX_ENABLED</code> / <code>LLMQ_PURGE_TENANT_REMOVE</code></td><td>true / keys</td><td>Request index per model and tenant (needed to find a tenant's calls) / what a tenant purge removes</td></tr>
        <tr><td>Registry <code>queue.retention_secs</code>, <code>dlq_retention_secs</code></td><td>86400</td><td>Queue stream and dead-letter trimming</td></tr>
        <tr><td>Registry <code>sticky.idle_ttl_secs</code></td><td>1800</td><td>Session-to-deployment stickiness</td></tr>
      </tbody>
    </table></div>
  </section>

  <section>
    <div class="eyebrow">Catalogue</div>
    <h2>Every key</h2>
    <p class="prose">Open a row for its fields and life. The "if evicted" line describes a store that evicts it, which in 0.1.0b3 only happens in native mode on an evicting policy.</p>
    <div class="controls">
      <input id="q" type="search" placeholder="Search keys, fields, scripts..." aria-label="Search keys">
      <span class="count" id="count"></span>
    </div>
    <div class="controls" id="groups" role="group" aria-label="Filter by group"></div>
    <div class="controls" id="modes" role="group" aria-label="Filter by swept behaviour"></div>
    <div class="keys" id="keys"></div>
  </section>
</main>

<script>
const ROWS = __DATA__;
const esc = s => String(s).replace(/[&<>"]/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));
const md = s => esc(s).replace(/`([^`]+)`/g, "<code>$1</code>").replace(/\*\*([^*]+)\*\*/g, "<b>$1</b>");
const kind = r => r.swept.startsWith("Indexed") ? ["indexed", "ok", "indexed in P:ttl"]
  : r.swept.startsWith("Native") ? ["native", "warn", "native on purpose"]
  : r.swept.startsWith("Not stored") ? ["none", "plain", "not stored"] : ["none", "plain", "no expiry"];
const groups = ["All", ...new Set(ROWS.map(r => r.group))];
const modes = [["all", "Any expiry"], ["indexed", "Indexed in P:ttl"], ["native", "Native on purpose"], ["none", "No expiry"]];
let group = "All", mode = "all";
function chips(el, items, get, set) {
  el.innerHTML = "";
  for (const [value, label] of items) {
    const b = document.createElement("button");
    b.className = "chip"; b.type = "button"; b.textContent = label;
    b.setAttribute("aria-pressed", String(get() === value));
    b.onclick = () => { set(value); render(); };
    el.append(b);
  }
}
const FIELDS = [["Schema", "schema"], ["Store", "store"], ["Written by", "written_by"], ["Read by", "read_by"],
  ["Lifetime", "lifetime"], ["Swept mode", "swept"], ["Count / size", "count"],
  ["If evicted (volatile-*)", "evicted_volatile"], ["If evicted (allkeys-*)", "evicted_allkeys"], ["Deleted by", "deleted_by"]];
function render() {
  chips(document.getElementById("groups"), groups.map(g => [g, g]), () => group, v => group = v);
  chips(document.getElementById("modes"), modes, () => mode, v => mode = v);
  const q = document.getElementById("q").value.trim().toLowerCase();
  const shown = ROWS.filter(r => (group === "All" || r.group === group)
    && (mode === "all" || kind(r)[0] === mode)
    && (!q || JSON.stringify(r).toLowerCase().includes(q)));
  document.getElementById("count").textContent = `${shown.length} of ${ROWS.length} keys`;
  document.getElementById("keys").innerHTML = shown.map(r => {
    const [, cls, label] = kind(r);
    return `<details class="key"><summary>
      <span><span class="kname">${esc(r.key)}</span><span class="kgroup">${esc(r.group)}</span></span>
      <span class="ktype">${esc(r.type)}</span>
      <span class="klife">${md(r.lifetime)}</span>
      <span><span class="pill ${cls}">${label}</span></span></summary>
      <dl class="body">${FIELDS.map(([l, k]) => `<dt>${l}</dt><dd>${md(r[k])}</dd>`).join("")}</dl></details>`;
  }).join("") || `<p class="count">No key matches. Clear the search or pick another group.</p>`;
}
document.getElementById("q").addEventListener("input", render);
render();
</script>
"""


def build(rows: list[dict[str, str]], version: str) -> str:
    """The page for `rows` (store-keys.json entries)."""
    data = json.dumps(rows).replace("</", r"<\/")  # no </script> inside the data
    return PAGE.replace("__DATA__", data).replace("__VERSION__", version)


def main() -> int:
    """Write the page; print its path and size."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--src", type=Path, default=ROOT / "docs/reference/store-keys.json")
    parser.add_argument("--out", type=Path, default=ROOT / "reports/store-keys.html")
    args = parser.parse_args()
    text = (ROOT / "src/llm_queue/_version.py").read_text(encoding="utf-8")
    match = re.search(r'__version__ = "([^"]+)"', text)
    rows = json.loads(args.src.read_text(encoding="utf-8"))
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(build(rows, match.group(1) if match else "?"), encoding="utf-8")
    print(args.out, args.out.stat().st_size)
    return 0


if __name__ == "__main__":
    sys.exit(main())
