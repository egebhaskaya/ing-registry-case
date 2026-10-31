// Build one self-contained page from the saved k6 summaries. No browser server is needed.
export function renderReports(entries, unreadable = []) {
  const runs = entries.map(entry => summarize(entry));
  const rows = runs.map(run => `<tbody>
<tr><th scope="row" class="run-id">${escape(run.id)}${run.problem ? '<br><span class="review">Needs review</span>' : ''}</th><td>${escape(run.displayMode)}</td><td>${integer(run.requests)}</td><td>${integer(run.httpFailures)}</td><td>${decimal(run.value('http_req_duration', 'p(95)'))}</td><td>${decimal(run.duration)}</td><td>${integer(run.vus)}</td></tr>
<tr class="detail-row"><td colspan="7"><details><summary>Run details</summary>
<p class="${run.problem ? 'review' : 'muted'}">${escape(run.status)}</p>
<p>${escape(run.meaning)}</p><p class="muted">${escape(run.completion)}</p>
${workloadDetails(run)}
<div class="detail-grid"><section><h2>HTTP outcomes</h2><table><thead><tr><th>Response</th><th>Requests</th></tr></thead><tbody>${run.outcomes.map(([label, amount]) => `<tr><th scope="row">${escape(label)}</th><td>${integer(amount)}</td></tr>`).join('')}</tbody></table><p class="note">Invalid successful response bodies: ${integer(run.invalidBodies)}. These are checks, not additional requests.</p></section>
<section><h2>Response times (ms)</h2><div class="scroll"><table><thead><tr><th>Responses</th><th>p50</th><th>p95</th><th>Average</th><th>Maximum</th></tr></thead><tbody>${latencyRow(run, 'All HTTP', 'http_req_duration')}${latencyRow(run, 'Successful', 'registration_success_latency')}</tbody></table></div><p class="note">p95: approximately 95% of measured responses were at or below this time.</p>
<h2>Checks</h2>${run.checks.length ? `<table><thead><tr><th>Check</th><th>Passed</th><th>Failed</th></tr></thead><tbody>${run.checks.map(check => `<tr><th scope="row">${escape(check.name)}</th><td>${integer(check.passes)}</td><td>${integer(check.fails)}</td></tr>`).join('')}</tbody></table>` : '<p class="muted">No check results recorded.</p>'}<p class="note">One request can run multiple checks. No performance thresholds are defined.</p></section></div>
<details class="raw"><summary>Full JSON summary</summary><pre>${escape(JSON.stringify(run.data, null, 2))}</pre></details>
</details></td></tr></tbody>`).join('');

  return `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Load-test runs</title>
<style>
:root{color-scheme:light dark;--bg:light-dark(#f5f6f8,#15181d);--surface:light-dark(#fff,#20252d);--text:light-dark(#17202b,#eef1f5);--muted:light-dark(#536172,#b0bbc9);--line:light-dark(#dce1e8,#39424f);--accent:light-dark(#145b49,#8cdbc1);--error:light-dark(#a52b25,#ffb0a8)}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:16px/1.55 system-ui,sans-serif}main{max-width:1280px;margin:40px auto;padding:0 24px 40px}h1{font-size:30px;line-height:1.2;margin:8px 0}h2{font-size:17px;margin:20px 0 10px}p{margin:10px 0}.muted,.note{color:var(--muted)}.note{font-size:14px}.review{color:var(--error);font-weight:600}.scroll{overflow-x:auto}.runs{background:var(--surface);border:1px solid var(--line);border-radius:10px;margin-top:24px;overflow-x:auto}table{border-collapse:collapse;width:100%;font-variant-numeric:tabular-nums}th,td{padding:14px 12px;text-align:right;vertical-align:top}th:first-child{text-align:left}thead th{font-size:13px;color:var(--muted);white-space:nowrap;border-bottom:1px solid var(--line)}tbody th{font-weight:400}.run-id{max-width:360px;overflow-wrap:anywhere;min-width:210px}.runs>table>tbody>tr:first-child>td{white-space:nowrap}.detail-row>td{text-align:left;padding:0 12px 12px;border-bottom:1px solid var(--line)}.runs>table>tbody:last-child>.detail-row>td{border-bottom:0}summary{color:var(--accent);cursor:pointer;width:fit-content;padding:5px 0}summary:focus-visible{outline:2px solid var(--accent);outline-offset:3px}.detail-grid{display:grid;grid-template-columns:1fr 1.4fr;gap:32px}.detail-grid td,.detail-grid th{padding:8px 6px;border-bottom:1px solid var(--line)}.detail-grid tbody th{overflow-wrap:anywhere}.raw{margin-top:12px}pre{max-height:420px;overflow:auto;padding:16px;background:var(--bg);font:13px/1.5 ui-monospace,monospace}.empty{padding:24px}.intro{max-width:880px}@media(max-width:800px){main{margin:24px auto;padding:0 16px 24px}.detail-grid{grid-template-columns:1fr;gap:0}th,td{padding:12px 8px}.run-id{min-width:180px}}@media print{body{background:white;color:black}.runs{border:0}summary{color:black}}
</style></head><body><main>
<p class="muted">LOCAL LOAD TESTS</p><h1>All runs</h1>
<div class="intro"><p>${integer(runs.length)} saved ${runs.length === 1 ? 'run' : 'runs'}, newest first. Each load-test run updates this page. Refresh this browser tab after a run finishes.</p>
<p class="note">Async and batched measure HTTP acceptance into RabbitMQ; sync waits for Registry commit. Batched groups queued registrations into database transactions. These summaries do not measure Compliance completion or batch commit time. Burst runs use clients that wait for responses; continuous runs schedule new requests at a target arrival rate. Actual achieved rates can differ from the target.</p></div>
${unreadable.length ? `<details class="review"><summary>${integer(unreadable.length)} summaries could not be read</summary><ul>${unreadable.map(file => `<li>${escape(file)}</li>`).join('')}</ul></details>` : ''}
<div class="runs">${runs.length ? `<table aria-label="Load-test run comparison"><thead><tr><th>Run</th><th>Mode</th><th>Requests</th><th>HTTP failures</th><th>HTTP p95 (ms)</th><th>Duration (s)</th><th title="Burst clients or preallocated continuous VUs; see details for observed maximum">VUs</th></tr></thead>${rows}</table>` : '<p class="empty">No runs yet. Start the services and run make k6, make k6-sync or make k6-batched.</p>'}</div>
</main></body></html>`;
}

function summarize({ data, fileName }) {
  const value = (name, stat) => number(data.metrics[name]?.values?.[stat]);
  const count = name => value(name, 'count') ?? 0;
  const id = data.run?.id || fileName.replace(/\.summary\.json$/, '');
  const accepted = count('registrations_accepted'), created = count('registrations_created'), replayed = count('registrations_replayed');
  const mode = data.run?.mode || /^(async|sync|batched)-/.exec(id)?.[1] || (accepted > 0 ? 'async' : created + replayed > 0 ? 'sync' : 'unknown');
  const requests = value('http_reqs', 'count') ?? value('iterations', 'count') ?? 0;
  const iterations = value('iterations', 'count') ?? 0;
  const requested = number(data.run?.requestedIterations);
  const continuous = data.run?.scenario === 'continuous';
  const scenario = continuous ? 'continuous' : data.run?.scenario === 'burst' || requested !== null ? 'burst' : 'legacy';
  const requestedRate = number(data.run?.requestedRate);
  const droppedIterations = count('dropped_iterations');
  const interrupted = typeof data.run?.interrupted === 'boolean' ? data.run.interrupted : null;
  const exitCode = number(data.run?.exitCode);
  const stoppedBySignal = continuous && interrupted === true && [105, 129, 130, 143].includes(exitCode);
  const unexpectedExit = exitCode !== null && exitCode !== 0 && !stoppedBySignal;
  const incomplete = !continuous && requested !== null && iterations !== requested;
  const checks = collectChecks(data.root_group);
  const failedChecks = checks.reduce((sum, check) => sum + (number(check.fails) ?? 0), 0);
  // k6's failure Rate counts true "request failed" samples as passes, not fails.
  const httpFailures = value('http_req_failed', 'passes');
  const invalidBodies = count('registrations_invalid_response');
  const problem = httpFailures > 0 || failedChecks > 0 || invalidBodies > 0 || droppedIterations > 0 || incomplete || (!continuous && interrupted === true) || unexpectedExit;
  const knownOutcomes = accepted + created + replayed + count('registrations_conflicts') + count('registrations_rejected_503');
  return { data, id, mode, requests, value, checks, httpFailures, invalidBodies, problem,
    continuous, scenario, requestedRate, droppedIterations, interrupted, exitCode, stoppedBySignal,
    displayMode: continuous ? `${mode} / continuous` : mode,
    preallocatedVus: number(data.run?.virtualUsers),
    maxVus: number(data.run?.maxVirtualUsers),
    observedVus: value('vus', 'max'),
    plannedDuration: data.run?.plannedDuration,
    duration: number(data.state?.testRunDurationMs) === null ? null : data.state.testRunDurationMs / 1000,
    vus: number(data.run?.virtualUsers) ?? value('vus_max', 'max') ?? value('vus', 'max'),
    status: problem ? 'Review errors, dropped iterations, incomplete burst or k6 exit status below.' : requests === 0 ? 'No requests recorded.' : httpFailures === null ? 'HTTP failure count was not recorded.' : 'No HTTP, check or dropped-iteration failures recorded.',
    completion: continuous ? `${integer(iterations)} iterations completed during the recorded run; continuous runs have no fixed iteration target.` : requested === null ? 'Requested iteration count was not recorded in this older summary.' : `${integer(iterations)} of ${integer(requested)} requested iterations completed${iterations === requested ? '.' : ' — incomplete run.'}`,
    meaning: mode === 'async' ? 'Async: a successful HTTP response means RabbitMQ accepted the command; processing can continue afterward.' : mode === 'batched' ? 'Batched: a successful HTTP response means RabbitMQ accepted one registration into the batch queue. Grouping and database commit happen afterward; this HTTP timing excludes both.' : mode === 'sync' ? 'Sync: a successful HTTP response follows the Registry database commit. Compliance continues separately.' : 'Endpoint mode was not recorded in this summary.',
    outcomes: [['202 Accepted', accepted], ['201 Created', created], ['200 Replayed', replayed], ['409 Conflict', count('registrations_conflicts')], ['503 Unavailable', count('registrations_rejected_503')], ['Other responses / transport errors', value('registrations_other', 'count') ?? Math.max(requests - knownOutcomes, 0)]],
  };
}

function workloadDetails(run) {
  const workload = run.continuous ? 'Continuous arrival rate' : run.scenario === 'burst' ? 'Shared-iteration burst' : 'Not recorded (older summary)';
  const target = run.continuous ? decimal(run.requestedRate) : run.scenario === 'burst' ? 'Not fixed (closed-loop)' : 'Not recorded';
  const vus = run.continuous
    ? `Preallocated VUs: ${integer(run.preallocatedVus)}; configured ceiling: ${integer(run.maxVus)}; observed VUs (sampled maximum): ${integer(run.observedVus)}.`
    : `Client VUs: ${integer(run.vus)}.`;
  const stop = run.stoppedBySignal ? 'Stopped by signal.'
    : run.interrupted === true ? 'k6 recorded an interruption; its cause is not identified here.'
    : run.interrupted === false ? 'k6 did not record an interruption.' : 'Interruption status was not recorded.';
  const duration = run.continuous ? ` Duration ceiling: ${escape(run.plannedDuration ?? 'Not recorded')}.` : '';
  const termination = run.continuous || run.interrupted !== null ? ` ${escape(stop)}` : '';
  const exit = run.exitCode === null ? '' : ` k6 exit code: ${integer(run.exitCode)}.`;
  return `<p><strong>Workload:</strong> ${escape(workload)}. <strong>Target starts/s:</strong> ${escape(target)}. <strong>Actual HTTP requests/s:</strong> ${decimal(run.value('http_reqs', 'rate'))}. <strong>Dropped iterations:</strong> ${integer(run.droppedIterations)}.</p>
<p class="muted">${escape(vus)}${duration}${termination}${exit}</p>
${run.continuous || run.droppedIterations > 0 ? `<p class="note">Dropped iterations are starts k6 could not schedule, not failed HTTP responses.${run.continuous ? ' Preallocated VUs are a starting allocation, not fixed request concurrency.' : ''}</p>` : ''}`;
}

function latencyRow(run, label, metric) {
  return `<tr><th scope="row">${escape(label)}</th>${['med', 'p(95)', 'avg', 'max'].map(stat => `<td>${decimal(run.value(metric, stat))}</td>`).join('')}</tr>`;
}
function number(value) { return typeof value === 'number' && Number.isFinite(value) ? value : null; }
function decimal(value) { return number(value) === null ? '—' : value.toFixed(1); }
function integer(value) { return number(value) === null ? '—' : Math.round(value).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ','); }
function escape(value) { return String(value).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c]); }
function collectChecks(group) {
  if (!group || typeof group !== 'object') return [];
  const checks = Object.values(group.checks || {}).filter(check => check && typeof check === 'object');
  return [...checks, ...Object.values(group.groups || {}).flatMap(collectChecks)];
}
