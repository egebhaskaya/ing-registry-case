// k6 load scenario for the registration burst (docs/design.md section 7).
//
// Exactly one registration submission per iteration, 10,000 iterations in total shared among the
// virtual users ("shared-iterations"): this bounds client concurrency (VUS) and the total count, and
// it is a closed loop: a slower server lowers the send rate. It is NOT a fixed arrival-rate benchmark.
//
// Environment:  BASE_URL (default http://localhost:8080), RUN_ID, VUS (50), ITERATIONS (10000),
//               MAX_DURATION (10m). Run through load-tests/run-k6.sh.

import http from 'k6/http';
import { check } from 'k6';
import exec from 'k6/execution';
import { Counter } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const RUN_ID = __ENV.RUN_ID || `run-${Date.now()}`;
const RUN_HEX = fnv1a32Hex(RUN_ID);

export const options = {
  scenarios: {
    burst: {
      executor: 'shared-iterations',
      vus: Number(__ENV.VUS || 50),
      iterations: Number(__ENV.ITERATIONS || 10000),
      maxDuration: __ENV.MAX_DURATION || '10m',
    },
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  // No thresholds on purpose: the run reports, it does not pass or fail on a target we did not measure yet.
};

const accepted = new Counter('registrations_accepted');
const rejected503 = new Counter('registrations_rejected_503');
const other = new Counter('registrations_other');

export default function () {
  // iterationInTest is unique across all VUs for the whole run: it makes key and email unique.
  const n = exec.scenario.iterationInTest;
  const key = `${RUN_HEX}-0000-4000-8000-${n.toString(16).padStart(12, '0')}`;
  const body = JSON.stringify({
    fullName: `Load Test ${n}`,
    email: `load-${RUN_ID}-${n}@example.com`,
    countryCode: 'NL',
  });

  const res = http.post(`${BASE_URL}/registrations`, body, {
    headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key },
    tags: { name: 'POST /registrations' },
  });

  const ok = check(res, { 'status is 202': (r) => r.status === 202 });
  if (ok) {
    accepted.add(1);
  } else if (res.status === 503) {
    rejected503.add(1); // overloaded / broker rejected / confirm timeout: see the reason in the body
  } else {
    other.add(1);
  }
  // No retries here (design section 7): the primary run keeps its counts clean.
}

// Writes the full summary as JSON for the experiment script and prints a short text summary.
export function handleSummary(data) {
  const m = data.metrics;
  const val = (name, stat) => (m[name] && m[name].values && m[name].values[stat] !== undefined ? m[name].values[stat] : 'n/a');
  const lines = [
    `run id:            ${RUN_ID}`,
    `iterations:        ${val('iterations', 'count')} (wanted ${Number(__ENV.ITERATIONS || 10000)})`,
    `accepted (202):    ${val('registrations_accepted', 'count')}`,
    `rejected (503):    ${val('registrations_rejected_503', 'count')}`,
    `other:             ${val('registrations_other', 'count')}`,
    `http p50 / p95 ms: ${fmt(val('http_req_duration', 'med'))} / ${fmt(val('http_req_duration', 'p(95)'))}`,
    `submission took:   ${fmt(data.state.testRunDurationMs / 1000)} s`,
  ];
  return {
    stdout: lines.join('\n') + '\n',
    [`/results/${RUN_ID}.summary.json`]: JSON.stringify(data, null, 2),
  };
}

function fmt(x) {
  return typeof x === 'number' ? x.toFixed(1) : String(x);
}

// FNV-1a: turns the run id into 8 hex characters so the Idempotency-Key is UUID-shaped and unique per run.
function fnv1a32Hex(s) {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  return h.toString(16).padStart(8, '0');
}
