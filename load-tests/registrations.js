// One registration per shared iteration. VUS bounds closed-loop client concurrency;
// Rabbit consumer concurrency and the database pool bound server processing.
// MODE=async measures publisher-confirmed acceptance. MODE=sync measures commit before response.
// No retries: rejected requests stay visible in the primary benchmark.
import http from 'k6/http';
import { check } from 'k6';
import exec from 'k6/execution';
import { Counter, Trend } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const RUN_ID = __ENV.RUN_ID || `run-${Date.now()}`;
const RUN_HEX = fnv1a32Hex(RUN_ID);
const MODE = __ENV.MODE || 'async';
const SUMMARY_DIR = __ENV.SUMMARY_DIR || '/results';
if (MODE !== 'async' && MODE !== 'sync') throw new Error('MODE must be async or sync');
const PATH = MODE === 'sync' ? '/registrations/sync' : '/registrations';

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
  // Report measurements without inventing an unmeasured throughput or latency threshold.
};

const accepted = new Counter('registrations_accepted');
const created = new Counter('registrations_created');
const replayed = new Counter('registrations_replayed');
const conflicts = new Counter('registrations_conflicts');
const rejected503 = new Counter('registrations_rejected_503');
const other = new Counter('registrations_other');
const invalidResponse = new Counter('registrations_invalid_response');
const successLatency = new Trend('registration_success_latency', true);

export default function () {
  const n = exec.scenario.iterationInTest;
  const key = `${RUN_HEX}-0000-4000-8000-${n.toString(16).padStart(12, '0')}`;
  const email = `load-${RUN_ID}-${n}@example.com`;
  const body = JSON.stringify({ fullName: `Load Test ${n}`, email, countryCode: 'NL' });
  const res = http.post(`${BASE_URL}${PATH}`, body, {
    headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key },
    tags: { name: `POST ${PATH}` },
  });

  // These categories are mutually exclusive and sum to the number of HTTP attempts.
  switch (res.status) {
    case 201: created.add(1); break;
    case 200: replayed.add(1); break;
    case 202: accepted.add(1); break;
    case 409: conflicts.add(1); break;
    case 503: rejected503.add(1); break;
    default: other.add(1);
  }

  if (MODE === 'async') {
    if (check(res, { 'status is 202': (r) => r.status === 202 })) {
      successLatency.add(res.timings.duration);
    }
  } else {
    const successfulStatus = res.status === 201 || res.status === 200;
    check(res, { 'status confirms a committed registration': () => successfulStatus });
    if (successfulStatus) {
      let result;
      try { result = res.json(); } catch (_) { result = null; }
      const validBody = check(result, {
        'sync response contains the committed user': (value) => value !== null
          && typeof value.userId === 'string'
          && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value.userId)
          && value.registrationId === key && value.status === 'REGISTERED'
          && value.email === email.toLowerCase(),
      });
      if (validBody) successLatency.add(res.timings.duration);
      else invalidResponse.add(1);
    }
  }
}

export function handleSummary(data) {
  const summary = { ...data, run: {
    id: RUN_ID,
    mode: MODE,
    endpoint: PATH,
    requestedIterations: Number(__ENV.ITERATIONS || 10000),
    virtualUsers: Number(__ENV.VUS || 50),
    finishedAt: new Date().toISOString(),
  } };
  const m = data.metrics;
  const val = (name, stat) => (m[name] && m[name].values && m[name].values[stat] !== undefined ? m[name].values[stat] : 'n/a');
  const count = (name) => (m[name] && m[name].values ? m[name].values.count : 0);
  const lines = [
    `run id:            ${RUN_ID}`,
    `mode:              ${MODE} (${MODE === 'sync' ? 'commit before response' : 'publisher-confirmed acceptance'})`,
    `iterations:        ${val('iterations', 'count')} (wanted ${Number(__ENV.ITERATIONS || 10000)})`,
    `created (201):     ${count('registrations_created')}`,
    `replayed (200):    ${count('registrations_replayed')}`,
    `accepted (202):    ${count('registrations_accepted')}`,
    `conflicts (409):   ${count('registrations_conflicts')}`,
    `rejected (503):    ${count('registrations_rejected_503')}`,
    `other:             ${count('registrations_other')}`,
    `invalid bodies:    ${count('registrations_invalid_response')}`,
    `http p50 / p95 ms: ${fmt(val('http_req_duration', 'med'))} / ${fmt(val('http_req_duration', 'p(95)'))}`,
    `success p50/p95 ms: ${fmt(val('registration_success_latency', 'med'))} / ${fmt(val('registration_success_latency', 'p(95)'))}`,
    `submission took:   ${fmt(data.state.testRunDurationMs / 1000)} s`,
  ];
  return {
    stdout: lines.join('\n') + '\n',
    [`${SUMMARY_DIR}/${RUN_ID}.summary.json`]: JSON.stringify(summary, null, 2),
  };
}

function fmt(x) { return typeof x === 'number' ? x.toFixed(1) : String(x); }

// Give each run a compact, deterministic prefix for unique registration keys.
function fnv1a32Hex(s) {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  return h.toString(16).padStart(8, '0');
}
