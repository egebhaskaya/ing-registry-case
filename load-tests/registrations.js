// One registration per iteration: a fixed 10,000-request burst by default, or a fixed arrival rate.
// Continuous mode runs until Ctrl+C or its finite DURATION ceiling; dropped iterations remain visible.
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
const CONTINUOUS_FLAG = __ENV.CONTINUOUS === undefined ? '0' : __ENV.CONTINUOUS;
if (CONTINUOUS_FLAG !== '0' && CONTINUOUS_FLAG !== '1') throw new Error('CONTINUOUS must be 0 or 1');
const CONTINUOUS = CONTINUOUS_FLAG === '1';
const VUS = positiveInteger('VUS', 50);
const RATE = positiveInteger('RATE', 100);
const MAX_VUS = positiveInteger('MAX_VUS', 200);
if (CONTINUOUS && MAX_VUS < VUS) throw new Error('MAX_VUS must be at least VUS in continuous mode');
const ITERATIONS = Number(__ENV.ITERATIONS || 10000);
const BURST_DURATION = __ENV.MAX_DURATION || '10m';
const CONTINUOUS_DURATION = __ENV.DURATION || '8760h';

export const options = {
  scenarios: CONTINUOUS ? {
    continuous: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: CONTINUOUS_DURATION,
      preAllocatedVUs: VUS,
      maxVUs: MAX_VUS,
      gracefulStop: '30s',
    },
  } : {
    burst: {
      executor: 'shared-iterations',
      vus: VUS,
      iterations: ITERATIONS,
      maxDuration: BURST_DURATION,
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
  const interrupted = data.state && data.state.isInterrupted;
  const summary = { ...data, run: {
    id: RUN_ID,
    mode: MODE,
    endpoint: PATH,
    scenario: CONTINUOUS ? 'continuous' : 'burst',
    requestedIterations: CONTINUOUS ? null : ITERATIONS,
    virtualUsers: VUS,
    requestedRate: CONTINUOUS ? RATE : null,
    maxVirtualUsers: CONTINUOUS ? MAX_VUS : VUS,
    plannedDuration: CONTINUOUS ? CONTINUOUS_DURATION : BURST_DURATION,
    ...(typeof interrupted === 'boolean' ? { interrupted } : {}),
    finishedAt: new Date().toISOString(),
  } };
  const m = data.metrics;
  const val = (name, stat) => (m[name] && m[name].values && m[name].values[stat] !== undefined ? m[name].values[stat] : 'n/a');
  const count = (name) => (m[name] && m[name].values ? m[name].values.count : 0);
  const lines = [
    `run id:            ${RUN_ID}`,
    `mode:              ${MODE} (${MODE === 'sync' ? 'commit before response' : 'publisher-confirmed acceptance'})`,
    `scenario:          ${CONTINUOUS ? 'continuous fixed arrival rate; stop with Ctrl+C' : 'burst (shared iterations)'}`,
    ...(CONTINUOUS ? [
      `target rate:       ${RATE} iterations/s (one POST each)`,
      `VU baseline / max: ${VUS} / ${MAX_VUS}`,
      `duration ceiling:  ${CONTINUOUS_DURATION}`,
      `iterations:        ${val('iterations', 'count')} completed`,
      `dropped iterations: ${count('dropped_iterations')}`,
    ] : [`iterations:        ${val('iterations', 'count')} (wanted ${ITERATIONS})`]),
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
    `observed HTTP /s:  ${fmt(val('http_reqs', 'rate'))}`,
  ];
  return {
    stdout: lines.join('\n') + '\n',
    [`${SUMMARY_DIR}/${RUN_ID}.summary.json`]: JSON.stringify(summary, null, 2),
  };
}

function fmt(x) { return typeof x === 'number' ? x.toFixed(1) : String(x); }

function positiveInteger(name, fallback) {
  const value = Number(__ENV[name] === undefined ? fallback : __ENV[name]);
  if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`${name} must be a positive integer`);
  return value;
}

// Give each run a compact, deterministic prefix for unique registration keys.
function fnv1a32Hex(s) {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  return h.toString(16).padStart(8, '0');
}
