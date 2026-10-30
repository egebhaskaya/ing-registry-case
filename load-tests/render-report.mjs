#!/usr/bin/env node
// Rebuild the single report from all JSON summaries; existing JSON is never modified.
import { readFile, readdir, stat, writeFile, rename } from 'node:fs/promises';
import { resolve, dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { renderReports } from './report.mjs';

const results = resolve(process.argv[2] || join(dirname(fileURLToPath(import.meta.url)), 'results'));
const entries = [];
const unreadable = [];
for (const file of await readdir(results, { withFileTypes: true })) {
  if (!file.isFile() || !file.name.endsWith('.summary.json')) continue;
  try {
    const path = join(results, file.name);
    const data = JSON.parse(await readFile(path, 'utf8'));
    if (!data?.metrics || typeof data.metrics !== 'object') throw new Error('Not a k6 summary');
    entries.push({ data, fileName: file.name, modified: (await stat(path)).mtimeMs });
  } catch (error) {
    unreadable.push(`${file.name}: ${error.message}`);
  }
}
entries.sort((a, b) => b.modified - a.modified || b.fileName.localeCompare(a.fileName));
const output = join(results, 'index.html');
const temporary = join(results, `.report-${process.pid}.tmp`);
await writeFile(temporary, renderReports(entries, unreadable), 'utf8');
await rename(temporary, output);
console.log(`report: ${output} (${entries.length} runs)`);
if (unreadable.length) console.warn(`${unreadable.length} unreadable summaries are listed in the report.`);
