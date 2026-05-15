#!/usr/bin/env node
import BaseWorker from './BaseWorker.js';

async function readStdin() {
  return await new Promise((resolve) => {
    let data = '';
    process.stdin.setEncoding('utf8');
    process.stdin.on('data', chunk => data += chunk);
    process.stdin.on('end', () => resolve(data));
  });
}

async function main() {
  const input = await readStdin();
  const event = JSON.parse(input || '{}');
  const w = new BaseWorker();
  await w.processEvent(event);
  process.exit(0);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
