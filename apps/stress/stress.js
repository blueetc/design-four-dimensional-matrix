#!/usr/bin/env node
// @ts-check
'use strict';

const fs = require('fs');
const os = require('os');
const path = require('path');
const http = require('http');
const assert = require('node:assert/strict');
const { spawn } = require('child_process');

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function cleanupDirectory(dirPath) {
  try {
    fs.rmSync(dirPath, { recursive: true, force: true });
  } catch {
    // Ignore cleanup errors.
  }
}

function quoteShellValue(value) {
  return `"${String(value).replace(/"/g, '\\"')}"`;
}

function clearModule(modulePath) {
  try {
    delete require.cache[require.resolve(modulePath)];
  } catch {
    // Ignore missing cache entries.
  }
}

async function getFreePort() {
  return new Promise((resolve, reject) => {
    const server = http.createServer();
    server.on('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const address = server.address();
      const port = typeof address === 'object' && address ? address.port : 0;
      server.close((err) => {
        if (err) {
          reject(err);
          return;
        }
        resolve(port);
      });
    });
  });
}

function httpPostJson(port, requestPath, payload, timeoutMs = 3000) {
  return new Promise((resolve, reject) => {
    const body = JSON.stringify(payload || {});
    const req = http.request({
      hostname: '127.0.0.1',
      port,
      path: requestPath,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(body),
      },
      timeout: timeoutMs,
    }, (res) => {
      let data = '';
      res.setEncoding('utf8');
      res.on('data', (chunk) => {
        data += chunk;
      });
      res.on('end', () => {
        resolve({ statusCode: res.statusCode || 0, body: data });
      });
    });

    req.on('timeout', () => {
      req.destroy(new Error(`POST ${requestPath} timed out`));
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

function createSseClient(port) {
  return new Promise((resolve, reject) => {
    const req = http.request({
      hostname: '127.0.0.1',
      port,
      path: '/api/events',
      method: 'GET',
      headers: {
        Accept: 'text/event-stream',
      },
    });

    req.on('error', reject);

    req.on('response', (res) => {
      if (res.statusCode !== 200) {
        reject(new Error(`Unexpected SSE status: ${res.statusCode}`));
        req.destroy();
        return;
      }

      res.setEncoding('utf8');
      let buffer = '';
      const waiters = new Set();
      let closed = false;

      const failAll = (error) => {
        for (const waiter of waiters) {
          clearTimeout(waiter.timeoutId);
          waiter.reject(error);
        }
        waiters.clear();
      };

      const maybeResolveWaiters = () => {
        for (const waiter of Array.from(waiters)) {
          if (buffer.includes(waiter.needle)) {
            clearTimeout(waiter.timeoutId);
            waiters.delete(waiter);
            waiter.resolve();
          }
        }
      };

      res.on('data', (chunk) => {
        buffer += chunk;
        maybeResolveWaiters();
      });

      res.on('error', (err) => {
        if (closed) return;
        closed = true;
        failAll(err);
      });

      res.on('close', () => {
        if (closed) return;
        closed = true;
        failAll(new Error('SSE connection closed'));
      });

      const client = {
        waitFor(needle, timeoutMs = 3000) {
          if (buffer.includes(needle)) return Promise.resolve();
          return new Promise((resolveWait, rejectWait) => {
            const waiter = {
              needle,
              resolve: resolveWait,
              reject: rejectWait,
              timeoutId: setTimeout(() => {
                waiters.delete(waiter);
                rejectWait(new Error(`Timed out waiting for SSE payload: ${needle}`));
              }, timeoutMs),
            };
            waiters.add(waiter);
          });
        },
        close() {
          if (closed) return;
          closed = true;
          failAll(new Error('SSE client closed'));
          req.destroy();
          res.destroy();
        },
      };

      client.waitFor(':connected', 3000)
        .then(() => resolve(client))
        .catch((error) => {
          client.close();
          reject(error);
        });
    });

    req.end();
  });
}

async function waitForServerReady(port, timeoutMs = 12000) {
  const start = Date.now();
  let lastError = null;

  while (Date.now() - start < timeoutMs) {
    try {
      const response = await new Promise((resolve, reject) => {
        const req = http.request({
          hostname: '127.0.0.1',
          port,
          path: '/',
          method: 'GET',
          timeout: 1000,
        }, (res) => {
          res.resume();
          resolve(res.statusCode || 0);
        });
        req.on('timeout', () => req.destroy(new Error('health check timeout')));
        req.on('error', reject);
        req.end();
      });

      if (response > 0) return;
    } catch (error) {
      lastError = error;
    }

    await sleep(150);
  }

  throw new Error(`Server did not become ready in ${timeoutMs}ms${lastError ? `: ${lastError.message}` : ''}`);
}

function startServerProcess(repoRoot, workspaceRoot, port) {
  const serverPath = path.join(repoRoot, 'apps', 'server.js');
  const child = spawn(process.execPath, [serverPath], {
    cwd: repoRoot,
    env: {
      ...process.env,
      PORT: String(port),
      CLOUDWBOT_WORKSPACE: workspaceRoot,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  let stdout = '';
  let stderr = '';
  child.stdout.on('data', (chunk) => {
    stdout += chunk.toString();
    if (stdout.length > 4000) stdout = stdout.slice(-4000);
  });
  child.stderr.on('data', (chunk) => {
    stderr += chunk.toString();
    if (stderr.length > 4000) stderr = stderr.slice(-4000);
  });

  return {
    child,
    getLogs() {
      return { stdout, stderr };
    },
  };
}

async function stopServerProcess(child) {
  if (!child || child.exitCode !== null) return;

  await new Promise((resolve) => {
    const onExit = () => resolve();
    child.once('exit', onExit);
    child.kill('SIGTERM');
    setTimeout(() => {
      if (child.exitCode === null) {
        child.kill('SIGKILL');
      }
    }, 1500);
  });
}

function createTestReport(name) {
  return {
    name,
    startedAt: Date.now(),
    finishedAt: 0,
    durationMs: 0,
    details: {},
  };
}

function finalizeTestReport(report) {
  report.finishedAt = Date.now();
  report.durationMs = report.finishedAt - report.startedAt;
  return report;
}

async function runTaskLimitStress(tempWorkspaceRoot) {
  const report = createTestReport('任务数量限制');

  process.env.CLOUDWBOT_WORKSPACE = tempWorkspaceRoot;
  clearModule('../lib/config');
  clearModule('../lib/taskManager');

  const config = require('../lib/config');
  const taskManager = require('../lib/taskManager');

  const requestedTasks = config.TASK_MAX_COUNT + 40;
  const baseTime = Date.now() - requestedTasks;

  for (let i = 0; i < requestedTasks; i += 1) {
    const { taskRun } = taskManager.createTaskRun(`stress-task-${i + 1}`);
    taskRun.status = i % 2 === 0 ? 'completed' : 'failed';
    taskRun.createdAt = new Date(baseTime + i).toISOString();
    taskRun.summary = `summary-${i + 1}`;
    taskManager.flushTaskRunSave(taskRun);
  }

  const visibleTasks = taskManager.listKnownTaskRuns();

  assert.strictEqual(
    visibleTasks.length,
    config.TASK_MAX_COUNT,
    `Expected ${config.TASK_MAX_COUNT} visible tasks, got ${visibleTasks.length}`,
  );

  assert.ok(
    taskManager.taskRuns.size <= config.TASK_MAX_COUNT,
    `Expected in-memory task map <= ${config.TASK_MAX_COUNT}, got ${taskManager.taskRuns.size}`,
  );

  report.details = {
    taskMaxCount: config.TASK_MAX_COUNT,
    generatedTaskCount: requestedTasks,
    visibleTaskCount: visibleTasks.length,
    inMemoryTaskCount: taskManager.taskRuns.size,
  };

  return finalizeTestReport(report);
}

async function runTimeoutStress(repoRoot) {
  const report = createTestReport('任务超时控制');
  const { runShellCommand } = require('../lib/executor');

  const timeoutMs = 200;
  const concurrentRuns = 8;
  const shellCommand = `${quoteShellValue(process.execPath)} -e "setTimeout(() => {}, 5000)"`;

  const results = await Promise.all(
    Array.from({ length: concurrentRuns }, (_, index) => {
      return runShellCommand(shellCommand, repoRoot, `timeout-${index + 1}`, null, timeoutMs);
    }),
  );

  const timedOutCount = results.filter((result) => result.timedOut).length;
  const successfulCount = results.filter((result) => result.success).length;

  assert.strictEqual(
    timedOutCount,
    concurrentRuns,
    `Expected all runs to time out (${concurrentRuns}), got ${timedOutCount}`,
  );

  assert.strictEqual(successfulCount, 0, `Expected no successful runs, got ${successfulCount}`);

  report.details = {
    timeoutMs,
    concurrentRuns,
    timedOutCount,
    successfulCount,
  };

  return finalizeTestReport(report);
}

async function runSseReconnectStress(repoRoot, tempWorkspaceRoot) {
  const report = createTestReport('SSE 重连稳定性');

  const port = await getFreePort();
  const { child, getLogs } = startServerProcess(repoRoot, tempWorkspaceRoot, port);

  try {
    await waitForServerReady(port, 12000);

    const reconnectCycles = 8;
    for (let i = 0; i < reconnectCycles; i += 1) {
      const client = await createSseClient(port);
      try {
        const postResult = await httpPostJson(port, '/api/message', { command: 'listTasks' }, 3000);
        assert.strictEqual(postResult.statusCode, 202, `Expected 202 from /api/message, got ${postResult.statusCode}`);
        await client.waitFor('"command":"taskList"', 2500);
      } finally {
        client.close();
      }
    }

    const concurrentClients = 12;
    const clients = await Promise.all(Array.from({ length: concurrentClients }, () => createSseClient(port)));
    try {
      const postResult = await httpPostJson(port, '/api/message', { command: 'listTasks' }, 3000);
      assert.strictEqual(postResult.statusCode, 202, `Expected 202 from /api/message, got ${postResult.statusCode}`);
      await Promise.all(clients.map((client) => client.waitFor('"command":"taskList"', 2500)));
    } finally {
      for (const client of clients) {
        client.close();
      }
    }

    report.details = {
      port,
      reconnectCycles,
      concurrentClients,
      checks: ['single reconnect loop', 'multi-client broadcast'],
    };

    return finalizeTestReport(report);
  } catch (error) {
    const logs = getLogs();
    const serverTail = [logs.stdout ? `stdout:\n${logs.stdout}` : '', logs.stderr ? `stderr:\n${logs.stderr}` : '']
      .filter(Boolean)
      .join('\n\n');
    throw new Error(`${error.message}${serverTail ? `\n\nServer logs:\n${serverTail}` : ''}`, { cause: error });
  } finally {
    await stopServerProcess(child);
  }
}

function printReport(report) {
  console.log(`\n[${report.name}] PASS (${report.durationMs}ms)`);
  for (const [key, value] of Object.entries(report.details)) {
    console.log(`- ${key}: ${Array.isArray(value) ? value.join(', ') : value}`);
  }
}

async function main() {
  const repoRoot = path.resolve(__dirname, '..', '..');
  const tempWorkspaceRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'cloudwbot-stress-'));

  console.log('CloudWBot stress test start');
  console.log(`- temp workspace: ${tempWorkspaceRoot}`);

  try {
    const reports = [];
    reports.push(await runTaskLimitStress(tempWorkspaceRoot));
    reports.push(await runTimeoutStress(repoRoot));
    reports.push(await runSseReconnectStress(repoRoot, tempWorkspaceRoot));

    for (const report of reports) {
      printReport(report);
    }

    const totalMs = reports.reduce((sum, report) => sum + report.durationMs, 0);
    console.log(`\nAll stress checks passed (${totalMs}ms)`);
  } finally {
    cleanupDirectory(tempWorkspaceRoot);
  }
}

main().catch((error) => {
  console.error('\nStress test failed:');
  console.error(error?.stack || error?.message || String(error));
  process.exitCode = 1;
});
