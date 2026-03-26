#!/usr/bin/env node
// @ts-check
'use strict';

const fs = require('fs');
const http = require('http');
const https = require('https');
const os = require('os');
const path = require('path');

const projectRoot = path.resolve(process.argv[2] || process.cwd());
const serverBase = process.env.CLOUDWBOT_SERVER || 'http://127.0.0.1:3000';
const timeoutMs = Number(process.env.CLOUDWBOT_TASK_TIMEOUT_MS || 30 * 60 * 1000);
const outputDir = path.join(projectRoot, 'tt');
const logsDir = path.join(outputDir, 'logs');

fs.mkdirSync(logsDir, { recursive: true });

function generateTaskId() {
  return `${Date.now().toString(16)}${Math.floor(Math.random() * 0xffffffff).toString(16).padStart(8, '0')}`;
}

function resolveRequestClient(serverUrl) {
  return serverUrl.protocol === 'https:' ? https : http;
}

function normalizeServerUrl(serverBaseUrl) {
  const serverUrl = new URL(serverBaseUrl);
  // Some local Node servers bind to 127.0.0.1 only; avoid localhost IPv6 resolution issues.
  if (serverUrl.hostname === 'localhost') {
    serverUrl.hostname = '127.0.0.1';
  }
  return serverUrl;
}

function requestJson(serverUrl, method, route, payload) {
  return new Promise((resolve, reject) => {
    const client = resolveRequestClient(serverUrl);
    const body = payload ? JSON.stringify(payload) : '';
    const req = client.request(
      {
        protocol: serverUrl.protocol,
        hostname: serverUrl.hostname,
        port: serverUrl.port,
        path: route,
        method,
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(body),
        },
      },
      (res) => {
        let data = '';
        res.on('data', (chunk) => {
          data += chunk.toString();
        });
        res.on('end', () => {
          if (res.statusCode >= 200 && res.statusCode < 300) {
            resolve(data);
            return;
          }
          reject(new Error(`HTTP ${res.statusCode}: ${data || 'request failed'}`));
        });
      },
    );

    req.on('error', reject);
    if (body) req.write(body);
    req.end();
  });
}

function postMessage(serverUrl, payload) {
  return requestJson(serverUrl, 'POST', '/api/message', payload);
}

function connectEvents(serverUrl, onMessage) {
  const client = resolveRequestClient(serverUrl);
  const req = client.request(
    {
      protocol: serverUrl.protocol,
      hostname: serverUrl.hostname,
      port: serverUrl.port,
      path: '/api/events',
      method: 'GET',
      headers: {
        Accept: 'text/event-stream',
        'Cache-Control': 'no-cache',
      },
    },
    (res) => {
      let buffer = '';
      res.on('data', (chunk) => {
        buffer += chunk.toString();
        let boundaryIndex = buffer.indexOf('\n\n');
        while (boundaryIndex !== -1) {
          const block = buffer.slice(0, boundaryIndex);
          buffer = buffer.slice(boundaryIndex + 2);

          for (const line of block.split(/\r?\n/)) {
            if (!line.startsWith('data: ')) continue;
            const json = line.slice(6);
            try {
              const payload = JSON.parse(json);
              onMessage(payload);
            } catch {
              // Ignore malformed event lines.
            }
          }

          boundaryIndex = buffer.indexOf('\n\n');
        }
      });
    },
  );

  req.on('error', (err) => {
    onMessage({ command: '__event_error__', error: err?.message || String(err) });
  });

  req.end();
  return req;
}

function locateTaskSnapshot(taskId) {
  const candidates = [
    path.join(projectRoot, '.cloudwbot_workspace', 'tasks', taskId, '.cloudwbot-task.json'),
    path.join(os.homedir(), 'cloudwbot_workspace', 'tasks', taskId, '.cloudwbot-task.json'),
  ];

  return candidates.find((candidate) => fs.existsSync(candidate)) || '';
}

function readTaskSnapshot(taskId) {
  const snapshotPath = locateTaskSnapshot(taskId);
  if (!snapshotPath) return { snapshotPath: '', snapshot: null };
  try {
    const snapshot = JSON.parse(fs.readFileSync(snapshotPath, 'utf8'));
    return { snapshotPath, snapshot };
  } catch {
    return { snapshotPath, snapshot: null };
  }
}

function buildAnalysisDescription(rootPath) {
  return [
    `深度分析项目 ${rootPath} 的能力与缺陷，并输出可执行改进建议。`,
    '硬性要求：',
    '1) 必须读取真实源码文件（不能只列目录）。',
    '2) 必须执行多个核查命令，并基于真实输出得出结论。',
    '3) 产出 deep_project_analysis.md 报告。',
    '4) 报告必须包含：能力清单、缺陷清单、风险等级、修复优先级、证据清单（命令、源码文件路径、输出片段）。',
    '5) 报告结论必须明确指出当前项目最关键的系统性问题。',
    '6) 最后使用 RUN 命令打开报告。',
  ].join('\n');
}

async function main() {
  const serverUrl = normalizeServerUrl(serverBase);
  const taskId = generateTaskId();
  const logFile = path.join(logsDir, `task-${taskId}.log`);
  const resultFile = path.join(outputDir, 'last_deep_analysis_result.json');

  const writeLog = (line) => {
    const text = `${new Date().toISOString()} ${line}`;
    fs.appendFileSync(logFile, `${text}\n`, 'utf8');
    process.stdout.write(`${text}\n`);
  };

  let finished = false;
  let finalStatus = 'unknown';
  let finalSummary = '';
  let finalError = '';

  writeLog(`任务创建中 taskId=${taskId}`);

  const eventConn = connectEvents(serverUrl, async (message) => {
    if (!message || typeof message !== 'object') return;

    if (message.command === '__event_error__') {
      writeLog(`SSE 连接错误: ${message.error || 'unknown'}`);
      return;
    }

    const eventTaskId = message.taskId || message.sourceTaskId;
    if (eventTaskId !== taskId) return;

    if (message.command === 'taskPlanReady') {
      writeLog(`方案就绪，自动确认执行 taskId=${taskId}`);
      try {
        await postMessage(serverUrl, { command: 'confirmTaskPlan', taskId });
      } catch (err) {
        writeLog(`自动确认失败: ${err?.message || String(err)}`);
      }
      return;
    }

    if (message.command === 'taskTrace' && message.message) {
      writeLog(`[trace] ${message.message}`);
      return;
    }

    if (message.command === 'console' && message.line) {
      writeLog(`[console] ${String(message.line).slice(0, 800)}`);
      return;
    }

    if (message.command === 'taskStatus') {
      writeLog(`[status] ${message.status}`);
      if (message.summary) {
        finalSummary = message.summary;
      }
      if (message.error) {
        finalError = message.error;
      }

      if (
        message.status === 'completed'
        || message.status === 'failed'
        || message.status === 'interrupted'
        || message.status === 'blocked'
      ) {
        finished = true;
        finalStatus = message.status;
      }
    }
  });

  const timeoutHandle = setTimeout(() => {
    if (!finished) {
      finished = true;
      finalStatus = 'timeout';
      finalError = `等待任务超时（${Math.round(timeoutMs / 1000)} 秒）`;
      writeLog(finalError);
    }
  }, timeoutMs);

  try {
    await postMessage(serverUrl, {
      command: 'executeTask',
      taskId,
      description: buildAnalysisDescription(projectRoot),
    });
    writeLog('任务已投递到 CloudWBot。');
  } catch (err) {
    clearTimeout(timeoutHandle);
    eventConn.destroy();
    writeLog(`任务投递失败: ${err?.message || String(err)}`);
    process.exitCode = 2;
    return;
  }

  while (!finished) {
    // Wait until terminal status or timeout.
    // eslint-disable-next-line no-await-in-loop
    await new Promise((resolve) => setTimeout(resolve, 250));
  }

  clearTimeout(timeoutHandle);
  eventConn.destroy();

  const { snapshotPath, snapshot } = readTaskSnapshot(taskId);
  const finalPayload = {
    taskId,
    status: finalStatus,
    summary: finalSummary || snapshot?.summary || '',
    error: finalError || snapshot?.error || '',
    snapshotPath,
    writtenFiles: snapshot?.writtenFiles || [],
    runCommands: snapshot?.runCommands || [],
    createdAt: snapshot?.createdAt || '',
    finishedAt: new Date().toISOString(),
    logFile,
  };

  fs.writeFileSync(resultFile, JSON.stringify(finalPayload, null, 2), 'utf8');
  writeLog(`任务结束 status=${finalStatus}`);
  writeLog(`结果文件: ${resultFile}`);
  if (snapshotPath) {
    writeLog(`任务快照: ${snapshotPath}`);
  }

  if (finalStatus !== 'completed') {
    process.exitCode = 1;
  }
}

main().catch((err) => {
  process.stderr.write(`脚本异常: ${err?.stack || err?.message || String(err)}\n`);
  process.exitCode = 2;
});
