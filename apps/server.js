// @ts-check
'use strict';

const http = require('http');
const https = require('https');
const path = require('path');
const fs = require('fs');
const { spawn } = require('child_process');
const { EventEmitter } = require('events');

// Load modules
const { PORT, OLLAMA_URL, MIME, TASK_FILE_PREVIEW_MAX_BYTES, APP_ROOT } = require('./lib/config');
const { checkOllama, listModels, streamOllama } = require('./lib/ollama');
const { 
  getTaskRun, 
  listKnownTaskRuns, 
  createTaskRun, 
  toClientTask, 
  scheduleTaskRunSave, 
  flushTaskRunSave,
  buildTaskSummary,
  formatRerunSource,
  formatFollowUpSource,
  toWorkspaceRelativePath,
  copyWorkspaceContents,
  resolveTaskPreviewPath,
} = require('./lib/taskManager');
const { executeAiResponse } = require('./lib/executor');
const { validateTaskExecutionResult } = require('./lib/resultVerifier');
const { evaluateTaskOutcome } = require('./lib/outcomeJudge');
const { evaluateExpertExecutionGate } = require('./lib/expertGate');
const { 
  truncateText, 
  looksLikeActionableFollowUp, 
} = require('./lib/utils');

const STATIC_DIR = path.join(__dirname, 'webview');

// MIME types helper
function getTaskFileMime(filePath) {
  return MIME[path.extname(filePath).toLowerCase()] || 'application/octet-stream';
}

// ---------------------------------------------------------------------------
// SSE message bus — broadcast server events to all connected browsers
// ---------------------------------------------------------------------------

const bus = new EventEmitter();
bus.setMaxListeners(0);
const TASK_PLAN_CONFIRM_TIMEOUT_MS = 5000;

function readPositiveIntEnv(name, defaultValue) {
  const parsed = Number.parseInt(String(process.env[name] || ''), 10);
  if (!Number.isFinite(parsed)) return defaultValue;
  return Math.max(1, parsed);
}

function readNonNegativeIntEnv(name, defaultValue) {
  const parsed = Number.parseInt(String(process.env[name] || ''), 10);
  if (!Number.isFinite(parsed)) return defaultValue;
  return Math.max(0, parsed);
}

const TASK_REPLAN_MODE = String(process.env.TASK_REPLAN_MODE || 'budget').trim().toLowerCase();
const TASK_MAX_AUTO_REPLAN_ATTEMPTS = readPositiveIntEnv('TASK_MAX_AUTO_REPLAN_ATTEMPTS', 12);
const TASK_MAX_AUTO_REPLAN_DURATION_MS = readNonNegativeIntEnv('TASK_MAX_AUTO_REPLAN_DURATION_MS', 10 * 60 * 1000);
const TASK_OUTCOME_MIN_CONFIDENCE = 0.55;
const TASK_EXPERT_QA_MAX_ROUNDS = readPositiveIntEnv('TASK_EXPERT_QA_MAX_ROUNDS', 12);
const TASK_EXPERT_QA_MAX_DURATION_MS = readNonNegativeIntEnv('TASK_EXPERT_QA_MAX_DURATION_MS', 10 * 60 * 1000);
const TASK_EXPERT_QA_CATEGORY_STREAK_LIMIT = readPositiveIntEnv('TASK_EXPERT_QA_CATEGORY_STREAK_LIMIT', 2);
const TASK_EXPERT_QA_CATEGORY_TOTAL_LIMIT = readPositiveIntEnv('TASK_EXPERT_QA_CATEGORY_TOTAL_LIMIT', 4);
const pendingTaskPlans = new Map();
const RUNTIME_FEATURE_FINGERPRINT = [
  'acceptance-checklist',
  'expert-artifact-context',
  'uncertain-evidence-relaxed',
  'budget-retry-mode',
  'expert-category-fuse',
].join(',');

function readTaskFileIfExists(workspaceDir, filename, maxBytes = 120 * 1024) {
  try {
    const target = path.resolve(String(workspaceDir || '.'), filename);
    if (!fs.existsSync(target)) return '';
    const stat = fs.statSync(target);
    if (!stat.isFile()) return '';
    if (stat.size > maxBytes) return '';
    return fs.readFileSync(target, 'utf8');
  } catch {
    return '';
  }
}

function loadTaskPromptOverrides(workspaceDir) {
  const systemPrompt = readTaskFileIfExists(workspaceDir, 'system_prompt_database_agent.md');
  const developerPrompt = readTaskFileIfExists(workspaceDir, 'developer_prompt_database_agent.md');
  return {
    systemPrompt,
    developerPrompt,
    enabled: Boolean(systemPrompt || developerPrompt),
  };
}

function loadTaskExecutionPolicies(workspaceDir) {
  const readJson = (filename) => {
    const raw = readTaskFileIfExists(workspaceDir, filename, 256 * 1024);
    if (!raw) return null;
    try {
      return JSON.parse(raw);
    } catch {
      return null;
    }
  };

  const commandWhitelist = readJson('command_whitelist.json');
  const dbSecurityPolicy = readJson('db_security_policy.defaults.json');
  const toolSchema = readJson('tools.schema.json');

  const blockedCommandPatterns = Array.isArray(commandWhitelist?.blockedPatterns)
    ? commandWhitelist.blockedPatterns.filter((item) => typeof item === 'string' && item.trim())
    : [];

  const shellTimeout = Number(commandWhitelist?.runCommandPolicy?.timeoutSecondsDefault);
  const shellTimeoutMs = Number.isFinite(shellTimeout) && shellTimeout > 0
    ? Math.round(shellTimeout * 1000)
    : undefined;

  const dbMaxAffectedRowsRaw = Number(dbSecurityPolicy?.execRules?.maxAffectedRows);
  const dbMaxAffectedRows = Number.isFinite(dbMaxAffectedRowsRaw) && dbMaxAffectedRowsRaw >= 0
    ? Math.floor(dbMaxAffectedRowsRaw)
    : undefined;

  return {
    commandWhitelist,
    dbSecurityPolicy,
    toolSchema,
    blockedCommandPatterns,
    shellTimeoutMs,
    dbMaxAffectedRows,
    enabled: Boolean(commandWhitelist || dbSecurityPolicy || toolSchema),
  };
}

/** Send a message to all connected SSE clients. */
function post(data) {
  bus.emit('msg', data);
}

// Task emitters
function emitTaskTrace(taskId, message) {
  const taskRun = getTaskRun(taskId);
  if (taskRun) {
    taskRun.trace.push(message);
    scheduleTaskRunSave(taskRun);
  }
  post({ command: 'taskTrace', taskId, message });
}

function emitTaskStream(taskId, token) {
  const taskRun = getTaskRun(taskId);
  if (taskRun) {
    taskRun.aiOutput += token;
    scheduleTaskRunSave(taskRun);
  }
  post({ command: 'taskStream', taskId, token });
}

function emitTaskConsole(taskId, line) {
  const taskRun = getTaskRun(taskId);
  if (taskRun) {
    taskRun.consoleLines.push(line);
    scheduleTaskRunSave(taskRun);
  }
  post({ command: 'console', taskId, line });
}

function toClientPendingTaskPlan(pendingTask) {
  const options = pendingTask.options || {};
  return {
    id: pendingTask.taskId,
    taskId: pendingTask.taskId,
    description: pendingTask.description,
    rerunOfTaskId: options.rerunOfTaskId || '',
    rerunOfDescription: options.rerunOfDescription || '',
    followUpOfTaskId: options.followUpOfTaskId || '',
    followUpOfDescription: options.followUpOfDescription || '',
    status: 'pending',
    createdAt: pendingTask.createdAt,
    result: '',
    error: '',
    summary: pendingTask.awaitingConfirmation
      ? `方案已生成，等待确认（${TASK_PLAN_CONFIRM_TIMEOUT_MS / 1000}秒后默认执行）`
      : '正在本地生成任务方案...',
    workspaceDir: '',
    aiOutput: '',
    writtenFiles: [],
    trace: pendingTask.awaitingConfirmation
      ? ['🧠 任务方案生成完成，等待用户确认或取消']
      : ['🧠 正在本地生成任务方案'],
    consoleLines: [],
    planProposal: pendingTask.planText || '',
    planStatus: pendingTask.awaitingConfirmation ? 'awaiting-confirmation' : 'planning',
    planDueAt: pendingTask.dueAt || 0,
    planModel: pendingTask.model || '',
  };
}

function clearPendingTaskPlan(taskId) {
  const pendingTask = pendingTaskPlans.get(taskId);
  if (!pendingTask) return null;
  if (pendingTask.timer) {
    clearTimeout(pendingTask.timer);
    pendingTask.timer = null;
  }
  pendingTaskPlans.delete(taskId);
  return pendingTask;
}

// ---------------------------------------------------------------------------
// HTTP server
// ---------------------------------------------------------------------------

const server = http.createServer((req, res) => {
  const requestUrl = new URL(req.url, `http://localhost:${PORT}`);
  const { pathname, searchParams } = requestUrl;

  // CORS preflight
  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Headers': 'Content-Type',
    });
    res.end();
    return;
  }

  // Browser default favicon probe: return empty success to avoid noisy 404 logs.
  if ((req.method === 'GET' || req.method === 'HEAD') && pathname === '/favicon.ico') {
    res.writeHead(204);
    res.end();
    return;
  }

  // SSE: GET /api/events  — persistent server → browser stream
  if (req.method === 'GET' && pathname === '/api/events') {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
    });
    res.write(':connected\n\n');

    const listener = (data) => {
      try {
        res.write(`data: ${JSON.stringify(data)}\n\n`);
      } catch {
        // Client disconnected
      }
    };
    bus.on('msg', listener);
    req.on('close', () => bus.off('msg', listener));
    return;
  }

  // API: GET /api/task-file - Serve task file
  if ((req.method === 'GET' || req.method === 'HEAD') && pathname === '/api/task-file') {
    serveTaskFile(
      res,
      searchParams.get('taskId') || '',
      searchParams.get('filePath') || '',
      searchParams.get('download') === '1',
      req.method === 'HEAD',
    );
    return;
  }

  // API: POST /api/message  — browser → server command
  if (req.method === 'POST' && pathname === '/api/message') {
    let body = '';
    req.on('data', (c) => (body += c));
    req.on('end', async () => {
      try {
        const msg = JSON.parse(body);
        res.writeHead(202, { 'Content-Type': 'application/json' });
        res.end('{"ok":true}');
        // Handle asynchronously so the POST response returns immediately
        handleMessage(msg).catch((e) => console.error('[handleMessage]', e));
      } catch {
        res.writeHead(400);
        res.end('{"error":"invalid json"}');
      }
    });
    return;
  }

  // Static files from apps/webview/
  const filePath =
    pathname === '/'
      ? path.join(STATIC_DIR, 'index.html')
      : path.join(STATIC_DIR, pathname.slice(1));

  // Security: prevent path traversal using path.relative()
  const resolved = path.resolve(filePath);
  const base = path.resolve(STATIC_DIR);
  const rel = path.relative(base, resolved);
  if (rel.startsWith('..') || path.isAbsolute(rel)) {
    res.writeHead(403);
    res.end('Forbidden');
    return;
  }

  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(404);
      res.end('Not found');
      return;
    }
    const ext = path.extname(filePath).toLowerCase();
    res.writeHead(200, { 'Content-Type': MIME[ext] || 'application/octet-stream' });
    res.end(data);
  });
});

server.listen(PORT, '127.0.0.1', () => {
  console.log(`CloudWBot running → http://localhost:${PORT}`);
  if (OLLAMA_URL !== 'http://localhost:11434') console.log(`Ollama URL : ${OLLAMA_URL}`);
});

// ---------------------------------------------------------------------------
// Message router
// ---------------------------------------------------------------------------

async function handleMessage(message) {
  switch (message.command) {
    case 'chat': {
      const { text, taskId } = message;
      try {
        await streamOllama(text, (token) => post({ command: 'chatToken', token, taskId }));
        post({ command: 'chatDone', taskId });
      } catch (err) {
        post({ command: 'chatDone', taskId });
        post({ command: 'chatError', error: err?.message || String(err) });
      }
      break;
    }

    case 'executeTask': {
      await prepareTaskExecution(message.taskId, message.description, {
        rerunOfTaskId: message.rerunOfTaskId,
        rerunOfDescription: message.rerunOfDescription,
      });
      break;
    }

    case 'confirmTaskPlan': {
      await approveTaskPlan(message.taskId, 'manual');
      break;
    }

    case 'cancelTaskPlan': {
      cancelTaskPlan(message.taskId, '用户取消执行（方案确认阶段）');
      break;
    }

    case 'listTasks': {
      const storedTasks = listKnownTaskRuns().map(toClientTask);
      const pendingTasks = [...pendingTaskPlans.values()].map(toClientPendingTaskPlan);
      const knownTaskIds = new Set(storedTasks.map((task) => task.id || task.taskId));
      post({
        command: 'taskList',
        tasks: storedTasks.concat(pendingTasks.filter((task) => !knownTaskIds.has(task.id || task.taskId))),
      });
      break;
    }

    case 'previewTaskFile': {
      await previewTaskFile(message.taskId, message.filePath);
      break;
    }

    case 'revealTaskFile': {
      await revealTaskFile(message.taskId, message.filePath);
      break;
    }

    case 'askTask': {
      await answerTaskFollowUp(message.taskId, message.question);
      break;
    }

    case 'cancelTask': {
      cancelTaskPlan(message.taskId, '用户取消执行（方案确认阶段）');
      break;
    }

    case 'getStatus': {
      const ollamaOk = await checkOllama();
      const models = ollamaOk ? await listModels() : [];
      post({ command: 'status', ollamaOk, models, url: OLLAMA_URL });
      break;
    }

    case 'listModels': {
      post({ command: 'models', models: await listModels() });
      break;
    }

    case 'getHints': {
      post({ command: 'hints', hints: loadHints(message.query) });
      break;
    }

    case 'searchGithub': {
      post({ command: 'githubResults', results: await searchGithub(message.query) });
      break;
    }
  }
}

// ---------------------------------------------------------------------------
// Task execution
// ---------------------------------------------------------------------------

function buildTaskExecutionPrompt(description, workspaceDir) {
  const promptOverrides = loadTaskPromptOverrides(workspaceDir);
  return [
    'You are a local task executor. Produce directly executable output.',
    '',
    'Rules:',
    '1. For every file you create or modify, output the full final content using this exact format:',
    'FILE: <relative/path>',
    '```',
    '<full file content>',
    '```',
    '2. For every shell command that should actually run, output a separate line:',
    'RUN: <command>',
    '3. Never use placeholder comments like "... rest of code".',
    '4. Never put runnable commands in fenced shell blocks unless the task is explicitly creating a .sh file. Use RUN: lines instead.',
    '5. If the task asks to open, launch, or preview a generated file, include the correct RUN command to do so.',
    '6. When generating browser code, ensure every referenced library is imported and every selected DOM element actually exists.',
    '7. When updating an existing file, keep the same filename and output the full replacement content.',
    '8. For HTML files, always output a complete document with <!DOCTYPE html>, <html>, <head>, and <body>.',
    '9. If HTML uses document.querySelector("canvas"), include a <canvas> element in the body.',
    '10. If HTML uses THREE.OrbitControls, load OrbitControls.js before the inline script that uses it.',
    '11. If HTML uses THREE.*, ensure three.js is imported before any inline script that references THREE.',
    '12. For codebase analysis tasks, run concrete inspection commands (for example: ls, find, grep, cat, sed) and base conclusions on real outputs. Never use assumed sample outputs.',
    '13. If the output is a report, include a dedicated "证据清单" (or "Evidence") section containing: executed commands, referenced source file paths, and output snippets (including exit code or key stdout/stderr lines).',
    '14. Do not ask the user to confirm assumptions mid-execution. If information is missing, run local inspection commands first and continue with evidence-based steps.',
    '15. For database tasks, never embed raw credentials directly in a URI string. Use environment variables and driver parameters (or URL-encode credentials) to avoid connection parsing errors.',
    '16. When writing Python code that embeds HTML/JavaScript templates, never use f-strings unless interpolation is required; keep JavaScript braces literal to avoid Python NameError.',
    '17. Never leave SQL placeholders such as your_table_name or TODO table names. Use concrete tables/columns discovered from real inspection output.',
    '18. Avoid fragile python -c one-liners for multi-step logic. If Python logic includes with/for/if/try/class/def, write a .py file and run it with RUN: python <file>.',
    '19. For data/visualization tasks, you must verify row_count > 0 before claiming success. If query result is empty, do not mark done: revise query against real business tables and rerun.',
    '20. Do not create mock tables only to satisfy query syntax (for example empty flights/categories). Prefer existing production-like tables and provide evidence of non-empty result.',
    '21. For relational tables with foreign keys, insert/validate parent table records first, then child table records. If FK violation appears, fix dependency order before retrying.',
    '22. For RUN lines containing multiple shell commands, make them fail-fast (for example with set -e) so early failures are not hidden by later successful commands.',
    '23. For database tasks, first discover real tables (for example via information_schema or \\dt), then only query discovered tables. Never guess table names such as customers/orders unless discovered in command output.',
    '24. Never ask the user to answer prerequisite questions in execution output. If parameters are missing, choose conservative defaults, execute local discovery commands, and continue.',
    '25. Choose OS-native service checks: use systemctl only on Linux; on macOS prefer pg_isready / brew services / launchctl instead of systemctl.',
    '26. For psql commands that execute SQL or meta-commands, always specify an explicit database (for example -d postgres) before discovery; do not rely on implicit current-user database.',
    '',
    'Example:',
    'FILE: HelloWorldWindow.java',
    '```java',
    'import javax.swing.JFrame;',
    '// ... complete code',
    '```',
    'RUN: javac HelloWorldWindow.java',
    'RUN: java HelloWorldWindow',
    '',
    `Work directory: ${workspaceDir}`,
    '',
    `Task: ${description}`,
    promptOverrides.systemPrompt
      ? `\nTask-local SYSTEM prompt override (must follow):\n${truncateText(promptOverrides.systemPrompt, 12000)}`
      : '',
    promptOverrides.developerPrompt
      ? `\nTask-local DEVELOPER prompt override (must follow):\n${truncateText(promptOverrides.developerPrompt, 12000)}`
      : '',
  ].join('\n');
}

function buildTaskPlanningPrompt(description, options = {}) {
  const context = [];
  if (options.rerunOfDescription) {
    context.push(`重跑来源任务: ${options.rerunOfDescription}`);
  }
  if (options.followUpOfDescription) {
    context.push(`继续来源任务: ${options.followUpOfDescription}`);
  }

  return [
    '你是本地任务执行前的规划助手。请先输出执行方案，不要直接输出最终代码。',
    '必须用中文，方案要更丰富、更先进、更稳妥，且可直接落地。',
    '',
    '输出结构（按顺序）：',
    '1) 目标理解（明确交付物）',
    '2) 执行路线（分步骤）',
    '3) 本地资源优先策略（优先使用本机已有能力）',
    '4) 联网资源策略（仅在必要时说明触发条件与来源）',
    '5) 风险与回退方案（失败时怎么兜底）',
    '6) 验收清单（可验证）',
    '',
    '约束：',
    '- 强调可执行性与可验证性，避免空泛建议。',
    '- 说明预计会生成哪些文件、可能执行哪些命令。',
    '- 只输出方案文本，不要使用 FILE: 或 RUN: 前缀。',
    '',
    context.length > 0 ? `上下文:\n${context.join('\n')}` : '',
    `用户诉求: ${description}`,
  ].filter(Boolean).join('\n');
}

function buildTaskReplanPrompt(basePrompt, context = {}) {
  const failureReasons = Array.isArray(context.failureReasons)
    ? context.failureReasons.filter(Boolean)
    : [];
  const attemptedCommands = Array.isArray(context.attemptedCommands)
    ? context.attemptedCommands.filter(Boolean)
    : [];
  const outputSnippets = Array.isArray(context.outputSnippets)
    ? context.outputSnippets.filter(Boolean).slice(-8)
    : [];
  const discoveredTables = Array.isArray(context.discoveredTables)
    ? context.discoveredTables.filter(Boolean)
    : [];

  return [
    basePrompt,
    '',
    'The previous execution attempt did not satisfy completion checks. Re-plan and execute again with a corrected strategy.',
    `Current attempt: ${context.attempt || 2}/${context.maxAttempts || 2}`,
    context.failureSummary ? `Previous failure summary: ${context.failureSummary}` : '',
    failureReasons.length > 0 ? `Failure reasons:\n- ${failureReasons.join('\n- ')}` : '',
    discoveredTables.length > 0 ? `Discovered database tables (must use these exact names only):\n- ${discoveredTables.join('\n- ')}` : '',
    attemptedCommands.length > 0 ? `Commands already attempted:\n- ${attemptedCommands.join('\n- ')}` : '',
    outputSnippets.length > 0 ? `Recent execution output snippets:\n- ${outputSnippets.join('\n- ')}` : '',
    'Hard requirements for this retry:',
    '1. Do not repeat failing commands unchanged unless you first fix prerequisites.',
    '2. Start with concrete RUN inspection commands to verify current workspace state.',
    '3. Keep conclusions strictly aligned with real command output and generated files.',
    '4. If blocked by credentials/permissions/dependencies, provide blocker evidence and do not claim completion.',
    '5. For PostgreSQL foreign-key errors, create/insert parent table rows first, then child rows; verify with SELECT counts before JOIN/export.',
    '6. When exporting .csv from psql, use real CSV output (for example: psql -A -F, -t ... > file.csv or \\copy (... ) TO ... CSV HEADER).',
    discoveredTables.length > 0
      ? `7. You already discovered real tables. You must only use these names: ${discoveredTables.join(', ')}. Do not use any table outside this set.`
      : '7. First run table discovery and only use table names that appear in actual output. Do not guess.',
  ].filter(Boolean).join('\n');
}

function extractDiscoveredTablesFromConsoleLines(lines) {
  const tableSet = new Set();
  let captureMode = '';

  for (const rawLine of Array.isArray(lines) ? lines : []) {
    const line = String(rawLine || '').trim();
    if (!line) continue;

    if (/^\$\s*.*information_schema\.tables/i.test(line) || /^\$\s*.*\\dt\b/i.test(line)) {
      captureMode = 'tables';
      continue;
    }

    if (/^命令退出码:/i.test(line)) {
      captureMode = '';
      continue;
    }

    if (captureMode !== 'tables') continue;

    // psql -t -A output: one table name per line.
    if (/^[a-z][a-z0-9_]{1,63}$/i.test(line)) {
      tableSet.add(line.toLowerCase());
      continue;
    }

    // psql \dt table row format: "public | table_name | table | owner"
    const tableRowMatch = line.match(/^\s*[a-z0-9_]+\s*\|\s*([a-z][a-z0-9_]*)\s*\|\s*table\s*\|/i);
    if (tableRowMatch) {
      tableSet.add(String(tableRowMatch[1] || '').toLowerCase());
    }
  }

  return [...tableSet];
}

function normalizeForComparison(text) {
  return String(text || '')
    .toLowerCase()
    .replace(/\s+/g, ' ')
    .trim();
}

function buildCommandSignature(commands) {
  return (Array.isArray(commands) ? commands : [])
    .map((cmd) => normalizeForComparison(cmd))
    .filter(Boolean)
    .join(' || ');
}

function buildConsoleFailureSignature(lines) {
  const signals = [];
  for (const line of Array.isArray(lines) ? lines : []) {
    const text = String(line || '').trim();
    if (!text) continue;
    if (
      /traceback|error|exception|undefinedtable|operationalerror|关键命令执行失败|命令运行超时|denied|not found|no such file/i.test(text)
    ) {
      signals.push(normalizeForComparison(text));
    }
  }
  return signals.slice(-20).join(' || ');
}

function assessStrategyChange(previousAttempt, currentAttempt) {
  const prevPlan = normalizeForComparison(previousAttempt?.planText || '');
  const currPlan = normalizeForComparison(currentAttempt?.planText || '');
  const prevCommands = buildCommandSignature(previousAttempt?.runCommands || []);
  const currCommands = buildCommandSignature(currentAttempt?.runCommands || []);
  const prevConsole = buildConsoleFailureSignature(previousAttempt?.consoleLines || []);
  const currConsole = buildConsoleFailureSignature(currentAttempt?.consoleLines || []);

  const planChanged = prevPlan !== currPlan;
  const commandsChanged = prevCommands !== currCommands;
  const consoleChanged = prevConsole !== currConsole;
  const changed = planChanged || commandsChanged || consoleChanged;

  const unchangedBasis = [];
  if (!planChanged) unchangedBasis.push('执行方案文本');
  if (!commandsChanged) unchangedBasis.push('执行命令序列');
  if (!consoleChanged) unchangedBasis.push('控制台失败信号');

  return {
    changed,
    unchangedBasis,
  };
}

function extractPrimaryFailureFingerprint(outcome, execution) {
  const reasons = Array.isArray(outcome?.reasons) ? outcome.reasons : [];
  const firstReason = String(reasons[0] || '').toLowerCase().replace(/\s+/g, ' ').trim();
  const fatalCommand = String(execution?.fatal?.command || '').toLowerCase().replace(/\s+/g, ' ').trim();
  const exitCode = Number.isFinite(Number(execution?.fatal?.exitCode)) ? Number(execution.fatal.exitCode) : -1;
  const commandResult = Array.isArray(execution?.commandResults)
    ? execution.commandResults.find((item) => item && item.success === false && item.allowedFailure === false)
    : null;
  const failedCommand = String(commandResult?.command || '').toLowerCase().replace(/\s+/g, ' ').trim();
  const failedMessage = String(commandResult?.message || '').toLowerCase().replace(/\s+/g, ' ').trim();

  const signatureParts = [
    fatalCommand || failedCommand,
    firstReason,
    failedMessage,
    String(exitCode),
  ].filter(Boolean);
  return signatureParts.join(' || ');
}

function readTextFileSafe(filePath, maxBytes = 64 * 1024) {
  try {
    const buffer = fs.readFileSync(filePath);
    if (buffer.includes(0)) return '';
    return buffer.subarray(0, maxBytes).toString('utf8');
  } catch {
    return '';
  }
}

function gatherRootFilesByExt(workspaceDir, ext) {
  const matches = [];
  const maxDepth = 4;
  const maxFiles = 200;
  const normalizedExt = String(ext || '').toLowerCase();
  const root = path.resolve(String(workspaceDir || '.'));
  const queue = [{ dir: root, depth: 0 }];

  while (queue.length > 0 && matches.length < maxFiles) {
    const current = queue.shift();
    if (!current) break;

    let entries;
    try {
      entries = fs.readdirSync(current.dir, { withFileTypes: true });
    } catch {
      continue;
    }

    for (const entry of entries) {
      const absPath = path.join(current.dir, entry.name);
      if (entry.isFile()) {
        if (entry.name.toLowerCase().endsWith(normalizedExt)) {
          matches.push(absPath);
          if (matches.length >= maxFiles) break;
        }
        continue;
      }

      if (!entry.isDirectory()) continue;
      if (entry.name === 'node_modules' || entry.name === '.git') continue;
      if (current.depth >= maxDepth) continue;
      queue.push({ dir: absPath, depth: current.depth + 1 });
    }
  }

  return matches;
}

function buildTaskAcceptanceChecklist({ objectiveText, execution, workspaceDir }) {
  const objective = String(objectiveText || '');
  const runCommands = Array.isArray(execution?.runCommands) ? execution.runCommands : [];
  const writtenFiles = Array.isArray(execution?.writtenFiles) ? execution.writtenFiles : [];
  const checklist = [];

  const requiresFileOutput = /(创建|生成|写入|输出|保存|修改|修复|create|generate|write|save|update|fix)/i.test(objective);
  if (requiresFileOutput) {
    checklist.push({
      id: 'file-output',
      pass: writtenFiles.length > 0,
      detail: writtenFiles.length > 0 ? `检测到输出文件 ${writtenFiles.length} 个` : '目标要求产出文件，但未检测到写入文件',
    });
  }

  const requiresExecutionEvidence = /(运行|执行|编译|验证|测试|run|execute|compile|verify|test)/i.test(objective);
  if (requiresExecutionEvidence) {
    checklist.push({
      id: 'execution-evidence',
      pass: runCommands.length > 0,
      detail: runCommands.length > 0 ? `检测到执行命令 ${runCommands.length} 条` : '目标要求执行验证，但未检测到 RUN 命令',
    });
  }

  const javaFiles = writtenFiles.filter((filePath) => filePath.toLowerCase().endsWith('.java'));
  if (javaFiles.length > 0 && /(java|swing|窗口|gui|界面)/i.test(objective)) {
    const missingVisible = [];
    for (const javaFile of javaFiles) {
      const source = readTextFileSafe(javaFile, 96 * 1024);
      if (!source) continue;
      const hasGuiSignal = /javax\.swing|java\.awt|javafx\.|new\s+JFrame\s*\(|extends\s+JFrame\b/.test(source);
      if (!hasGuiSignal) continue;
      const hasVisible = /\.setVisible\(\s*true\s*\)|\.show\(\s*\)|primaryStage\.show\(\s*\)/.test(source);
      if (!hasVisible) missingVisible.push(path.basename(javaFile));
    }
    checklist.push({
      id: 'java-gui-visible',
      pass: missingVisible.length === 0,
      detail: missingVisible.length === 0
        ? 'Java GUI 可见性调用检查通过'
        : `以下 GUI 文件缺少窗口可见化调用: ${missingVisible.join(', ')}`,
    });
  }

  const dataTask = /(宽表|数据流|报表|csv|table|dashboard|可视化|business\s*flow)/i.test(objective);
  if (dataTask) {
    const csvCandidates = [...new Set([
      ...writtenFiles.filter((filePath) => filePath.toLowerCase().endsWith('.csv')),
      ...gatherRootFilesByExt(workspaceDir, '.csv'),
    ])];
    checklist.push({
      id: 'data-csv-output',
      pass: csvCandidates.length > 0,
      detail: csvCandidates.length > 0
        ? `检测到 CSV 产物 ${csvCandidates.length} 个`
        : '数据任务未检测到 CSV 产物',
    });

    let hasNonEmptyCsv = false;
    for (const csvFile of csvCandidates) {
      const text = readTextFileSafe(csvFile, 96 * 1024);
      if (!text) continue;
      if (/\(0\s+rows?\)/i.test(text)) continue;
      const lines = text.split(/\r?\n/).filter((line) => line.trim().length > 0);
      if (lines.length > 1) {
        hasNonEmptyCsv = true;
        break;
      }
    }
    checklist.push({
      id: 'data-non-empty',
      pass: hasNonEmptyCsv,
      detail: hasNonEmptyCsv
        ? '数据产物非空检查通过'
        : (csvCandidates.length > 0 ? '检测到 CSV 产物，但未发现有效数据行' : '未发现 CSV 数据产物，无法验证非空数据'),
    });
  }

  const htmlTask = /(html|网页|web|页面|可视化)/i.test(objective);
  if (htmlTask) {
    const htmlCandidates = [...new Set([
      ...writtenFiles.filter((filePath) => /\.html?$/i.test(filePath)),
      ...gatherRootFilesByExt(workspaceDir, '.html'),
      ...gatherRootFilesByExt(workspaceDir, '.htm'),
    ])];
    checklist.push({
      id: 'html-output',
      pass: htmlCandidates.length > 0,
      detail: htmlCandidates.length > 0
        ? `检测到 HTML 产物 ${htmlCandidates.length} 个`
        : '目标要求 HTML 可视化，但未检测到 HTML 产物',
    });

    let hasRenderableStructure = false;
    for (const htmlFile of htmlCandidates) {
      const text = readTextFileSafe(htmlFile, 96 * 1024);
      if (!text) continue;
      const hasBody = /<body[\s>]/i.test(text);
      const hasRenderable = /<table[\s>]|<canvas[\s>]|<svg[\s>]|<script[\s>]/i.test(text);
      if (hasBody && hasRenderable) {
        hasRenderableStructure = true;
        break;
      }
    }
    checklist.push({
      id: 'html-renderable',
      pass: hasRenderableStructure,
      detail: hasRenderableStructure ? 'HTML 结构可渲染性检查通过' : 'HTML 产物缺少可渲染结构（body + table/canvas/svg/script）',
    });
  }

  return checklist;
}

function extractFirstJsonObject(text) {
  const source = String(text || '');
  const fencedMatch = source.match(/```json\s*([\s\S]*?)```/i);
  const candidate = fencedMatch ? fencedMatch[1] : source;
  const firstBrace = candidate.indexOf('{');
  const lastBrace = candidate.lastIndexOf('}');
  if (firstBrace === -1 || lastBrace === -1 || lastBrace <= firstBrace) return null;
  const jsonText = candidate.slice(firstBrace, lastBrace + 1);
  try {
    return JSON.parse(jsonText);
  } catch {
    return null;
  }
}

function loadExpertArtifactContext(workspaceDir, writtenFiles) {
  const root = path.resolve(String(workspaceDir || '.'));
  let remainingChars = 12000;
  const sections = [];
  const sampledFiles = new Set();

  try {
    const entries = fs.readdirSync(root, { withFileTypes: true })
      .slice(0, 60)
      .map((entry) => (entry.isDirectory() ? `${entry.name}/` : entry.name));
    sections.push(`输出目录清单（最多60项）:\n${entries.join('\n') || '（空）'}`);
    remainingChars -= entries.join('\n').length;
  } catch {
    sections.push('输出目录清单: （读取失败）');
  }

  for (const filePath of (Array.isArray(writtenFiles) ? writtenFiles : []).slice(0, 8)) {
    if (remainingChars <= 0) break;
    try {
      const absolutePath = path.resolve(String(filePath || ''));
      sampledFiles.add(absolutePath);
      const relPath = toWorkspaceRelativePath(root, absolutePath);
      const stat = fs.statSync(absolutePath);
      const header = `FILE: ${relPath} | size=${stat.size}`;

      if (stat.size > 512 * 1024) {
        sections.push(`${header}\n（文件过大，跳过内容预览）`);
        continue;
      }

      const content = fs.readFileSync(absolutePath, 'utf8');
      const clipped = truncateText(content, Math.min(remainingChars, 1800));
      sections.push(`${header}\n\`\`\`\n${clipped}\n\`\`\``);
      remainingChars -= clipped.length;
    } catch {
      // Ignore unreadable artifacts.
    }
  }

  // Also sample key workspace artifacts that were produced by RUN redirects
  // (for example CSV/TXT/SQL/HTML files), even if they are not in writtenFiles.
  try {
    const candidateExtRe = /\.(csv|txt|sql|html?|json|log)$/i;
    const candidatePaths = fs.readdirSync(root)
      .filter((name) => candidateExtRe.test(name))
      .map((name) => path.join(root, name))
      .filter((absPath) => !sampledFiles.has(absPath))
      .slice(0, 8);

    for (const absPath of candidatePaths) {
      if (remainingChars <= 0) break;
      try {
        const relPath = toWorkspaceRelativePath(root, absPath);
        const stat = fs.statSync(absPath);
        if (!stat.isFile()) continue;

        const header = `ARTIFACT: ${relPath} | size=${stat.size}`;
        if (stat.size > 512 * 1024) {
          sections.push(`${header}\n（文件过大，跳过内容预览）`);
          continue;
        }

        const content = fs.readFileSync(absPath, 'utf8');
        const clipped = truncateText(content, Math.min(remainingChars, 1800));
        sections.push(`${header}\n\`\`\`\n${clipped}\n\`\`\``);
        remainingChars -= clipped.length;
      } catch {
        // Ignore unreadable artifacts.
      }
    }
  } catch {
    // Ignore directory scan errors.
  }

  return sections.join('\n\n');
}

function buildExpertQualityReviewPrompt({ objectiveText, workspaceDir, execution, round, consoleLines }) {
  const writtenFiles = Array.isArray(execution?.writtenFiles) ? execution.writtenFiles : [];
  const runCommands = Array.isArray(execution?.runCommands) ? execution.runCommands : [];
  const commandResults = Array.isArray(execution?.commandResults) ? execution.commandResults : [];
  const messages = Array.isArray(execution?.messages) ? execution.messages : [];
  const consoleTail = Array.isArray(consoleLines) ? consoleLines.slice(-80) : [];

  const commandSummary = commandResults.slice(-12).map((item) => {
    const marker = item.success ? 'OK' : (item.allowedFailure ? 'WARN' : 'FAIL');
    return `${marker} | ${item.command} | ${item.message}`;
  }).join('\n');
  const artifactContext = loadExpertArtifactContext(workspaceDir, writtenFiles);

  return [
    '你是本地任务执行结果的专家质检器。请严格基于证据判断，不要主观猜测。',
    '请检查当前结果是否真正满足用户目标；若存在问题，给出可执行的修复追问。',
    '只输出 JSON，不要输出任何额外解释。',
    '',
    'JSON schema:',
    '{',
    '  "pass": boolean,',
    '  "issues": string[],',
    '  "followUpTask": string,',
    '  "confidence": number,',
    '  "evidence": string[]',
    '}',
    '',
    '约束:',
    '- pass=true 时，issues 必须为空数组，followUpTask 为空字符串。',
    '- pass=false 时，issues 至少 1 条，followUpTask 必须是可直接执行的修复任务描述。',
    '- followUpTask 必须明确要修改/新增哪些文件与命令验证方式。',
    '- 不允许要求用户手工操作；应给出可由执行器自动完成的动作。',
    '',
    `质检轮次: ${round}`,
    `用户问题:\n${objectiveText}`,
    `工作目录: ${workspaceDir}`,
    `生成文件:\n${writtenFiles.join('\n') || '（无）'}`,
    `执行命令:\n${runCommands.join('\n') || '（无）'}`,
    `命令结果:\n${commandSummary || '（无）'}`,
    `实时控制台输出(尾部):\n${consoleTail.join('\n') || '（无）'}`,
    `输出目录产物与内容:\n${artifactContext || '（无）'}`,
    `结果摘要:\n${messages.slice(-20).join('\n') || '（无）'}`,
  ].join('\n');
}

function buildExpertFollowUpPrompt({ objectiveText, review, workspaceDir }) {
  const issues = Array.isArray(review?.issues) ? review.issues.filter(Boolean) : [];
  const followUpTask = String(review?.followUpTask || '').trim();
  return [
    buildTaskExecutionPrompt(followUpTask || objectiveText, workspaceDir),
    '',
    '你正在执行“专家质检修复轮”。必须只针对下列问题进行修复，并提供可验证证据。',
    `原始用户问题:\n${objectiveText}`,
    `专家问题:\n${issues.join('\n- ') ? `- ${issues.join('\n- ')}` : '（未提供问题，按目标补齐验证与证据）'}`,
    '修复要求:',
    '1. 仅输出 FILE:/RUN: 可执行内容。',
    '2. 必须包含至少一条用于验证修复结果的 RUN 命令。',
    '3. 若无法完成，必须输出可证据化的阻塞原因，不得声称已完成。',
  ].join('\n');
}

function classifyExpertFailureCategory(reason) {
  const text = String(reason || '').toLowerCase();
  if (!text) return 'unknown';
  if (/凭据|认证|权限|连接|依赖|interactive|timeout|阻塞|blocked/.test(text)) return 'blocker';
  if (/事实核查|证据|evidence|源码读取|推测/.test(text)) return 'verification';
  if (/目标判定|置信度|retryable|低于阈值|未通过/.test(text)) return 'outcome';
  const acceptanceMatch = text.match(/验收项未通过\(([^)]+)\)/);
  if (acceptanceMatch && acceptanceMatch[1]) {
    return `acceptance:${acceptanceMatch[1]}`;
  }
  return 'unknown';
}

function createEmptyExpertGateState() {
  return {
    reasons: [],
    categories: [],
    categoryTotals: {},
    lastCategory: '',
    round: 0,
  };
}

async function runExpertQualityLoop({
  taskId,
  taskRun,
  objectiveText,
  workspaceDir,
  initialExecution,
}) {
  let currentExecution = initialExecution;
  let lastFailureSummary = '';
  const startedAt = Date.now();
  let round = 0;
  const categoryTotals = Object.create(null);
  let lastPrimaryCategory = '';
  let primaryCategoryStreak = 0;
  const expertGateState = createEmptyExpertGateState();

  const recordExpertFailure = (reasons, currentRound) => {
    const reasonList = Array.isArray(reasons) ? reasons.filter(Boolean).map((item) => String(item)) : [];
    const categories = [...new Set(reasonList.map((reason) => classifyExpertFailureCategory(reason)))];
    const primaryCategory = categories[0] || 'unknown';
    categoryTotals[primaryCategory] = (categoryTotals[primaryCategory] || 0) + 1;

    if (lastPrimaryCategory === primaryCategory) {
      primaryCategoryStreak += 1;
    } else {
      lastPrimaryCategory = primaryCategory;
      primaryCategoryStreak = 1;
    }

    expertGateState.reasons = reasonList.slice(0, 8);
    expertGateState.categories = categories;
    expertGateState.lastCategory = primaryCategory;
    expertGateState.round = currentRound;
    expertGateState.categoryTotals = { ...categoryTotals };

    const hitStreakFuse = primaryCategoryStreak >= TASK_EXPERT_QA_CATEGORY_STREAK_LIMIT;
    const hitTotalFuse = categoryTotals[primaryCategory] >= TASK_EXPERT_QA_CATEGORY_TOTAL_LIMIT;

    if (hitStreakFuse) {
      return {
        shouldFuse: true,
        reason: `专家修复连续命中同一失败类别(${primaryCategory}) ${primaryCategoryStreak} 轮，已触发细粒度熔断。`,
      };
    }
    if (hitTotalFuse) {
      return {
        shouldFuse: true,
        reason: `专家修复累计命中失败类别(${primaryCategory}) ${categoryTotals[primaryCategory]} 次，已触发细粒度熔断。`,
      };
    }

    return { shouldFuse: false, reason: '' };
  };

  while (true) {
    round += 1;
    if (round > TASK_EXPERT_QA_MAX_ROUNDS) {
      break;
    }
    if (TASK_EXPERT_QA_MAX_DURATION_MS > 0 && Date.now() - startedAt > TASK_EXPERT_QA_MAX_DURATION_MS) {
      break;
    }
    emitTaskTrace(taskId, `🧑‍⚖️ 专家质检第 ${round}/${TASK_EXPERT_QA_MAX_ROUNDS} 轮`);

    let reviewRaw;
    try {
      const reviewPrompt = buildExpertQualityReviewPrompt({
        objectiveText,
        workspaceDir,
        execution: currentExecution,
        round,
        consoleLines: taskRun.consoleLines,
      });
      const model = await require('./lib/ollama').getModel();
      reviewRaw = await streamOllama(reviewPrompt, () => {}, model);
    } catch (err) {
      const msg = `专家质检失败：${err?.message || String(err)}`;
      currentExecution.messages.push(`⚠️ ${msg}`);
      return {
        ok: false,
        status: 'failed',
        summary: msg,
        execution: currentExecution,
        expertGate: expertGateState,
      };
    }

    const review = extractFirstJsonObject(reviewRaw);
    if (!review) {
      const msg = '专家质检返回格式无效（非 JSON）。';
      currentExecution.messages.push(`⚠️ ${msg}`);
      return {
        ok: false,
        status: 'failed',
        summary: msg,
        execution: currentExecution,
        expertGate: expertGateState,
      };
    }

    const pass = Boolean(review.pass);
    let issues = Array.isArray(review.issues) ? review.issues.filter(Boolean).map((item) => String(item)) : [];
    if (pass && issues.length === 0) {
      const acceptanceChecklist = buildTaskAcceptanceChecklist({
        objectiveText,
        execution: currentExecution,
        workspaceDir,
      });
      const guard = evaluateExpertExecutionGate({
        objectiveText,
        execution: currentExecution,
        aiResponse: '',
        projectRoot: APP_ROOT,
        minConfidence: TASK_OUTCOME_MIN_CONFIDENCE,
        acceptanceChecklist,
      });
      for (const item of guard.acceptanceChecklist) {
        emitTaskTrace(taskId, `${item.pass ? '✅' : '⚠️'} 专家审议守门验收[${item.id}] ${item.detail}`);
      }
      if (!guard.ok) {
        issues = guard.reasons;
        review.issues = issues;
        review.followUpTask = String(review.followUpTask || '').trim() || objectiveText;
        const categoryState = recordExpertFailure(issues, round);
        const guardMsg = `⚠️ 专家给出通过，但规则守门拒绝通过（第 ${round} 轮）：${issues.join('；')}`;
        currentExecution.messages.push(guardMsg);
        emitTaskTrace(taskId, guardMsg);
        if (categoryState.shouldFuse) {
          const fusedMsg = `${categoryState.reason} 请先人工检查该类别问题后再继续。`;
          currentExecution.messages.push(`❌ ${fusedMsg}`);
          emitTaskTrace(taskId, `🛑 ${fusedMsg}`);
          return {
            ok: false,
            status: 'blocked',
            summary: fusedMsg,
            execution: currentExecution,
            expertGate: expertGateState,
          };
        }
      } else {
        const confidenceValue = Number(review.confidence);
        const confidenceText = Number.isFinite(confidenceValue)
          ? `${Math.round(Math.max(0, Math.min(1, confidenceValue)) * 100)}%`
          : '未知';
        const passMsg = `✅ 专家质检通过（第 ${round} 轮，置信度 ${confidenceText}）`;
        currentExecution.messages.push(passMsg);
        emitTaskTrace(taskId, passMsg);
        return {
          ok: true,
          status: 'pass',
          summary: passMsg,
          execution: currentExecution,
          expertGate: expertGateState,
        };
      }
    }

    const issueMsg = `⚠️ 专家质检发现问题（第 ${round} 轮）: ${issues.join('；') || '未提供具体问题'}`;
    currentExecution.messages.push(issueMsg);
    emitTaskTrace(taskId, issueMsg);

    const followUpPrompt = buildExpertFollowUpPrompt({ objectiveText, review, workspaceDir });
    let followUpResponse;
    try {
      emitTaskTrace(taskId, `🤖 正在执行专家修复追问（第 ${round} 轮）...`);
      const model = await require('./lib/ollama').getModel();
      followUpResponse = await streamOllama(followUpPrompt, (token) => {
        emitTaskStream(taskId, token);
      }, model);
    } catch (err) {
      const msg = `专家修复追问失败：${err?.message || String(err)}`;
      currentExecution.messages.push(`❌ ${msg}`);
      return {
        ok: false,
        status: 'failed',
        summary: msg,
        execution: currentExecution,
        expertGate: expertGateState,
      };
    }

    taskRun.aiResponse = taskRun.aiResponse
      ? `${taskRun.aiResponse}\n\n===== EXPERT FOLLOW-UP ROUND ${round} =====\n${followUpResponse}`
      : followUpResponse;
    scheduleTaskRunSave(taskRun);

    const followUpExecution = await executeAiResponse(followUpResponse, workspaceDir, taskId, objectiveText, {
      emitTaskConsole,
      emitTaskTrace,
    });
    currentExecution = followUpExecution;

    const acceptanceChecklist = buildTaskAcceptanceChecklist({
      objectiveText,
      execution: followUpExecution,
      workspaceDir,
    });
    const guard = evaluateExpertExecutionGate({
      objectiveText,
      execution: followUpExecution,
      aiResponse: followUpResponse,
      projectRoot: APP_ROOT,
      minConfidence: TASK_OUTCOME_MIN_CONFIDENCE,
      acceptanceChecklist,
    });
    for (const item of guard.acceptanceChecklist) {
      emitTaskTrace(taskId, `${item.pass ? '✅' : '⚠️'} 专家修复验收[${item.id}] ${item.detail}`);
    }
    if (!guard.ok) {
      lastFailureSummary = guard.reasons.join('；') || '专家修复轮执行未达标';
      const categoryState = recordExpertFailure(guard.reasons, round);
      const failMsg = `❌ 专家修复轮执行未达标：${lastFailureSummary}`;
      followUpExecution.messages.push(failMsg);
      emitTaskTrace(taskId, failMsg);
      if (categoryState.shouldFuse) {
        const fusedMsg = `${categoryState.reason} 请先人工检查该类别问题后再继续。`;
        followUpExecution.messages.push(`❌ ${fusedMsg}`);
        emitTaskTrace(taskId, `🛑 ${fusedMsg}`);
        return {
          ok: false,
          status: 'blocked',
          summary: fusedMsg,
          execution: currentExecution,
          expertGate: expertGateState,
        };
      }
      // Continue to next expert round with latest execution evidence.
      continue;
    }
  }

  const elapsedMs = Date.now() - startedAt;
  const stopReason = (TASK_EXPERT_QA_MAX_DURATION_MS > 0 && elapsedMs > TASK_EXPERT_QA_MAX_DURATION_MS)
    ? `已达到专家质检时间预算（${Math.round(TASK_EXPERT_QA_MAX_DURATION_MS / 1000)}秒）`
    : `已达到专家质检轮次上限（${TASK_EXPERT_QA_MAX_ROUNDS}轮）`;
  const msg = lastFailureSummary
    ? `专家质检停止：${stopReason}。最后一次失败：${lastFailureSummary}`
    : `专家质检停止：${stopReason}。`;
  currentExecution.messages.push(`❌ ${msg}`);
  return {
    ok: false,
    status: 'blocked',
    summary: msg,
    execution: currentExecution,
    expertGate: expertGateState,
  };
}

function buildPlanningFallback(description, errorMessage) {
  const error = errorMessage ? `（规划模型异常: ${errorMessage}）` : '';
  return [
    `目标理解：围绕“${description}”完成可运行、可验证的交付。`,
    '执行路线：',
    '1. 先识别现有产物与可复用文件，避免重复生成。',
    '2. 按最小改动实现目标，优先保证可编译/可运行。',
    '3. 对关键输出执行本地验证（编译、运行、打开预览）。',
    '本地资源优先策略：优先使用本地命令与已有依赖；仅在缺失时再考虑联网补充。',
    '联网资源策略：仅在本地缺少关键依赖时触发，并优先使用可信来源。',
    '风险与回退：若主方案失败，改用保守实现并保留可回退路径。',
    '验收清单：文件已生成、命令执行成功、结果可复现。',
    error,
  ].filter(Boolean).join('\n');
}

async function prepareTaskExecution(taskId, description, options = {}) {
  clearPendingTaskPlan(taskId);

  const pendingTask = {
    taskId,
    description,
    options,
    createdAt: new Date().toISOString(),
    planText: '',
    model: '',
    awaitingConfirmation: false,
    dueAt: 0,
    timer: null,
  };
  pendingTaskPlans.set(taskId, pendingTask);

  post({
    command: 'taskStatus',
    taskId,
    description,
    status: 'pending',
    step: 0,
    rerunOfTaskId: options.rerunOfTaskId || '',
    rerunOfDescription: options.rerunOfDescription || '',
    followUpOfTaskId: options.followUpOfTaskId || '',
    followUpOfDescription: options.followUpOfDescription || '',
    autoSelect: Boolean(options.autoSelect),
  });
  post({ command: 'taskPlanStart', taskId, description });

  try {
    const model = await require('./lib/ollama').getModel();
    pendingTask.model = model;
    post({ command: 'taskPlanMeta', taskId, model });

    const planningPrompt = typeof options.buildPlanPrompt === 'function'
      ? options.buildPlanPrompt(description)
      : buildTaskPlanningPrompt(description, options);

    pendingTask.planText = await streamOllama(
      planningPrompt,
      (token) => {
        const currentPending = pendingTaskPlans.get(taskId);
        if (!currentPending) return;
        currentPending.planText += token;
        post({ command: 'taskPlanToken', taskId, token });
      },
      model,
    );
  } catch (err) {
    const message = err?.message || String(err);
    pendingTask.planText = buildPlanningFallback(description, message);
    post({
      command: 'taskPlanError',
      taskId,
      error: `方案生成失败，已切换为稳妥默认方案：${message}`,
    });
  }

  const latestPending = pendingTaskPlans.get(taskId);
  if (!latestPending) return;

  latestPending.awaitingConfirmation = true;
  latestPending.dueAt = Date.now() + TASK_PLAN_CONFIRM_TIMEOUT_MS;

  post({
    command: 'taskPlanReady',
    taskId,
    description,
    plan: latestPending.planText,
    model: latestPending.model,
    dueAt: latestPending.dueAt,
    timeoutSeconds: TASK_PLAN_CONFIRM_TIMEOUT_MS / 1000,
  });

  latestPending.timer = setTimeout(() => {
    const waitingTask = pendingTaskPlans.get(taskId);
    if (!waitingTask || !waitingTask.awaitingConfirmation) return;
    approveTaskPlan(taskId, 'auto').catch((err) => {
      post({
        command: 'chatError',
        error: `任务自动确认后执行失败: ${err?.message || String(err)}`,
      });
    });
  }, TASK_PLAN_CONFIRM_TIMEOUT_MS);
}

async function approveTaskPlan(taskId, mode = 'manual') {
  const pendingTask = clearPendingTaskPlan(taskId);
  if (!pendingTask) {
    if (mode === 'manual') {
      post({
        command: 'taskPlanDecision',
        taskId,
        decision: 'ignored',
        reason: '该任务不在待确认队列中，可能已执行或已取消。',
      });
    }
    return;
  }

  post({
    command: 'taskPlanDecision',
    taskId,
    decision: mode === 'auto' ? 'auto-approved' : 'approved',
    timeoutSeconds: TASK_PLAN_CONFIRM_TIMEOUT_MS / 1000,
  });

  await runTask(taskId, pendingTask.description, pendingTask.options);
}

function cancelTaskPlan(taskId, reason = '用户取消执行（方案确认阶段）') {
  const pendingTask = clearPendingTaskPlan(taskId);
  if (!pendingTask) {
    post({
      command: 'taskPlanDecision',
      taskId,
      decision: 'ignored',
      reason: '该任务不在待确认队列中，无法取消。',
    });
    return;
  }

  post({ command: 'taskPlanDecision', taskId, decision: 'cancelled', reason });

  const { taskRun, workspaceDir } = createTaskRun(pendingTask.description, {
    ...pendingTask.options,
    taskId,
  });
  taskRun.status = 'interrupted';
  taskRun.error = reason;
  taskRun.resultLines = [`⚠️ ${reason}`];
  taskRun.summary = buildTaskSummary(taskRun);
  flushTaskRunSave(taskRun);

  post({
    command: 'taskStatus',
    taskId,
    description: pendingTask.description,
    status: 'interrupted',
    error: taskRun.error,
    summary: taskRun.summary,
    workspaceDir,
    rerunOfTaskId: taskRun.rerunOfTaskId,
    rerunOfDescription: taskRun.rerunOfDescription,
    followUpOfTaskId: taskRun.followUpOfTaskId,
    followUpOfDescription: taskRun.followUpOfDescription,
    autoSelect: Boolean(pendingTask.options.autoSelect),
  });
}

function buildTaskContinuationPrompt(taskRun, question, workspaceDir) {
  const sections = [
    buildTaskExecutionPrompt(question, workspaceDir),
    'This is a continuation of an existing local task. Work from the provided outputs and keep filenames stable when updating files.',
    'If the follow-up asks to read/analyze code, inspect real files via RUN commands before concluding and avoid hypothetical examples.',
    'For analysis reports, include a dedicated evidence section with command records, referenced source files, and output snippets.',
    `Source task description:\n${taskRun.description}`,
    `Source task summary:\n${taskRun.summary || buildTaskSummary(taskRun)}`,
  ];

  if (taskRun.resultLines.length > 0) {
    sections.push(`Source execution results:\n${truncateText(taskRun.resultLines.join('\n'), 6000)}`);
  }

  const fileContext = loadTaskFileContext(taskRun);
  if (fileContext) {
    sections.push(`Source files:\n${fileContext}`);
  }

  sections.push(`Follow-up request:\n${question}`);
  return sections.join('\n\n');
}

function loadTaskFileContext(taskRun) {
  let remainingChars = 16000;
  const sections = [];

  for (const filePath of taskRun.writtenFiles.slice(0, 4)) {
    if (remainingChars <= 0) break;
    try {
      const content = fs.readFileSync(filePath, 'utf8');
      const clipped = truncateText(content, Math.min(remainingChars, 4000));
      sections.push(`FILE: ${toWorkspaceRelativePath(taskRun.workspaceDir, filePath)}\n\`\`\`\n${clipped}\n\`\`\``);
      remainingChars -= clipped.length;
    } catch {
      // Ignore files that cannot be read back.
    }
  }

  return sections.join('\n\n');
}

function buildTaskFollowUpPrompt(taskRun, question) {
  const sections = [
    'You are helping the user inspect or continue a finished local coding task.',
    'Reply in Chinese. Be concrete about generated files, commands, and outputs.',
    'Base every conclusion on the provided file contents and execution results. Do not invent commands, files, or fixes that are not supported by the context.',
    'If you identify a concrete problem, explain exactly which file content or command caused it.',
    `Task description:\n${taskRun.description}`,
    `Task status:\n${taskRun.status}`,
    `Workspace directory:\n${taskRun.workspaceDir}`,
    `Task summary:\n${taskRun.summary || buildTaskSummary(taskRun)}`,
  ];

  if (taskRun.resultLines.length > 0) {
    sections.push(`Execution results:\n${truncateText(taskRun.resultLines.join('\n'), 6000)}`);
  }

  if (taskRun.aiResponse) {
    sections.push(`Original AI output:\n${truncateText(taskRun.aiResponse, 8000)}`);
  }

  const fileContext = loadTaskFileContext(taskRun);
  if (fileContext) {
    sections.push(`Generated files:\n${fileContext}`);
  }

  sections.push(`User follow-up question:\n${question}`);
  return sections.join('\n\n');
}

async function runTask(taskId, description, options = {}) {
  const { taskRun, workspaceDir, usedFallback } = createTaskRun(description, {
    ...options,
    taskId,
  });

  if (options.cloneWorkspaceFrom) {
    try {
      copyWorkspaceContents(options.cloneWorkspaceFrom, workspaceDir);
    } catch (err) {
      taskRun.status = 'failed';
      taskRun.error = `准备继续任务工作区失败: ${err?.message || String(err)}`;
      taskRun.summary = buildTaskSummary(taskRun);
      flushTaskRunSave(taskRun);
      post({
        command: 'taskStatus',
        taskId,
        description,
        status: 'failed',
        error: taskRun.error,
        summary: taskRun.summary,
        workspaceDir,
        rerunOfTaskId: taskRun.rerunOfTaskId,
        rerunOfDescription: taskRun.rerunOfDescription,
        followUpOfTaskId: taskRun.followUpOfTaskId,
        followUpOfDescription: taskRun.followUpOfDescription,
        autoSelect: Boolean(options.autoSelect),
      });
      return;
    }
  }

  post({
    command: 'taskStatus',
    taskId,
    description,
    status: 'running',
    step: 0,
    workspaceDir,
    rerunOfTaskId: taskRun.rerunOfTaskId,
    rerunOfDescription: taskRun.rerunOfDescription,
    followUpOfTaskId: taskRun.followUpOfTaskId,
    followUpOfDescription: taskRun.followUpOfDescription,
    autoSelect: Boolean(options.autoSelect),
  });
  
  emitTaskTrace(
    taskId,
    usedFallback
      ? `📁 自动识别的项目根目录不可写，已回退到主目录工作区: ${workspaceDir}`
      : `📁 工作目录: ${workspaceDir}`,
  );
  emitTaskTrace(taskId, `🧩 运行时能力: ${RUNTIME_FEATURE_FINGERPRINT}`);
  
  if (taskRun.rerunOfTaskId) {
    emitTaskTrace(taskId, `♻️ 该任务由重跑创建，来源: ${formatRerunSource(taskRun)}`);
  }
  if (taskRun.followUpOfTaskId) {
    emitTaskTrace(taskId, `🧭 该任务由追问继续执行创建，来源: ${formatFollowUpSource(taskRun)}`);
  }

  const taskPolicies = loadTaskExecutionPolicies(workspaceDir);
  if (taskPolicies.enabled) {
    emitTaskTrace(
      taskId,
      `🛡️ 已加载任务本地策略: blockedPatterns=${taskPolicies.blockedCommandPatterns.length}, shellTimeout=${taskPolicies.shellTimeoutMs ? `${Math.round(taskPolicies.shellTimeoutMs / 1000)}s` : 'default'}`,
    );
    if (taskPolicies.dbSecurityPolicy) {
      const rowCap = Number(taskPolicies.dbSecurityPolicy?.execRules?.maxAffectedRows);
      if (Number.isFinite(rowCap)) {
        emitTaskTrace(taskId, `🗄️ 数据库安全策略已加载: maxAffectedRows=${rowCap}`);
      }
    }
  }

  const basePrompt = typeof options.buildPrompt === 'function'
    ? options.buildPrompt(workspaceDir)
    : buildTaskExecutionPrompt(description, workspaceDir);
  // Keep outcome/verification objective focused on the current follow-up ask.
  // Appending full historical descriptions can misclassify task intent.
  const objectiveText = String(description || '');
  const maxAttempts = Math.max(1, TASK_MAX_AUTO_REPLAN_ATTEMPTS);
  const retryWithBudget = TASK_REPLAN_MODE !== 'fixed';
  const startedAt = Date.now();

  let previousAttemptFailure = null;
  let finalExecution = null;
  let finalOutcome = null;
  let attempt = 0;
  let previousFailureFingerprint = '';
  let repeatedFailureCount = 0;

  while (true) {
    attempt += 1;
    const elapsedMs = Date.now() - startedAt;
    const withinAttemptCap = attempt <= maxAttempts;
    const withinTimeBudget = TASK_MAX_AUTO_REPLAN_DURATION_MS <= 0 || elapsedMs <= TASK_MAX_AUTO_REPLAN_DURATION_MS;
    if (!withinAttemptCap) break;
    if (retryWithBudget && !withinTimeBudget) break;

    let fullResponse;
    try {
      emitTaskTrace(
        taskId,
        attempt === 1
          ? '🤖 正在请求 AI 生成方案...'
          : `🤖 第${attempt}轮自动重规划中（预算模式，已用 ${Math.round(elapsedMs / 1000)} 秒）...`,
      );
      const model = await require('./lib/ollama').getModel();
      emitTaskTrace(taskId, `🧠 使用模型: ${model}`);

      const prompt = attempt === 1
        ? basePrompt
        : buildTaskReplanPrompt(basePrompt, {
          attempt,
          maxAttempts,
          failureSummary: previousAttemptFailure?.summary || '',
          failureReasons: previousAttemptFailure?.reasons || [],
          attemptedCommands: previousAttemptFailure?.runCommands || [],
          outputSnippets: previousAttemptFailure?.messages || [],
          discoveredTables: extractDiscoveredTablesFromConsoleLines(taskRun.consoleLines),
        });

      fullResponse = await streamOllama(prompt, (token) => {
        emitTaskStream(taskId, token);
      }, model);
    } catch (err) {
      const msg = err?.message || String(err);
      const hint =
        msg.includes('ECONNREFUSED') || msg.includes('connect')
          ? ' — 请确认 Ollama 已启动（ollama serve）'
          : msg.includes('cloud is disabled') || msg.includes('remote model is unavailable')
            ? ' — 当前模型不可在本地执行，请设置 OLLAMA_MODEL 为本地模型'
            : msg.includes('model') || msg.includes('not found')
              ? ' — 请设置 OLLAMA_MODEL 环境变量或运行 ollama pull <模型名>'
              : '';
      taskRun.status = 'failed';
      taskRun.error = `AI 请求失败（第${attempt}轮）: ${msg}${hint}`;
      taskRun.summary = buildTaskSummary(taskRun);
      flushTaskRunSave(taskRun);
      post({
        command: 'taskStatus',
        taskId,
        status: 'failed',
        error: taskRun.error,
        summary: taskRun.summary,
        workspaceDir,
        rerunOfTaskId: taskRun.rerunOfTaskId,
        rerunOfDescription: taskRun.rerunOfDescription,
        followUpOfTaskId: taskRun.followUpOfTaskId,
        followUpOfDescription: taskRun.followUpOfDescription,
      });
      return;
    }

    taskRun.aiResponse = taskRun.aiResponse
      ? `${taskRun.aiResponse}\n\n===== ATTEMPT ${attempt} =====\n${fullResponse}`
      : fullResponse;
    scheduleTaskRunSave(taskRun);

    emitTaskTrace(
      taskId,
      attempt === 1
        ? '📝 正在解析并执行 AI 方案...'
        : `📝 正在执行重规划方案（第${attempt}轮）...`,
    );

    const attemptConsoleStart = taskRun.consoleLines.length;

    let execution;
    try {
      execution = await executeAiResponse(fullResponse, workspaceDir, taskId, description, {
        emitTaskConsole,
        emitTaskTrace,
      }, {
        shellTimeout: taskPolicies.shellTimeoutMs,
        blockedCommandPatterns: taskPolicies.blockedCommandPatterns,
        dbMaxAffectedRows: taskPolicies.dbMaxAffectedRows,
      });
    } catch (err) {
      taskRun.status = 'failed';
      taskRun.error = err?.message || String(err);
      taskRun.summary = buildTaskSummary(taskRun);
      flushTaskRunSave(taskRun);
      post({
        command: 'taskStatus',
        taskId,
        status: 'failed',
        error: taskRun.error,
        summary: taskRun.summary,
        workspaceDir,
        rerunOfTaskId: taskRun.rerunOfTaskId,
        rerunOfDescription: taskRun.rerunOfDescription,
        followUpOfTaskId: taskRun.followUpOfTaskId,
        followUpOfDescription: taskRun.followUpOfDescription,
      });
      return;
    }

    const verification = validateTaskExecutionResult({
      objectiveText,
      runCommands: execution.runCommands,
      writtenFiles: execution.writtenFiles,
      aiResponse: fullResponse,
    }, {
      projectRoot: APP_ROOT,
    });

    if (verification.checked) {
      const verifyMessage = verification.ok
        ? `ℹ️ ${verification.summary}`
        : `⚠️ ${verification.summary}`;
      execution.messages.push(verifyMessage);
      emitTaskTrace(taskId, `🧪 ${verifyMessage}`);
    }

    const outcome = evaluateTaskOutcome({
      objectiveText,
      execution,
      verification,
      aiResponse: fullResponse,
      minConfidence: TASK_OUTCOME_MIN_CONFIDENCE,
    });
    let judgedOutcome = outcome;

    const acceptanceChecklist = buildTaskAcceptanceChecklist({
      objectiveText,
      execution,
      workspaceDir,
    });
    const failedAcceptanceItems = acceptanceChecklist.filter((item) => !item.pass);
    for (const item of acceptanceChecklist) {
      emitTaskTrace(taskId, `${item.pass ? '✅' : '⚠️'} 验收项[${item.id}] ${item.detail}`);
    }
    if (failedAcceptanceItems.length > 0) {
      const failureReasons = failedAcceptanceItems.map((item) => `验收项未通过(${item.id}): ${item.detail}`);
      judgedOutcome = {
        ok: false,
        status: 'retryable',
        confidence: Math.min(outcome.confidence || 0.5, 0.5),
        reasons: [...(outcome.reasons || []), ...failureReasons],
        summary: `目标判定未通过，需修复验收项：${failureReasons.join('；')}`,
        blockerType: '',
        blockerHint: '',
      };
    }

    const attemptConsoleLines = taskRun.consoleLines.slice(attemptConsoleStart);
    if (!judgedOutcome.ok && attempt > 1 && previousAttemptFailure) {
      const strategy = assessStrategyChange(previousAttemptFailure, {
        planText: fullResponse,
        runCommands: execution.runCommands,
        consoleLines: attemptConsoleLines,
      });
      if (!strategy.changed) {
        const basisText = strategy.unchangedBasis.length > 0
          ? strategy.unchangedBasis.join('、')
          : '方案与控制台输出';
        const reason = `重规划后执行思路重复（依据：${basisText}高度重复），已记录并继续在剩余轮次中让模型自修正。`;
        execution.messages.push(`⚠️ ${reason}`);
        emitTaskTrace(taskId, `🧭 ${reason}`);
      }
    }

    const outcomeMessage = judgedOutcome.ok ? `✅ ${judgedOutcome.summary}` : `❌ ${judgedOutcome.summary}`;
    execution.messages.push(outcomeMessage);
    emitTaskTrace(taskId, `🎯 ${judgedOutcome.summary}`);
    if (judgedOutcome.blockerHint) {
      emitTaskTrace(taskId, `🧭 阻塞提示: ${judgedOutcome.blockerHint}`);
    }

    finalExecution = execution;
    finalOutcome = judgedOutcome;

    // Persist latest execution evidence even before terminal status is finalized,
    // so long-running expert review does not leave task metadata empty.
    taskRun.resultLines = finalExecution.messages;
    taskRun.writtenFiles = finalExecution.writtenFiles;
    taskRun.runCommands = finalExecution.runCommands;
    scheduleTaskRunSave(taskRun);

    if (!judgedOutcome.ok) {
      const currentFingerprint = extractPrimaryFailureFingerprint(judgedOutcome, execution);
      if (currentFingerprint && currentFingerprint === previousFailureFingerprint) {
        repeatedFailureCount += 1;
      } else {
        repeatedFailureCount = 0;
      }
      previousFailureFingerprint = currentFingerprint;

      if (repeatedFailureCount >= 2) {
        const summary = '检测到重复失败模式（同类错误连续出现），已提前停止自动重规划以避免无效空转。';
        const hint = '建议先根据失败证据修正前置条件后再继续，例如：先插入父表再插入子表、用真实 CSV 导出参数、先验证行数再生成宽表。';
        finalOutcome = {
          ok: false,
          status: 'blocked',
          confidence: Math.min(judgedOutcome.confidence || 0.5, 0.5),
          reasons: [...(judgedOutcome.reasons || []), summary],
          summary,
          blockerType: 'repeated-failure',
          blockerHint: hint,
        };
        emitTaskTrace(taskId, `🛑 ${summary}`);
        emitTaskTrace(taskId, `🧭 ${hint}`);
        break;
      }
    } else {
      repeatedFailureCount = 0;
      previousFailureFingerprint = '';
    }

    if (judgedOutcome.ok) {
      break;
    }

    const canRetryByBudget = judgedOutcome.status === 'retryable'
      && attempt < maxAttempts
      && (retryWithBudget
        ? (TASK_MAX_AUTO_REPLAN_DURATION_MS <= 0 || (Date.now() - startedAt) <= TASK_MAX_AUTO_REPLAN_DURATION_MS)
        : true);
    if (canRetryByBudget) {
      previousAttemptFailure = {
        summary: judgedOutcome.summary,
        reasons: judgedOutcome.reasons,
        runCommands: execution.runCommands,
        messages: execution.messages,
        planText: fullResponse,
        consoleLines: attemptConsoleLines,
      };
      emitTaskTrace(taskId, `♻️ 第${attempt}轮未通过目标判定，正在自动重规划并重试（下一轮 ${attempt + 1}）`);
      continue;
    }

    break;
  }

  if (!finalExecution && !finalOutcome) {
    const elapsedMs = Date.now() - startedAt;
    const reason = retryWithBudget && TASK_MAX_AUTO_REPLAN_DURATION_MS > 0 && elapsedMs > TASK_MAX_AUTO_REPLAN_DURATION_MS
      ? `已达到自动重规划时间预算（${Math.round(TASK_MAX_AUTO_REPLAN_DURATION_MS / 1000)}秒）`
      : `已达到自动重规划轮次上限（${maxAttempts}轮）`;
    taskRun.status = 'blocked';
    taskRun.error = `任务停止：${reason}`;
    taskRun.summary = buildTaskSummary(taskRun);
    flushTaskRunSave(taskRun);
    post({
      command: 'taskStatus',
      taskId,
      status: 'blocked',
      error: taskRun.error,
      summary: taskRun.summary,
      workspaceDir,
      rerunOfTaskId: taskRun.rerunOfTaskId,
      rerunOfDescription: taskRun.rerunOfDescription,
      followUpOfTaskId: taskRun.followUpOfTaskId,
      followUpOfDescription: taskRun.followUpOfDescription,
    });
    return;
  }

  if (!finalExecution || !finalOutcome) {
    taskRun.status = 'failed';
    taskRun.error = '任务执行未产生有效结果。';
    taskRun.summary = buildTaskSummary(taskRun);
    flushTaskRunSave(taskRun);
    post({
      command: 'taskStatus',
      taskId,
      status: 'failed',
      error: taskRun.error,
      summary: taskRun.summary,
      workspaceDir,
      rerunOfTaskId: taskRun.rerunOfTaskId,
      rerunOfDescription: taskRun.rerunOfDescription,
      followUpOfTaskId: taskRun.followUpOfTaskId,
      followUpOfDescription: taskRun.followUpOfDescription,
    });
    return;
  }

  if (!finalOutcome.ok) {
    const terminalStatus = finalOutcome.status === 'blocked' ? 'blocked' : 'failed';
    taskRun.status = terminalStatus;
    taskRun.error = finalOutcome.blockerHint
      ? `${finalOutcome.summary}\n${finalOutcome.blockerHint}`
      : finalOutcome.summary;
    taskRun.resultLines = finalExecution.messages;
    taskRun.writtenFiles = finalExecution.writtenFiles;
    taskRun.runCommands = finalExecution.runCommands;
    taskRun.summary = buildTaskSummary(taskRun);
    flushTaskRunSave(taskRun);
    post({
      command: 'taskStatus',
      taskId,
      status: terminalStatus,
      result: finalExecution.messages.join('\n'),
      error: taskRun.error,
      summary: taskRun.summary,
      workspaceDir,
      writtenFiles: finalExecution.writtenFiles,
      rerunOfTaskId: taskRun.rerunOfTaskId,
      rerunOfDescription: taskRun.rerunOfDescription,
      followUpOfTaskId: taskRun.followUpOfTaskId,
      followUpOfDescription: taskRun.followUpOfDescription,
    });
    return;
  }

  const expertQuality = await runExpertQualityLoop({
    taskId,
    taskRun,
    objectiveText,
    workspaceDir,
    initialExecution: finalExecution,
  });
  finalExecution = expertQuality.execution;
  if (!expertQuality.ok) {
    const terminalStatus = expertQuality.status === 'blocked' ? 'blocked' : 'failed';
    taskRun.status = terminalStatus;
    taskRun.error = expertQuality.summary;
    taskRun.expertGate = expertQuality.expertGate || taskRun.expertGate || createEmptyExpertGateState();
    taskRun.resultLines = finalExecution.messages;
    taskRun.writtenFiles = finalExecution.writtenFiles;
    taskRun.runCommands = finalExecution.runCommands;
    taskRun.summary = buildTaskSummary(taskRun);
    flushTaskRunSave(taskRun);
    post({
      command: 'taskStatus',
      taskId,
      status: terminalStatus,
      result: finalExecution.messages.join('\n'),
      error: taskRun.error,
      summary: taskRun.summary,
      workspaceDir,
      writtenFiles: finalExecution.writtenFiles,
      expertGate: taskRun.expertGate,
      rerunOfTaskId: taskRun.rerunOfTaskId,
      rerunOfDescription: taskRun.rerunOfDescription,
      followUpOfTaskId: taskRun.followUpOfTaskId,
      followUpOfDescription: taskRun.followUpOfDescription,
    });
    return;
  }

  taskRun.status = 'completed';
  taskRun.expertGate = expertQuality.expertGate || taskRun.expertGate || createEmptyExpertGateState();
  taskRun.resultLines = finalExecution.messages;
  taskRun.writtenFiles = finalExecution.writtenFiles;
  taskRun.runCommands = finalExecution.runCommands;
  taskRun.summary = buildTaskSummary(taskRun);
  flushTaskRunSave(taskRun);
  post({
    command: 'taskStatus',
    taskId,
    status: 'completed',
    result: finalExecution.messages.join('\n'),
    summary: taskRun.summary,
    workspaceDir,
    writtenFiles: finalExecution.writtenFiles,
    expertGate: taskRun.expertGate,
    rerunOfTaskId: taskRun.rerunOfTaskId,
    rerunOfDescription: taskRun.rerunOfDescription,
    followUpOfTaskId: taskRun.followUpOfTaskId,
    followUpOfDescription: taskRun.followUpOfDescription,
  });
}

async function answerTaskFollowUp(taskId, question) {
  let taskRun = getTaskRun(taskId);
  if (!taskRun && taskId) {
    // Backward compatibility: older runs could expose a UI taskId different
    // from persisted taskId. Try prefix matching to recover the latest context.
    const candidates = listKnownTaskRuns();
    const matched = candidates.find((run) => run.taskId && run.taskId.startsWith(taskId.slice(0, 10)));
    if (matched) {
      taskRun = matched;
    }
  }
  if (!taskRun) {
    post({ command: 'chatDone', taskId });
    post({ command: 'chatError', error: '未找到该任务的上下文，无法继续追问。' });
    return;
  }

  if (!question || !question.trim()) {
    post({ command: 'chatDone', taskId });
    post({ command: 'chatError', error: '追问内容不能为空。' });
    return;
  }

  if (taskRun.status === 'running') {
    post({ command: 'chatDone', taskId });
    post({ command: 'chatError', error: '任务仍在运行中，请等待完成后再追问。' });
    return;
  }

  emitTaskTrace(taskId, `💬 追问: ${question}`);

  if (looksLikeActionableFollowUp(question)) {
    const { generateTaskId } = require('./lib/utils');
    const continuationTaskId = generateTaskId();
    post({ command: 'chatDone', taskId });
    post({
      command: 'followUpTaskStarted',
      sourceTaskId: taskRun.taskId,
      taskId: continuationTaskId,
      question,
    });
    await prepareTaskExecution(continuationTaskId, question, {
      followUpOfTaskId: taskRun.taskId,
      followUpOfDescription: taskRun.description,
      cloneWorkspaceFrom: taskRun.workspaceDir,
      buildPrompt: (workspaceDir) => buildTaskContinuationPrompt(taskRun, question, workspaceDir),
      buildPlanPrompt: (description) => [
        buildTaskPlanningPrompt(description, {
          followUpOfDescription: taskRun.description,
        }),
        `已有任务摘要:\n${taskRun.summary || buildTaskSummary(taskRun)}`,
      ].join('\n\n'),
      autoSelect: true,
    });
    return;
  }

  try {
    const model = await require('./lib/ollama').getModel();
    await streamOllama(
      buildTaskFollowUpPrompt(taskRun, question),
      (token) => post({ command: 'chatToken', token, taskId }),
      model,
    );
    post({ command: 'chatDone', taskId });
  } catch (err) {
    post({ command: 'chatDone', taskId });
    post({ command: 'chatError', error: `任务追问失败: ${err?.message || String(err)}` });
  }
}

// ---------------------------------------------------------------------------
// File serving
// ---------------------------------------------------------------------------

function serveTaskFile(res, taskId, requestedPath, download, headOnly = false) {
  const taskRun = getTaskRun(taskId);
  if (!taskRun) {
    res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('未找到该任务');
    return;
  }

  const filePath = resolveTaskPreviewPath(taskRun, requestedPath);
  if (!filePath || !fs.existsSync(filePath)) {
    res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('文件不存在');
    return;
  }

  try {
    const stat = fs.statSync(filePath);
    const filename = path.basename(filePath).replace(/"/g, '');
    res.writeHead(200, {
      'Content-Type': getTaskFileMime(filePath),
      'Content-Length': stat.size,
      'Content-Disposition': `${download ? 'attachment' : 'inline'}; filename="${filename}"`,
    });
    if (headOnly) {
      res.end();
      return;
    }
    fs.createReadStream(filePath).pipe(res);
  } catch (err) {
    res.writeHead(500, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end(`读取文件失败: ${err?.message}`);
  }
}

async function previewTaskFile(taskId, requestedPath) {
  const taskRun = getTaskRun(taskId);
  if (!taskRun) {
    post({ command: 'taskFilePreview', taskId, filePath: requestedPath, error: '未找到该任务，无法预览文件。' });
    return;
  }

  const filePath = resolveTaskPreviewPath(taskRun, requestedPath);
  if (!filePath || !fs.existsSync(filePath)) {
    post({ command: 'taskFilePreview', taskId, filePath: requestedPath, error: '文件不存在或不在任务工作目录内。' });
    return;
  }

  try {
    const buffer = fs.readFileSync(filePath);
    if (buffer.includes(0)) {
      post({
        command: 'taskFilePreview',
        taskId,
        filePath,
        displayPath: toWorkspaceRelativePath(taskRun.workspaceDir, filePath),
        error: '该文件是二进制内容，暂不支持直接预览。',
      });
      return;
    }

    const truncated = buffer.length > TASK_FILE_PREVIEW_MAX_BYTES;
    const content = buffer.subarray(0, TASK_FILE_PREVIEW_MAX_BYTES).toString('utf8');
    emitTaskTrace(taskId, `👁️ 预览文件: ${toWorkspaceRelativePath(taskRun.workspaceDir, filePath)}`);
    post({
      command: 'taskFilePreview',
      taskId,
      filePath,
      displayPath: toWorkspaceRelativePath(taskRun.workspaceDir, filePath),
      content,
      truncated,
    });
  } catch (err) {
    post({
      command: 'taskFilePreview',
      taskId,
      filePath,
      error: `读取文件失败: ${err?.message || String(err)}`,
    });
  }
}

async function revealTaskFile(taskId, requestedPath) {
  const taskRun = getTaskRun(taskId);
  if (!taskRun) {
    post({
      command: 'fileActionResult',
      action: 'reveal',
      taskId,
      filePath: requestedPath,
      ok: false,
      message: '未找到该任务，无法在文件管理器中显示文件。',
    });
    return;
  }

  const filePath = resolveTaskPreviewPath(taskRun, requestedPath);
  if (!filePath || !fs.existsSync(filePath)) {
    post({
      command: 'fileActionResult',
      action: 'reveal',
      taskId,
      filePath: requestedPath,
      ok: false,
      message: '文件不存在或不在任务工作目录内。',
    });
    return;
  }

  try {
    await revealFileInFileManager(filePath);
    const displayPath = toWorkspaceRelativePath(taskRun.workspaceDir, filePath);
    emitTaskTrace(taskId, `🗂️ 在文件管理器中显示: ${displayPath}`);
    post({
      command: 'fileActionResult',
      action: 'reveal',
      taskId,
      filePath,
      ok: true,
      message: `已在文件管理器中定位: ${displayPath}`,
    });
  } catch (err) {
    post({
      command: 'fileActionResult',
      action: 'reveal',
      taskId,
      filePath,
      ok: false,
      message: `打开文件管理器失败: ${err?.message || String(err)}`,
    });
  }
}

function revealFileInFileManager(filePath) {
  return new Promise((resolve, reject) => {
    let command;
    let args;

    if (process.platform === 'darwin') {
      command = 'open';
      args = ['-R', filePath];
    } else if (process.platform === 'win32') {
      command = 'explorer.exe';
      args = ['/select,', path.normalize(filePath)];
    } else {
      command = 'xdg-open';
      args = [path.dirname(filePath)];
    }

    const child = spawn(command, args, {
      detached: true,
      stdio: 'ignore',
    });

    child.once('error', reject);
    child.once('spawn', () => {
      child.unref();
      resolve();
    });
  });
}

// ---------------------------------------------------------------------------
// Hints
// ---------------------------------------------------------------------------

function loadHints(query) {
  try {
    const hintsPath = path.join(__dirname, '..', 'cloudwbot', 'hints.json');
    if (!fs.existsSync(hintsPath)) return [];
    const hints = JSON.parse(fs.readFileSync(hintsPath, 'utf8'));
    if (!query) return hints;
    const q = query.toLowerCase();
    return hints.filter(
      (h) => h.category.toLowerCase().includes(q) || h.content.toLowerCase().includes(q),
    );
  } catch { return []; }
}

// ---------------------------------------------------------------------------
// GitHub search (public API, unauthenticated)
// ---------------------------------------------------------------------------

function searchGithub(query) {
  return new Promise((resolve) => {
    const req = https.get(
      {
        hostname: 'api.github.com',
        path: `/search/repositories?q=${encodeURIComponent(query)}&per_page=10`,
        headers: { 'User-Agent': 'CloudWBot/0.1' },
      },
      (res) => {
        let data = '';
        res.on('data', (c) => (data += c));
        res.on('end', () => {
          try {
            resolve(
              (JSON.parse(data).items || []).map((item) => ({
                name: item.full_name,
                description: item.description || '',
                url: item.html_url,
                stars: item.stargazers_count,
              })),
            );
          } catch { resolve([]); }
        });
      },
    );
    req.on('error', () => resolve([]));
    req.setTimeout(10000, () => { req.destroy(); resolve([]); });
  });
}
