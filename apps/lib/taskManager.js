// @ts-check
'use strict';

const path = require('path');
const fs = require('fs');
const { TASK_META_FILENAME, TASK_MAX_COUNT, TASK_MAX_AGE_DAYS, getTaskStorageRoots, createTaskWorkspace } = require('./config');
const { humanTaskRunStatus, generateTaskId } = require('./utils');

const URI_CREDENTIAL_RE = /(\b[a-z][a-z0-9+.-]*:\/\/[^\s:/?#]+:)([^\s/]+)@/ig;
const PASSWORD_ASSIGNMENT_RE = /(\b(?:password|passwd|pwd|token|secret|api[_-]?key)\b\s*[:=]\s*)([^\s,;"'`]+)/ig;
const CHINESE_PASSWORD_RE = /(密码\s*(?:是|为|:|：)?\s*)([^\s，。,;；"'`]+)/g;

function redactSensitiveText(input) {
  const text = String(input || '');
  if (!text) return text;
  return text
    .replace(URI_CREDENTIAL_RE, (_, prefix) => `${prefix}***@`)
    .replace(PASSWORD_ASSIGNMENT_RE, (_, prefix) => `${prefix}***`)
    .replace(CHINESE_PASSWORD_RE, (_, prefix) => `${prefix}***`);
}

function redactSensitiveStringArray(values) {
  return Array.isArray(values)
    ? values.map((item) => redactSensitiveText(item))
    : [];
}

function sanitizeExpertGate(expertGate) {
  const input = expertGate && typeof expertGate === 'object' ? expertGate : {};
  const categoryTotals = {};
  for (const [key, value] of Object.entries(input.categoryTotals || {})) {
    if (!key) continue;
    const count = Number(value);
    if (!Number.isFinite(count) || count < 0) continue;
    categoryTotals[key] = Math.floor(count);
  }
  return {
    reasons: Array.isArray(input.reasons) ? input.reasons.filter(Boolean).map((item) => String(item)) : [],
    categories: Array.isArray(input.categories) ? input.categories.filter(Boolean).map((item) => String(item)) : [],
    categoryTotals,
    lastCategory: typeof input.lastCategory === 'string' ? input.lastCategory : '',
    round: Number.isFinite(Number(input.round)) ? Number(input.round) : 0,
  };
}

// In-memory task store
const taskRuns = new Map();

/**
 * Revive a task run from snapshot
 * @param {object} snapshot
 * @returns {object}
 */
function reviveTaskRun(snapshot) {
  const resultLines = Array.isArray(snapshot?.resultLines)
    ? snapshot.resultLines
    : typeof snapshot?.result === 'string' && snapshot.result
      ? snapshot.result.split('\n')
      : [];

  return {
    taskId: snapshot?.taskId || '',
    description: snapshot?.description || '未命名任务',
    rerunOfTaskId: snapshot?.rerunOfTaskId || '',
    rerunOfDescription: snapshot?.rerunOfDescription || '',
    followUpOfTaskId: snapshot?.followUpOfTaskId || '',
    followUpOfDescription: snapshot?.followUpOfDescription || '',
    workspaceDir: snapshot?.workspaceDir || '',
    usedFallback: Boolean(snapshot?.usedFallback),
    createdAt: snapshot?.createdAt || new Date().toISOString(),
    status: snapshot?.status || 'pending',
    aiResponse: snapshot?.aiResponse || '',
    aiOutput: snapshot?.aiOutput || '',
    resultLines,
    writtenFiles: Array.isArray(snapshot?.writtenFiles) ? snapshot.writtenFiles : [],
    runCommands: Array.isArray(snapshot?.runCommands) ? snapshot.runCommands : [],
    trace: Array.isArray(snapshot?.trace) ? snapshot.trace : [],
    consoleLines: Array.isArray(snapshot?.consoleLines) ? snapshot.consoleLines : [],
    expertGate: sanitizeExpertGate(snapshot?.expertGate),
    error: snapshot?.error || '',
    summary: snapshot?.summary || '',
    persistTimer: null,
  };
}

/**
 * Serialize task run for storage
 * @param {object} taskRun
 * @returns {object}
 */
function serializeTaskRun(taskRun) {
  const redactedDescription = redactSensitiveText(taskRun.description);
  const redactedAiResponse = redactSensitiveText(taskRun.aiResponse);
  const redactedAiOutput = redactSensitiveText(taskRun.aiOutput);
  const redactedResultLines = redactSensitiveStringArray(taskRun.resultLines);
  const redactedTrace = redactSensitiveStringArray(taskRun.trace);
  const redactedConsoleLines = redactSensitiveStringArray(taskRun.consoleLines);
  const sanitizedExpertGate = sanitizeExpertGate(taskRun.expertGate);
  const redactedError = redactSensitiveText(taskRun.error);
  const redactedSummary = redactSensitiveText(taskRun.summary);

  return {
    taskId: taskRun.taskId,
    description: redactedDescription,
    rerunOfTaskId: taskRun.rerunOfTaskId,
    rerunOfDescription: taskRun.rerunOfDescription,
    followUpOfTaskId: taskRun.followUpOfTaskId,
    followUpOfDescription: taskRun.followUpOfDescription,
    workspaceDir: taskRun.workspaceDir,
    usedFallback: taskRun.usedFallback,
    createdAt: taskRun.createdAt,
    status: taskRun.status,
    aiResponse: redactedAiResponse,
    aiOutput: redactedAiOutput,
    result: redactedResultLines.join('\n'),
    resultLines: redactedResultLines,
    writtenFiles: taskRun.writtenFiles,
    runCommands: taskRun.runCommands,
    trace: redactedTrace,
    consoleLines: redactedConsoleLines,
    expertGate: {
      ...sanitizedExpertGate,
      reasons: redactSensitiveStringArray(sanitizedExpertGate.reasons),
    },
    error: redactedError,
    summary: redactedSummary,
  };
}

/**
 * Write task run snapshot to disk
 * @param {object} taskRun
 */
function writeTaskRunSnapshot(taskRun) {
  if (!taskRun?.workspaceDir) return;
  try {
    fs.mkdirSync(taskRun.workspaceDir, { recursive: true });
    fs.writeFileSync(
      path.join(taskRun.workspaceDir, TASK_META_FILENAME),
      JSON.stringify(serializeTaskRun(taskRun), null, 2),
      'utf8',
    );
  } catch (err) {
    console.error('[taskManager] Failed to write task snapshot:', err?.message);
  }
}

/**
 * Schedule deferred save
 * @param {object} taskRun
 */
function scheduleTaskRunSave(taskRun) {
  if (!taskRun) return;
  if (taskRun.persistTimer) return;
  taskRun.persistTimer = setTimeout(() => {
    taskRun.persistTimer = null;
    writeTaskRunSnapshot(taskRun);
  }, 120);
}

/**
 * Flush pending save immediately
 * @param {object} taskRun
 */
function flushTaskRunSave(taskRun) {
  if (!taskRun) return;
  if (taskRun.persistTimer) {
    clearTimeout(taskRun.persistTimer);
    taskRun.persistTimer = null;
  }
  writeTaskRunSnapshot(taskRun);
}

/**
 * Clean old task files
 */
function cleanupOldTasks() {
  const maxAgeMs = TASK_MAX_AGE_DAYS * 24 * 60 * 60 * 1000;
  const now = Date.now();
  
  for (const root of getTaskStorageRoots()) {
    const tasksDir = path.join(root, 'tasks');
    if (!fs.existsSync(tasksDir)) continue;
    
    for (const entry of fs.readdirSync(tasksDir, { withFileTypes: true })) {
      if (!entry.isDirectory()) continue;
      
      const taskDir = path.join(tasksDir, entry.name);
      const metaPath = path.join(taskDir, TASK_META_FILENAME);
      
      try {
        const stats = fs.statSync(metaPath);
        if (now - stats.mtime.getTime() > maxAgeMs) {
          // Remove old task directory
          fs.rmSync(taskDir, { recursive: true, force: true });
          console.log(`[taskManager] Cleaned up old task: ${entry.name}`);
        }
      } catch {
        // Ignore errors
      }
    }
  }
}

/**
 * Read task run snapshot from disk
 * @param {string} metaPath
 * @returns {object|null}
 */
function readTaskRunSnapshot(metaPath) {
  try {
    if (!fs.existsSync(metaPath)) return null;
    const rawSnapshot = JSON.parse(fs.readFileSync(metaPath, 'utf8'));
    const taskRun = reviveTaskRun(rawSnapshot);
    let changed = false;

    const redactFields = [
      ['description', redactSensitiveText],
      ['aiResponse', redactSensitiveText],
      ['aiOutput', redactSensitiveText],
      ['error', redactSensitiveText],
      ['summary', redactSensitiveText],
    ];
    for (const [field, transform] of redactFields) {
      const original = taskRun[field] || '';
      const redacted = transform(original);
      if (original !== redacted) {
        taskRun[field] = redacted;
        changed = true;
      }
    }
    const redactArrayFields = ['resultLines', 'trace', 'consoleLines'];
    for (const field of redactArrayFields) {
      const original = Array.isArray(taskRun[field]) ? taskRun[field] : [];
      const redacted = redactSensitiveStringArray(original);
      const same = original.length === redacted.length && original.every((value, index) => value === redacted[index]);
      if (!same) {
        taskRun[field] = redacted;
        changed = true;
      }
    }

    const originalExpertGate = taskRun.expertGate;
    const sanitizedExpertGate = sanitizeExpertGate(originalExpertGate);
    if (JSON.stringify(originalExpertGate || {}) !== JSON.stringify(sanitizedExpertGate)) {
      taskRun.expertGate = sanitizedExpertGate;
      changed = true;
    }

    if (rawSnapshot?.status === 'running') {
      taskRun.status = 'interrupted';
      if (!taskRun.error) {
        taskRun.error = '任务在服务重启前中断';
      }
      if (!taskRun.trace.includes('⚠️ 检测到服务重启，该任务已标记为中断')) {
        taskRun.trace.push('⚠️ 检测到服务重启，该任务已标记为中断');
      }
      if (!taskRun.consoleLines.includes('⚠️ 服务重启后该任务被标记为中断')) {
        taskRun.consoleLines.push('⚠️ 服务重启后该任务被标记为中断');
      }
      changed = true;
    }

    if (!taskRun.summary) {
      taskRun.summary = buildTaskSummary(taskRun);
      changed = true;
    }

    if (changed) {
      writeTaskRunSnapshot(taskRun);
    }

    return taskRun;
  } catch {
    return null;
  }
}

/**
 * Get task run by ID
 * @param {string} taskId
 * @returns {object|null}
 */
function getTaskRun(taskId) {
  if (taskRuns.has(taskId)) return taskRuns.get(taskId);
  for (const root of getTaskStorageRoots()) {
    const snapshot = readTaskRunSnapshot(path.join(root, 'tasks', taskId, TASK_META_FILENAME));
    if (snapshot) {
      taskRuns.set(taskId, snapshot);
      return snapshot;
    }
  }
  return null;
}

/**
 * List all known task runs
 * @returns {object[]}
 */
function listKnownTaskRuns() {
  const runsById = new Map(taskRuns);

  for (const root of getTaskStorageRoots()) {
    const tasksDir = path.join(root, 'tasks');
    if (!fs.existsSync(tasksDir)) continue;

    for (const entry of fs.readdirSync(tasksDir, { withFileTypes: true })) {
      if (!entry.isDirectory()) continue;
      const snapshot = readTaskRunSnapshot(path.join(tasksDir, entry.name, TASK_META_FILENAME));
      if (!snapshot || !snapshot.taskId) continue;
      if (!runsById.has(snapshot.taskId)) {
        runsById.set(snapshot.taskId, snapshot);
        taskRuns.set(snapshot.taskId, snapshot);
      }
    }
  }

  // Limit in-memory tasks
  const sorted = [...runsById.values()].sort((left, right) => {
    return new Date(right.createdAt).getTime() - new Date(left.createdAt).getTime();
  });
  
  // Keep only recent tasks in memory
  if (sorted.length > TASK_MAX_COUNT) {
    const toRemove = sorted.slice(TASK_MAX_COUNT);
    for (const task of toRemove) {
      taskRuns.delete(task.taskId);
    }
  }

  return sorted.slice(0, TASK_MAX_COUNT);
}

/**
 * Format rerun source
 * @param {object} taskRun
 * @returns {string}
 */
function formatRerunSource(taskRun) {
  if (!taskRun?.rerunOfTaskId) return '';
  if (taskRun.rerunOfDescription) {
    return `任务"${taskRun.rerunOfDescription}" (${taskRun.rerunOfTaskId})`;
  }
  return `任务 ${taskRun.rerunOfTaskId}`;
}

/**
 * Format follow-up source
 * @param {object} taskRun
 * @returns {string}
 */
function formatFollowUpSource(taskRun) {
  if (!taskRun?.followUpOfTaskId) return '';
  if (taskRun.followUpOfDescription) {
    return `任务"${taskRun.followUpOfDescription}" (${taskRun.followUpOfTaskId})`;
  }
  return `任务 ${taskRun.followUpOfTaskId}`;
}

/**
 * Convert path to workspace-relative
 * @param {string} workspaceDir
 * @param {string} filePath
 * @returns {string}
 */
function toWorkspaceRelativePath(workspaceDir, filePath) {
  const rel = path.relative(workspaceDir, filePath);
  if (!rel || rel.startsWith('..') || path.isAbsolute(rel)) return filePath;
  return rel;
}

/**
 * Build task summary
 * @param {object} taskRun
 * @returns {string}
 */
function buildTaskSummary(taskRun) {
  const lines = [`工作目录: ${taskRun.workspaceDir}`];
  const rerunSource = formatRerunSource(taskRun);
  const followUpSource = formatFollowUpSource(taskRun);
  if (rerunSource) {
    lines.unshift(`重跑来源: ${rerunSource}`);
  }
  if (followUpSource) {
    lines.unshift(`继续来源: ${followUpSource}`);
  }
  const files = taskRun.writtenFiles.map((filePath) => toWorkspaceRelativePath(taskRun.workspaceDir, filePath));
  if (files.length > 0) lines.push(`生成文件: ${files.join(', ')}`);
  if (taskRun.runCommands.length > 0) lines.push(`执行命令: ${taskRun.runCommands.join(' ; ')}`);

  const highlights = taskRun.resultLines
    .map((line) => line.replace(/\s+/g, ' ').trim())
    .filter(Boolean)
    .filter((line) => /✅|❌|⚠️|ℹ️/.test(line));

  if (highlights.length > 0) {
    lines.push(`关键结果: ${highlights.slice(-3).join('；')}`);
  } else if (taskRun.error) {
    lines.push(`关键结果: ${taskRun.error.replace(/\s+/g, ' ').trim()}`);
  }

  lines.push(`状态: ${humanTaskRunStatus(taskRun.status)}`);
  return lines.join('\n');
}

/**
 * Copy workspace contents
 * @param {string} sourceDir
 * @param {string} targetDir
 */
function copyWorkspaceContents(sourceDir, targetDir) {
  if (!sourceDir || !fs.existsSync(sourceDir)) return;
  for (const entry of fs.readdirSync(sourceDir, { withFileTypes: true })) {
    if (entry.name === TASK_META_FILENAME) continue;
    const sourcePath = path.join(sourceDir, entry.name);
    const targetPath = path.join(targetDir, entry.name);
    try {
      fs.cpSync(sourcePath, targetPath, { recursive: true, force: true });
    } catch (err) {
      console.error('[taskManager] Failed to copy workspace file:', err?.message);
    }
  }
}

/**
 * Create a new task run
 * @param {string} description
 * @param {object} options
 * @returns {object}
 */
function createTaskRun(description, options = {}) {
  const preferredTaskId = typeof options.taskId === 'string' ? options.taskId.trim() : '';
  const taskId = preferredTaskId || generateTaskId();
  const { workspaceDir, usedFallback } = createTaskWorkspace(taskId);
  
  const taskRun = {
    taskId,
    description,
    rerunOfTaskId: options.rerunOfTaskId || '',
    rerunOfDescription: options.rerunOfDescription || '',
    followUpOfTaskId: options.followUpOfTaskId || '',
    followUpOfDescription: options.followUpOfDescription || '',
    workspaceDir,
    usedFallback,
    createdAt: new Date().toISOString(),
    status: 'running',
    aiResponse: '',
    aiOutput: '',
    resultLines: [],
    writtenFiles: [],
    runCommands: [],
    trace: [],
    consoleLines: [],
    expertGate: sanitizeExpertGate(),
    error: '',
    summary: '',
    persistTimer: null,
  };
  
  taskRuns.set(taskId, taskRun);
  flushTaskRunSave(taskRun);
  
  return { taskRun, taskId, workspaceDir, usedFallback };
}

/**
 * Convert task run to client format
 * @param {object} taskRun
 * @returns {object}
 */
function toClientTask(taskRun) {
  return {
    id: taskRun.taskId,
    taskId: taskRun.taskId,
    description: redactSensitiveText(taskRun.description),
    rerunOfTaskId: taskRun.rerunOfTaskId,
    rerunOfDescription: taskRun.rerunOfDescription,
    followUpOfTaskId: taskRun.followUpOfTaskId,
    followUpOfDescription: taskRun.followUpOfDescription,
    status: taskRun.status,
    createdAt: taskRun.createdAt,
    result: redactSensitiveStringArray(taskRun.resultLines).join('\n'),
    error: redactSensitiveText(taskRun.error),
    summary: redactSensitiveText(taskRun.summary),
    workspaceDir: taskRun.workspaceDir,
    aiOutput: redactSensitiveText(taskRun.aiOutput),
    writtenFiles: taskRun.writtenFiles,
    trace: redactSensitiveStringArray(taskRun.trace),
    consoleLines: redactSensitiveStringArray(taskRun.consoleLines),
    expertGate: sanitizeExpertGate(taskRun.expertGate),
  };
}

/**
 * Resolve task preview path
 * @param {object} taskRun
 * @param {string} requestedPath
 * @returns {string}
 */
function resolveTaskPreviewPath(taskRun, requestedPath) {
  if (!taskRun?.workspaceDir || !requestedPath) return '';
  const candidate = path.resolve(taskRun.workspaceDir, requestedPath);
  const rel = path.relative(taskRun.workspaceDir, candidate);
  const isInsideWorkspace = rel && !rel.startsWith('..') && !path.isAbsolute(rel);
  if (isInsideWorkspace) return candidate;

  const writtenMatch = taskRun.writtenFiles.find((filePath) => {
    return filePath === requestedPath || toWorkspaceRelativePath(taskRun.workspaceDir, filePath) === requestedPath;
  });
  return writtenMatch || '';
}

// Cleanup old tasks on module load
cleanupOldTasks();

module.exports = {
  taskRuns,
  getTaskRun,
  listKnownTaskRuns,
  createTaskRun,
  toClientTask,
  writeTaskRunSnapshot,
  scheduleTaskRunSave,
  flushTaskRunSave,
  buildTaskSummary,
  formatRerunSource,
  formatFollowUpSource,
  toWorkspaceRelativePath,
  copyWorkspaceContents,
  resolveTaskPreviewPath,
  cleanupOldTasks,
  redactSensitiveText,
};
