// @ts-check
'use strict';

const path = require('path');
const fs = require('fs');
const os = require('os');

// Try to load .env file
const envPath = path.join(process.cwd(), '.env');
if (fs.existsSync(envPath)) {
  const envContent = fs.readFileSync(envPath, 'utf8');
  for (const line of envContent.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const eqIndex = trimmed.indexOf('=');
    if (eqIndex === -1) continue;
    const key = trimmed.slice(0, eqIndex).trim();
    let value = trimmed.slice(eqIndex + 1).trim();
    // Remove quotes
    if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
      value = value.slice(1, -1);
    }
    if (!process.env[key]) {
      process.env[key] = value;
    }
  }
}

const PORT = process.env.PORT ? parseInt(process.env.PORT, 10) : 3000;
const OLLAMA_URL = (process.env.OLLAMA_URL || 'http://localhost:11434').replace(/\/$/, '');
const OLLAMA_MODEL = (process.env.OLLAMA_MODEL || '').trim();
const CLOUDWBOT_WORKSPACE = (process.env.CLOUDWBOT_WORKSPACE || '').trim();
const TASK_NON_FATAL_COMMAND_PREFIXES = (process.env.TASK_NON_FATAL_COMMAND_PREFIXES || 'open,xdg-open,start,explorer.exe,code,code-insiders')
  .split(',')
  .map((item) => item.trim().toLowerCase())
  .filter(Boolean);

// Constants
const TASK_META_FILENAME = '.cloudwbot-task.json';
const TASK_FILE_PREVIEW_MAX_BYTES = 24 * 1024;
const TASK_MAX_COUNT = 100; // Max tasks to keep in memory
const TASK_MAX_AGE_DAYS = 30; // Max age for task files

// MIME types
const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.txt': 'text/plain; charset=utf-8',
  '.md': 'text/markdown; charset=utf-8',
  '.log': 'text/plain; charset=utf-8',
  '.java': 'text/plain; charset=utf-8',
  '.py': 'text/plain; charset=utf-8',
  '.ts': 'text/plain; charset=utf-8',
  '.tsx': 'text/plain; charset=utf-8',
  '.jsx': 'text/plain; charset=utf-8',
  '.yml': 'text/plain; charset=utf-8',
  '.yaml': 'text/plain; charset=utf-8',
  '.sh': 'text/plain; charset=utf-8',
  '.ico': 'image/x-icon',
};

function directoryHasProjectMarker(dir) {
  return fs.existsSync(path.join(dir, 'package.json')) || fs.existsSync(path.join(dir, '.git'));
}

function findProjectRoot(startDir) {
  let current = path.resolve(startDir);
  while (true) {
    if (directoryHasProjectMarker(current)) {
      return current;
    }
    const parent = path.dirname(current);
    if (parent === current) {
      return '';
    }
    current = parent;
  }
}

function resolveAppRoot() {
  const serverDir = __dirname;
  const candidateRoots = [...new Set([process.cwd(), serverDir]
    .map((candidate) => findProjectRoot(candidate))
    .filter(Boolean))]
    .filter((candidate) => {
      const relativeToServer = path.relative(candidate, path.join(serverDir, '..', 'server.js'));
      return relativeToServer && !relativeToServer.startsWith('..') && !path.isAbsolute(relativeToServer);
    })
    .sort((left, right) => right.length - left.length);

  return candidateRoots[0] || path.resolve(serverDir, '..', '..');
}

const APP_ROOT = resolveAppRoot();

function getConfiguredWorkspaceRoot() {
  if (CLOUDWBOT_WORKSPACE) {
    return path.resolve(APP_ROOT, CLOUDWBOT_WORKSPACE);
  }
  return '';
}

function getTaskStorageRoots() {
  const configuredRoot = getConfiguredWorkspaceRoot();
  const roots = configuredRoot
    ? [configuredRoot]
    : [path.join(APP_ROOT, '.cloudwbot_workspace'), path.join(os.homedir(), 'cloudwbot_workspace')];
  return [...new Set(roots.map((root) => path.resolve(root)))];
}

function createTaskWorkspace(taskId) {
  const configuredRoot = getConfiguredWorkspaceRoot();
  const candidateRoots = configuredRoot
    ? [configuredRoot]
    : [path.join(APP_ROOT, '.cloudwbot_workspace'), path.join(os.homedir(), 'cloudwbot_workspace')];

  let lastError;
  for (let index = 0; index < candidateRoots.length; index += 1) {
    const root = candidateRoots[index];
    const workspaceDir = path.join(root, 'tasks', taskId);
    try {
      fs.mkdirSync(workspaceDir, { recursive: true });
      return { workspaceDir, usedFallback: !configuredRoot && index > 0 };
    } catch (error) {
      lastError = error;
    }
  }

  throw lastError;
}

module.exports = {
  PORT,
  OLLAMA_URL,
  OLLAMA_MODEL,
  CLOUDWBOT_WORKSPACE,
  TASK_NON_FATAL_COMMAND_PREFIXES,
  APP_ROOT,
  TASK_META_FILENAME,
  TASK_FILE_PREVIEW_MAX_BYTES,
  TASK_MAX_COUNT,
  TASK_MAX_AGE_DAYS,
  MIME,
  getConfiguredWorkspaceRoot,
  getTaskStorageRoots,
  createTaskWorkspace,
};
