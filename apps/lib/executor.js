// @ts-check
'use strict';

const path = require('path');
const fs = require('fs');
const { spawn } = require('child_process');
const { PORT, TASK_NON_FATAL_COMMAND_PREFIXES } = require('./config');
const { addUniqueString, stripShellToken, isOpenCommand, buildOpenCommand, selectAutoOpenTarget, isLikelyGuiJavaSource, scoreStandaloneCodeBlock, inferStandaloneFilename, looksLikeOpenRequest } = require('./utils');

const LEGACY_THREE_SCRIPT_URL = 'https://cdnjs.cloudflare.com/ajax/libs/three.js/r128/three.min.js';
const LEGACY_ORBITCONTROLS_SCRIPT_URL = 'https://cdn.jsdelivr.net/npm/three@0.128.0/examples/js/controls/OrbitControls.js';
const LEGACY_THREE_SCRIPT_TAG = `<script src="${LEGACY_THREE_SCRIPT_URL}"></script>`;
const LEGACY_ORBITCONTROLS_SCRIPT_TAG = `<script src="${LEGACY_ORBITCONTROLS_SCRIPT_URL}"></script>`;

// Default timeouts (in milliseconds)
const DEFAULT_SHELL_TIMEOUT = 60 * 1000; // 60 seconds
const DEFAULT_JAVA_TIMEOUT = 30 * 1000; // 30 seconds

function tokenizeShellCommand(command) {
  return (String(command || '').match(/"[^"]*"|'[^']*'|\S+/g) || []).map(stripShellToken);
}

function normalizeStandaloneShellCommand(rawLine) {
  let normalized = String(rawLine || '').trim();
  normalized = normalized.replace(/^\$\s*/, '').trim();
  normalized = normalized.replace(/^RUN:\s*/i, '').trim();
  return normalized;
}

function normalizeInteractiveCliCommand(command) {
  const raw = String(command || '').trim();
  if (!raw) return raw;

  const tokens = tokenizeShellCommand(raw);
  const executable = String(tokens[0] || '').toLowerCase();
  if (executable !== 'psql') return raw;

  if (/\s(?:--version|-V|--help|-\?)(\s|$)/.test(raw)) {
    return raw;
  }

  let rewritten = raw;
  if (/\s-W(\s|$)/.test(raw)) {
    rewritten = rewritten.replace(/\s-W(\s|$)/g, ' ');
    // Force non-interactive mode to avoid hanging on password prompt.
    if (!/\s-w(\s|$)/.test(rewritten)) {
      rewritten = `${rewritten.trim()} -w`;
    }
  }

  const isTableDiscoveryQuery = /\bselect\b[\s\S]*\btable_name\b[\s\S]*\binformation_schema\.tables\b/i.test(rewritten);
  if (isTableDiscoveryQuery) {
    // Emit plain table names for downstream shell scripts, without headers/formatting noise.
    if (!/\s-t(\s|$)/.test(rewritten)) {
      rewritten = `${rewritten.trim()} -t`;
    }
    if (!/\s-A(\s|$)/.test(rewritten)) {
      rewritten = `${rewritten.trim()} -A`;
    }
  }

  const hasDbFlag = /\s(?:-d|--dbname)\s+\S+/i.test(rewritten);
  const hasConnUri = /\bpostgres(?:ql)?:\/\//i.test(rewritten);
  const positionalDbMatch = /^\s*psql\s+(['"])?(?!-)([^'"\s]+)\1?/i.exec(rewritten);
  const hasPositionalDb = Boolean(positionalDbMatch && positionalDbMatch[2]);
  if (!hasDbFlag && !hasConnUri && !hasPositionalDb) {
    rewritten = `${rewritten.trim()} -d postgres`;
  }

  // Force SQL errors to return non-zero exit code to avoid false-positive success.
  if (!/\bON_ERROR_STOP\b/i.test(rewritten)) {
    rewritten = `${rewritten.trim()} -v ON_ERROR_STOP=1`;
  }

  return rewritten.trim();
}

function normalizeServiceProbeCommand(command) {
  const raw = String(command || '').trim();
  if (!raw) {
    return { command: raw, rewritten: false };
  }

  if (process.platform === 'darwin') {
    if (/^(?:sudo\s+)?systemctl\s+status\s+postgresql?(?:\s|$)/i.test(raw)) {
      return { command: 'pg_isready', rewritten: true };
    }
  }

  return { command: raw, rewritten: false };
}

function normalizeInlinePythonCommand(command) {
  const raw = String(command || '').trim();
  if (!raw) {
    return { command: raw, rewritten: false };
  }

  if (!/^python(?:\d+(?:\.\d+)?)?\b/i.test(raw)) {
    return { command: raw, rewritten: false };
  }

  const compoundStmtRe = /;\s*(with|for|while|if|try|class|def)\b/g;
  const tryRewrite = (input, quote) => {
    const escapedQuote = quote === '"' ? '"' : "'";
    const pattern = new RegExp(`-c\\s+${escapedQuote}([\\s\\S]*?)${escapedQuote}([\\s\\S]*)$`);
    const match = input.match(pattern);
    if (!match) return input;

    const script = String(match[1] || '');
    const tail = String(match[2] || '');
    const fixedScript = script.replace(compoundStmtRe, '\n$1');
    if (fixedScript === script) return input;

    const start = match.index || 0;
    const prefix = input.slice(0, start);
    return `${prefix}-c ${quote}${fixedScript}${quote}${tail}`;
  };

  const afterDoubleQuoteTry = tryRewrite(raw, '"');
  const rewritten = afterDoubleQuoteTry === raw ? tryRewrite(raw, "'") : afterDoubleQuoteTry;
  return { command: rewritten, rewritten: rewritten !== raw };
}

function normalizeFailFastCompositeCommand(command) {
  const raw = String(command || '').trim();
  if (!raw) return { command: raw, rewritten: false };
  if (!raw.includes(';')) return { command: raw, rewritten: false };
  if (/^\s*set\s+-e\b/.test(raw)) return { command: raw, rewritten: false };
  // Avoid touching shell control-flow snippets where semicolons are part of syntax.
  if (/\b(for|while|if|case|until|select|function)\b/i.test(raw)) {
    return { command: raw, rewritten: false };
  }
  return { command: `set -e; ${raw}`, rewritten: true };
}

function normalizeDbRedirectForDiagnostics(command) {
  const raw = String(command || '').trim();
  if (!raw) return { command: raw, rewritten: false };
  if (!/\b(psql|mysql)\b/i.test(raw)) return { command: raw, rewritten: false };
  if (/\s2>|\s2>>|2>&1/.test(raw)) return { command: raw, rewritten: false };

  const rewritten = raw.replace(/>\s*("[^"]+"|'[^']+'|[^\s;]+)(\s|$)/g, (full, filePath, tail) => {
    const normalized = String(filePath || '').replace(/^['"]|['"]$/g, '');
    if (!/\.(txt|log)$/i.test(normalized)) return full;
    return `> ${filePath} 2>&1${tail}`;
  });

  return { command: rewritten, rewritten: rewritten !== raw };
}

function normalizeNonFatalCommandPrefixes(prefixes) {
  return (Array.isArray(prefixes) ? prefixes : [])
    .map((item) => String(item || '').trim().toLowerCase())
    .filter(Boolean);
}

function compileBlockedCommandPatterns(patterns) {
  const compiled = [];
  for (const item of Array.isArray(patterns) ? patterns : []) {
    const source = String(item || '').trim();
    if (!source) continue;
    try {
      compiled.push(new RegExp(source, 'i'));
    } catch {
      // Ignore invalid regex from external policy files.
    }
  }
  return compiled;
}

function extractSqlFromCliCommand(command) {
  const raw = String(command || '').trim();
  if (!raw) return '';
  const psqlMatch = raw.match(/\bpsql\b[\s\S]*?\s-c\s+(["'])([\s\S]*?)\1/i);
  if (psqlMatch) return String(psqlMatch[2] || '').trim();
  const mysqlMatch = raw.match(/\bmysql\b[\s\S]*?\s-e\s+(["'])([\s\S]*?)\1/i);
  if (mysqlMatch) return String(mysqlMatch[2] || '').trim();
  const sqliteMatch = raw.match(/\bsqlite3\b[\s\S]*?\s+(["'])([\s\S]*?)\1\s*$/i);
  if (sqliteMatch && /\b(select|update|delete|insert|replace|alter|create|drop)\b/i.test(sqliteMatch[2] || '')) {
    return String(sqliteMatch[2] || '').trim();
  }
  return '';
}

function parseSimpleWriteSql(sqlText) {
  const sql = String(sqlText || '').trim();
  if (!sql) return null;
  const updateMatch = sql.match(/^update\s+([a-zA-Z0-9_$."`\[\]]+)\s+set\b([\s\S]*)$/i);
  if (updateMatch) {
    return {
      type: 'update',
      table: String(updateMatch[1] || '').replace(/["`\[\]]/g, '').toLowerCase(),
      hasWhere: /\bwhere\b/i.test(updateMatch[2] || ''),
    };
  }

  const deleteMatch = sql.match(/^delete\s+from\s+([a-zA-Z0-9_$."`\[\]]+)([\s\S]*)$/i);
  if (deleteMatch) {
    return {
      type: 'delete',
      table: String(deleteMatch[1] || '').replace(/["`\[\]]/g, '').toLowerCase(),
      hasWhere: /\bwhere\b/i.test(deleteMatch[2] || ''),
    };
  }

  return null;
}

function parseSimpleCountEstimateSql(sqlText) {
  const sql = String(sqlText || '').trim();
  if (!sql) return null;
  const countMatch = sql.match(/^select\s+count\(\*\)\s+from\s+([a-zA-Z0-9_$."`\[\]]+)([\s\S]*)$/i);
  if (!countMatch) return null;
  if (!/\bwhere\b/i.test(countMatch[2] || '')) return null;
  return {
    table: String(countMatch[1] || '').replace(/["`\[\]]/g, '').toLowerCase(),
  };
}

function parseCountFromOutputText(text) {
  const lines = String(text || '').split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
  for (let i = lines.length - 1; i >= 0; i -= 1) {
    const m = lines[i].match(/^\d+$/);
    if (m) return Number.parseInt(m[0], 10);
  }
  return null;
}

function isPathInsideWorkspace(workspaceRoot, targetPath) {
  const resolvedRoot = path.resolve(workspaceRoot || '.');
  const resolvedTarget = path.resolve(targetPath || '.');
  const rel = path.relative(resolvedRoot, resolvedTarget);
  return rel === '' || (!rel.startsWith('..') && !path.isAbsolute(rel));
}

function resolveTaskScopedPath(workspaceRoot, taskPath) {
  const safeRoot = path.resolve(workspaceRoot || '.');
  const candidate = path.isAbsolute(String(taskPath || ''))
    ? path.resolve(String(taskPath || ''))
    : path.resolve(safeRoot, String(taskPath || ''));
  if (!isPathInsideWorkspace(safeRoot, candidate)) return '';
  return candidate;
}

function isNonFatalCommandFailure(command, nonFatalCommandPrefixes) {
  const normalized = String(command || '').trim().toLowerCase();
  const firstToken = tokenizeShellCommand(normalized)[0] || '';
  return nonFatalCommandPrefixes.some((prefix) => {
    return normalized.startsWith(prefix) || firstToken === prefix;
  });
}

function shouldRequireNonEmptyData(taskDescription) {
  return /(宽表|数据流|可视化|看板|dashboard|report|报表|business\s*flow|category|时间变化)/i.test(String(taskDescription || ''));
}

function extractRedirectedCsvPath(command, cwd) {
  const text = String(command || '');
  const redirectMatch = text.match(/>\s*(?:"([^"]+)"|'([^']+)'|([^\s;]+))/);
  if (!redirectMatch) return '';
  const rawPath = redirectMatch[1] || redirectMatch[2] || redirectMatch[3] || '';
  if (!/\.csv$/i.test(rawPath)) return '';
  const resolved = path.resolve(String(cwd || '.'), rawPath);
  return resolved;
}

function detectEmptyDataArtifact(command, cwd, taskDescription) {
  if (!shouldRequireNonEmptyData(taskDescription)) return { empty: false, reason: '' };
  const csvPath = extractRedirectedCsvPath(command, cwd);
  if (!csvPath || !fs.existsSync(csvPath)) return { empty: false, reason: '' };

  let content = '';
  try {
    content = fs.readFileSync(csvPath, 'utf8');
  } catch {
    return { empty: false, reason: '' };
  }

  if (/\(0\s+rows?\)/i.test(content)) {
    return {
      empty: true,
      reason: `数据导出为空: ${path.basename(csvPath)} 显示 (0 rows)`,
    };
  }

  // Fallback for plain CSV content that has only header line and no data rows.
  const lines = content.split(/\r?\n/).filter((line) => line.trim().length > 0);
  if (lines.length <= 1 && /,/u.test(lines[0] || '')) {
    return {
      empty: true,
      reason: `数据导出为空: ${path.basename(csvPath)} 仅包含表头`,
    };
  }

  return { empty: false, reason: '' };
}

function runSessionBuiltin(command, sessionState, taskId, emitTaskConsole) {
  const trimmed = String(command || '').trim();
  const simpleCd = trimmed.match(/^cd(?:\s+(.+))?$/i);
  if (simpleCd) {
    if (emitTaskConsole) emitTaskConsole(taskId, `$ ${trimmed}`);
    const targetToken = simpleCd[1] ? stripShellToken(simpleCd[1].trim()) : (process.env.HOME || sessionState.cwd);
    const nextCwd = path.resolve(sessionState.cwd, targetToken || '.');
    const allowSessionEscapeWorkDir = Boolean(sessionState.allowSessionEscapeWorkDir);
    if (!allowSessionEscapeWorkDir && !isPathInsideWorkspace(sessionState.workspaceRoot || sessionState.cwd, nextCwd)) {
      const message = `命令退出码: 1（目录越界已阻止: ${nextCwd}）`;
      if (emitTaskConsole) emitTaskConsole(taskId, message);
      return {
        handled: true,
        result: { success: false, message, timedOut: false, exitCode: 1 },
      };
    }
    if (!fs.existsSync(nextCwd) || !fs.statSync(nextCwd).isDirectory()) {
      const message = `命令退出码: 1（目录不存在: ${nextCwd}）`;
      if (emitTaskConsole) emitTaskConsole(taskId, message);
      return {
        handled: true,
        result: { success: false, message, timedOut: false, exitCode: 1 },
      };
    }
    sessionState.cwd = nextCwd;
    const message = '命令退出码: 0';
    if (emitTaskConsole) emitTaskConsole(taskId, message);
    return {
      handled: true,
      result: { success: true, message, timedOut: false, exitCode: 0 },
    };
  }

  const simpleExport = trimmed.match(/^export\s+([A-Za-z_][A-Za-z0-9_]*)=(.*)$/);
  if (simpleExport) {
    if (emitTaskConsole) emitTaskConsole(taskId, `$ ${trimmed}`);
    const key = simpleExport[1];
    let value = simpleExport[2].trim();
    value = stripShellToken(value);
    sessionState.env[key] = value;
    const message = '命令退出码: 0';
    if (emitTaskConsole) emitTaskConsole(taskId, message);
    return {
      handled: true,
      result: { success: true, message, timedOut: false, exitCode: 0 },
    };
  }

  return { handled: false };
}

function extractStandaloneShellCommands(text) {
  const commands = [];
  const shellBlockRe = /```(?:sh|bash|zsh|shell)\n([\s\S]*?)```/gi;
  let match;
  while ((match = shellBlockRe.exec(text)) !== null) {
    for (const rawLine of match[1].split(/\r?\n/)) {
      const trimmed = rawLine.trim();
      if (!trimmed || trimmed.startsWith('#')) continue;
      const normalized = normalizeStandaloneShellCommand(trimmed);
      if (!normalized) continue;
      addUniqueString(commands, normalized);
    }
  }
  return commands;
}

function inferScriptFilenameFromRunCommands(runCommands, ext) {
  const normalizedExt = String(ext || '').toLowerCase();
  if (!normalizedExt || !Array.isArray(runCommands) || runCommands.length === 0) return '';

  for (const command of runCommands) {
    const tokens = tokenizeShellCommand(command);
    if (tokens.length < 2) continue;
    const executable = String(tokens[0] || '').toLowerCase();
    const scriptToken = stripShellToken(tokens[1] || '');
    if (!scriptToken || /\//.test(scriptToken) || /^\./.test(scriptToken) || path.isAbsolute(scriptToken)) continue;

    if ((normalizedExt === 'py') && /^python(?:\d+(?:\.\d+)?)?$/.test(executable) && /\.py$/i.test(scriptToken)) {
      return scriptToken;
    }
    if ((normalizedExt === 'js') && executable === 'node' && /\.m?js$/i.test(scriptToken)) {
      return scriptToken;
    }
    if ((normalizedExt === 'ts') && /^tsx?$/.test(executable) && /\.ts$/i.test(scriptToken)) {
      return scriptToken;
    }
    if ((normalizedExt === 'sh') && /^(bash|sh|zsh)$/.test(executable) && /\.sh$/i.test(scriptToken)) {
      return scriptToken;
    }
  }
  return '';
}

function registerWrittenFile(execution, writtenFiles, writtenFileContents, absPath, content, taskId, emitTaskConsole) {
  fs.mkdirSync(path.dirname(absPath), { recursive: true });
  fs.writeFileSync(absPath, content, 'utf8');
  writtenFileContents.set(absPath, content);
  const alreadyWritten = writtenFiles.includes(absPath);
  if (!alreadyWritten) {
    writtenFiles.push(absPath);
    execution.writtenFiles.push(absPath);
  }
  const msg = alreadyWritten ? `♻️ 已更新文件: ${absPath}` : `✅ 已写入文件: ${absPath}`;
  execution.messages.push(msg);
  if (emitTaskConsole) emitTaskConsole(taskId, msg);
}

function deriveHtmlTitle(filePath) {
  const baseName = path.basename(filePath, path.extname(filePath)).trim();
  return baseName || 'Generated Preview';
}

function insertMarkupBeforeFirstInlineScript(content, markup) {
  const inlineScriptMatch = /<script\b(?![^>]*\bsrc=)[^>]*>/i.exec(content);
  if (inlineScriptMatch) {
    return `${content.slice(0, inlineScriptMatch.index)}${markup}\n${content.slice(inlineScriptMatch.index)}`;
  }
  if (/<\/head>/i.test(content)) {
    return content.replace(/<\/head>/i, `${markup}\n</head>`);
  }
  if (/<body\b[^>]*>/i.test(content)) {
    return content.replace(/<body\b[^>]*>/i, (match) => `${match}\n${markup}`);
  }
  return `${markup}\n${content}`;
}

function insertMarkupAfterBodyStart(content, markup) {
  if (/<body\b[^>]*>/i.test(content)) {
    return content.replace(/<body\b[^>]*>/i, (match) => `${match}\n${markup}`);
  }
  return `${markup}\n${content}`;
}

function hasScriptImportBeforeUsage(content, scriptNeedlePattern, usagePattern) {
  const usageRe = usagePattern instanceof RegExp
    ? new RegExp(usagePattern.source, usagePattern.flags.replace(/g/g, ''))
    : new RegExp(String(usagePattern), 'i');
  const usageMatch = usageRe.exec(content);
  const scriptRe = new RegExp(`<script[^>]+src=["'][^"']*${scriptNeedlePattern}[^"']*["'][^>]*><\\/script>`, 'i');
  const scriptMatch = scriptRe.exec(content);
  if (!scriptMatch) return false;
  return !usageMatch || scriptMatch.index < usageMatch.index;
}

function buildTaskFileServeUrl(taskId, filePath) {
  const normalizedPath = String(filePath || '').replace(/\\/g, '/');
  return `http://localhost:${PORT}/api/task-file?taskId=${encodeURIComponent(taskId)}&filePath=${encodeURIComponent(normalizedPath)}`;
}

function normalizeOpenCommandForTask(command, workDir, taskId) {
  if (!isOpenCommand(command)) {
    return { command, rewritten: false, previewUrl: '' };
  }

  const tokens = (command.match(/"[^"]*"|'[^']*'|\S+/g) || []).map(stripShellToken);
  if (tokens.length < 2) {
    return { command, rewritten: false, previewUrl: '' };
  }

  const rawTarget = tokens[tokens.length - 1] || '';
  if (!rawTarget || /^https?:\/\//i.test(rawTarget) || !/\.html?(?:$|[?#])/i.test(rawTarget)) {
    return { command, rewritten: false, previewUrl: '' };
  }

  const absoluteTarget = path.resolve(workDir, rawTarget);
  const relativeTarget = path.relative(workDir, absoluteTarget);
  const isInsideTaskWorkspace = relativeTarget && !relativeTarget.startsWith('..') && !path.isAbsolute(relativeTarget);
  if (!isInsideTaskWorkspace) {
    return { command, rewritten: false, previewUrl: '' };
  }

  const previewUrl = buildTaskFileServeUrl(taskId, relativeTarget);
  return {
    command: buildOpenCommand(previewUrl),
    rewritten: true,
    previewUrl,
  };
}

function ensureHtmlDocumentStructure(content, filePath) {
  let updated = content.trim() ? content : `<!DOCTYPE html>\n<html lang="zh-CN">\n<head>\n<meta charset="UTF-8">\n<title>${deriveHtmlTitle(filePath)}</title>\n</head>\n<body>\n</body>\n</html>\n`;
  const repairs = [];
  const title = deriveHtmlTitle(filePath);
  const hasHtml = /<html[\s>]/i.test(updated);
  const hasHead = /<head[\s>]/i.test(updated);
  const hasBody = /<body[\s>]/i.test(updated);

  if (!hasHtml && !hasHead && !hasBody) {
    updated = `<!DOCTYPE html>\n<html lang="zh-CN">\n<head>\n<meta charset="UTF-8">\n<title>${title}</title>\n</head>\n<body>\n${updated.trim()}\n</body>\n</html>\n`;
    repairs.push('已补全完整 HTML 文档骨架');
    return { content: updated, repairs };
  }

  if (!hasHtml) {
    updated = `<!DOCTYPE html>\n<html lang="zh-CN">\n${updated.trim()}\n</html>\n`;
    repairs.push('已补全 <html> 根节点');
  } else if (!/<!DOCTYPE html>/i.test(updated)) {
    updated = `<!DOCTYPE html>\n${updated.replace(/^\s*/, '')}`;
    repairs.push('已补全 <!DOCTYPE html>');
  }

  if (!/<head[\s>]/i.test(updated)) {
    updated = updated.replace(
      /<html([^>]*)>/i,
      `<html$1>\n<head>\n<meta charset="UTF-8">\n<title>${title}</title>\n</head>`,
    );
    repairs.push('已补全 <head> 与标题');
  } else {
    if (!/<meta[^>]+charset=/i.test(updated)) {
      updated = updated.replace(/<head([^>]*)>/i, `<head$1>\n<meta charset="UTF-8">`);
      repairs.push('已补全字符集声明');
    }
    if (!/<title[\s>]/i.test(updated)) {
      updated = updated.replace(/<head([^>]*)>/i, `<head$1>\n<title>${title}</title>`);
      repairs.push('已补全标题');
    }
  }

  if (!/<body[\s>]/i.test(updated)) {
    if (/<\/head>/i.test(updated)) {
      updated = updated.replace(/<\/head>/i, '</head>\n<body>');
    } else {
      updated = updated.replace(/<html([^>]*)>/i, '<html$1>\n<body>');
    }
    if (/<\/html>/i.test(updated)) {
      updated = updated.replace(/<\/html>/i, '\n</body>\n</html>');
    } else {
      updated += '\n</body>';
    }
    repairs.push('已补全 <body>');
  }

  return { content: updated, repairs };
}

function repairGeneratedHtml(content, filePath) {
  const result = ensureHtmlDocumentStructure(content, filePath);
  let updated = result.content;
  const repairs = [...result.repairs];
  const warnings = [];

  const usesLegacyThree = /\bTHREE\./.test(updated) && !/from\s*['"][^'"]*three[^'"]*['"]/i.test(updated);
  if (usesLegacyThree && !hasScriptImportBeforeUsage(updated, 'three(?:\\.min)?\\.js', /\bTHREE\./i)) {
    updated = insertMarkupBeforeFirstInlineScript(updated, LEGACY_THREE_SCRIPT_TAG);
    repairs.push('已提前引入 three.js');
  }

  if (/https:\/\/threejs\.org\/examples\/js\/controls\/OrbitControls\.js/i.test(updated)) {
    updated = updated.replace(
      /https:\/\/threejs\.org\/examples\/js\/controls\/OrbitControls\.js/gi,
      LEGACY_ORBITCONTROLS_SCRIPT_URL,
    );
    repairs.push('已将 OrbitControls 脚本切换为稳定 CDN 版本');
  }

  const usesLegacyOrbitControls = /\bTHREE\.OrbitControls\b/.test(updated) && !/from\s*['"][^'"]*OrbitControls\.js['"]/i.test(updated);
  if (usesLegacyOrbitControls && !hasScriptImportBeforeUsage(updated, 'OrbitControls\\.js', /\bTHREE\.OrbitControls\b/i)) {
    updated = insertMarkupBeforeFirstInlineScript(updated, LEGACY_ORBITCONTROLS_SCRIPT_TAG);
    repairs.push('已提前引入 OrbitControls 脚本');
  }

  const needsCanvasElement = /document\.querySelector\(\s*['"]canvas['"]\s*\)/i.test(updated) && !/<canvas[\s>]/i.test(updated);
  if (needsCanvasElement) {
    updated = insertMarkupAfterBodyStart(updated, '<canvas></canvas>');
    repairs.push('已补充 <canvas> 元素');
  }

  const hasCanvasTarget = /document\.querySelector\(\s*['"]canvas['"]\s*\)/i.test(updated) || /<canvas[\s>]/i.test(updated);
  const hasRendererMount = /appendChild\(\s*renderer\.domElement\s*\)|append\(\s*renderer\.domElement\s*\)/i.test(updated);
  if (/new\s+THREE\.WebGLRenderer\s*\(/.test(updated) && !hasCanvasTarget && !hasRendererMount) {
    warnings.push('检测到 WebGLRenderer，但未发现 <canvas> 元素或 renderer.domElement 挂载，页面仍可能空白。');
  }

  return { content: updated, repairs, warnings };
}

function applyGeneratedHtmlHealthChecks(execution, writtenFiles, writtenFileContents, taskId, emitTaskConsole, emitTaskTrace) {
  for (const filePath of writtenFiles) {
    if (!/\.html?$/i.test(filePath)) continue;
    try {
      const original = writtenFileContents.get(filePath) || fs.readFileSync(filePath, 'utf8');
      const report = repairGeneratedHtml(original, filePath);
      if (report.content !== original) {
        fs.writeFileSync(filePath, report.content, 'utf8');
        writtenFileContents.set(filePath, report.content);
      }
      if (report.repairs.length > 0) {
        const message = `ℹ️ HTML 健康检查已修复 ${path.basename(filePath)}: ${report.repairs.join('；')}`;
        execution.messages.push(message);
        if (emitTaskConsole) emitTaskConsole(taskId, message);
        if (emitTaskTrace) emitTaskTrace(taskId, `🩺 ${message}`);
      }
      for (const warning of report.warnings) {
        const message = `⚠️ HTML 健康检查提示 ${path.basename(filePath)}: ${warning}`;
        execution.messages.push(message);
        if (emitTaskConsole) emitTaskConsole(taskId, message);
        if (emitTaskTrace) emitTaskTrace(taskId, `🩺 ${message}`);
      }
    } catch (err) {
      const message = `⚠️ HTML 健康检查失败 ${path.basename(filePath)}: ${err?.message || String(err)}`;
      execution.messages.push(message);
      if (emitTaskConsole) emitTaskConsole(taskId, message);
      if (emitTaskTrace) emitTaskTrace(taskId, `🩺 ${message}`);
    }
  }
}

function repairGeneratedPython(content) {
  let updated = String(content || '');
  const repairs = [];

  updated = updated.replace(/(postgresql(?:\+psycopg2)?:\/\/)([^\s"'`]+)/gi, (full, prefix, tail) => {
    const atIndex = String(tail).lastIndexOf('@');
    if (atIndex <= 0) return full;

    const credentialPart = String(tail).slice(0, atIndex);
    const hostPart = String(tail).slice(atIndex + 1);
    const colonIndex = credentialPart.indexOf(':');
    if (colonIndex <= 0) return full;

    const user = credentialPart.slice(0, colonIndex);
    const password = credentialPart.slice(colonIndex + 1);
    if (!password.includes('@')) return full;

    const encodedPassword = encodeURIComponent(password);
    repairs.push('已修复 PostgreSQL URI 中未转义的密码字符');
    return `${prefix}${user}:${encodedPassword}@${hostPart}`;
  });

  const usesOs = /\bos\.(getenv|environ)\b/.test(updated);
  const hasImportOs = /^\s*(import\s+os\b|from\s+os\s+import\b)/m.test(updated);
  if (usesOs && !hasImportOs) {
    updated = `import os\n${updated}`;
    repairs.push('已补充 import os');
  }

  const hasPlaceholderTable = /\byour_table_name\b/.test(updated);
  const expectsBusinessFlowColumns = /\bbusiness_time\b/.test(updated) && /\bcategory\b/.test(updated);
  if (hasPlaceholderTable && expectsBusinessFlowColumns) {
    const businessFlowQuery = `SELECT
  p.payment_date AS business_time,
  c.name AS category,
  c.name AS business_category,
  p.amount AS amount,
  p.amount AS flight_field1,
  1::numeric AS flight_field2
FROM payment p
JOIN rental r ON r.rental_id = p.rental_id
JOIN inventory i ON i.inventory_id = r.inventory_id
JOIN film_category fc ON fc.film_id = i.film_id
JOIN category c ON c.category_id = fc.category_id
WHERE p.payment_date IS NOT NULL
  AND c.name IS NOT NULL`;

    updated = updated.replace(
      /pd\.read_sql_query\(\s*(["'])[^\n]*?your_table_name[\s\S]*?\1\s*,\s*con\s*=\s*engine\s*\)/g,
      `pd.read_sql_query(
        """
${businessFlowQuery}
""",
        con=engine,
    )`,
    );

    // Fallback: if the script still contains placeholder table names, replace in-place.
    updated = updated.replace(/\byour_table_name\b/g, '(SELECT payment_date AS business_time, amount, customer_id FROM payment) AS business_flow_src');

    if (!/\byour_table_name\b/.test(updated)) {
      repairs.push('已将占位表 your_table_name 替换为可执行的业务流查询');
    }
  }

  if (/\bflight_table\b/.test(updated)) {
    const flightFlowQuery = `SELECT
  p.payment_date AS business_time,
  c.name AS business_category,
  p.amount AS flight_field1,
  1::numeric AS flight_field2
FROM payment p
JOIN rental r ON r.rental_id = p.rental_id
JOIN inventory i ON i.inventory_id = r.inventory_id
JOIN film_category fc ON fc.film_id = i.film_id
JOIN category c ON c.category_id = fc.category_id
WHERE p.payment_date IS NOT NULL`;
    updated = updated.replace(/SELECT\s+\*\s+FROM\s+flight_table\s*;?/gi, flightFlowQuery);
    if (!/\bflight_table\b/.test(updated)) {
      repairs.push('已将占位表 flight_table 替换为可执行的业务流查询');
    }
  }

  return { content: updated, repairs };
}

function applyGeneratedPythonHealthChecks(execution, writtenFiles, writtenFileContents, taskId, emitTaskConsole, emitTaskTrace) {
  for (const filePath of writtenFiles) {
    if (!/\.py$/i.test(filePath)) continue;
    try {
      const original = writtenFileContents.get(filePath) || fs.readFileSync(filePath, 'utf8');
      const report = repairGeneratedPython(original);
      if (report.content !== original) {
        fs.writeFileSync(filePath, report.content, 'utf8');
        writtenFileContents.set(filePath, report.content);
      }
      if (report.repairs.length > 0) {
        const message = `ℹ️ Python 健康检查已修复 ${path.basename(filePath)}: ${report.repairs.join('；')}`;
        execution.messages.push(message);
        if (emitTaskConsole) emitTaskConsole(taskId, message);
        if (emitTaskTrace) emitTaskTrace(taskId, `🩺 ${message}`);
      }
    } catch (err) {
      const message = `⚠️ Python 健康检查失败 ${path.basename(filePath)}: ${err?.message || String(err)}`;
      execution.messages.push(message);
      if (emitTaskConsole) emitTaskConsole(taskId, message);
      if (emitTaskTrace) emitTaskTrace(taskId, `🩺 ${message}`);
    }
  }
}

function detectJavaGuiCompletenessIssue(sourceText) {
  const text = String(sourceText || '');
  const hasGuiSignal = /javax\.swing|java\.awt|javafx\./.test(text)
    || /new\s+JFrame\s*\(/.test(text)
    || /extends\s+JFrame\b/.test(text);
  if (!hasGuiSignal) return '';

  const hasVisibleCall = /\.setVisible\(\s*true\s*\)/.test(text)
    || /\.show\(\s*\)/.test(text)
    || /primaryStage\.show\(\s*\)/.test(text);
  if (hasVisibleCall) return '';

  return 'Java GUI 代码缺少窗口可见化调用（例如 setVisible(true) 或 show()），即使编译通过也无法验证界面可见性。';
}

function hasExplicitJavaRunCommand(runCommands, javaFile) {
  const javaFileName = path.basename(javaFile);
  const className = path.basename(javaFile, '.java');
  return runCommands.some((command) => {
    const tokens = (command.match(/"[^"]*"|'[^']*'|\S+/g) || []).map(stripShellToken);
    if (tokens[0] === 'javac') {
      return tokens.slice(1).some((token) => path.basename(token) === javaFileName);
    }
    if (tokens[0] === 'java') {
      return tokens.slice(1).some((token) => path.basename(token, '.class') === className);
    }
    return false;
  });
}

function compileJava(javaFile, taskId, emitTaskConsole, timeout = DEFAULT_JAVA_TIMEOUT) {
  return new Promise((resolve, reject) => {
    if (emitTaskConsole) emitTaskConsole(taskId, `🔨 编译 Java: ${javaFile}`);
    
    const proc = spawn('javac', [javaFile], { cwd: path.dirname(javaFile) });
    let stderr = '';
    
    const timeoutId = setTimeout(() => {
      proc.kill('SIGTERM');
      const msg = `⚠️ Java 编译超时（${timeout / 1000} 秒）`;
      if (emitTaskConsole) emitTaskConsole(taskId, msg);
      reject(new Error(msg));
    }, timeout);
    
    proc.stderr.on('data', (data) => {
      stderr += data.toString();
      if (emitTaskConsole) emitTaskConsole(taskId, data.toString().trimEnd());
    });
    
    proc.stdout.on('data', () => {
      // Keep stream consumed to avoid potential backpressure in verbose builds.
    });
    
    proc.on('close', (code) => {
      clearTimeout(timeoutId);
      const msg = code === 0 ? '✅ Java 编译成功' : `❌ Java 编译失败:\n${stderr}`;
      if (emitTaskConsole) emitTaskConsole(taskId, msg);
      resolve({ success: code === 0, message: msg });
    });
    
    proc.on('error', (err) => {
      clearTimeout(timeoutId);
      const msg = `❌ 无法启动 javac: ${err?.message}（请确认已安装 JDK 并在 PATH 中）`;
      if (emitTaskConsole) emitTaskConsole(taskId, msg);
      resolve({ success: false, message: msg });
    });
  });
}

function runJava(className, classPath, taskId, emitTaskConsole, options = {}) {
  return new Promise((resolve) => {
    const { backgroundOnStart = false, timeout = DEFAULT_JAVA_TIMEOUT } = options;
    if (emitTaskConsole) emitTaskConsole(taskId, `▶️  运行 Java: ${className}`);
    
    const proc = spawn('java', ['-cp', classPath, className], { cwd: classPath });
    let settled = false;
    let startupTimer = null;
    let timeoutId = null;
    
    const settle = (result) => {
      if (settled) return;
      settled = true;
      if (startupTimer) clearTimeout(startupTimer);
      if (timeoutId) clearTimeout(timeoutId);
      resolve(result);
    };
    
    const onData = (data) => {
      if (emitTaskConsole) emitTaskConsole(taskId, data.toString().trimEnd());
    };
    
    proc.stdout.on('data', onData);
    proc.stderr.on('data', onData);
    
    if (backgroundOnStart) {
      startupTimer = setTimeout(() => {
        const msg = `✅ Java 窗口程序已启动 (${className})`;
        if (emitTaskConsole) emitTaskConsole(taskId, msg);
        settle({ success: true, message: msg });
      }, 1500);
    }
    
    timeoutId = setTimeout(() => {
      proc.kill('SIGTERM');
      const msg = `⚠️ Java 程序运行超时（${timeout / 1000} 秒）`;
      if (emitTaskConsole) emitTaskConsole(taskId, msg);
      settle({ success: false, message: msg });
    }, timeout);
    
    proc.on('close', (code) => {
      const msg = code === 0
        ? (backgroundOnStart ? `ℹ️ Java 程序已退出 (${className})` : `✅ Java 程序执行完成 (${className})`)
        : `⚠️  Java 程序退出码: ${code}`;
      if (emitTaskConsole) emitTaskConsole(taskId, msg);
      if (!backgroundOnStart || !settled) settle({ success: code === 0, message: msg });
    });
    
    proc.on('error', (err) => {
      const msg = `❌ 无法启动 java: ${err?.message}（请确认已安装 JDK 并在 PATH 中）`;
      if (emitTaskConsole) emitTaskConsole(taskId, msg);
      settle({ success: false, message: msg });
    });
  });
}

function runShellCommand(cmd, cwd, taskId, emitTaskConsole, timeout = DEFAULT_SHELL_TIMEOUT, sessionEnv = null) {
  return new Promise((resolve) => {
    if (emitTaskConsole) emitTaskConsole(taskId, `$ ${cmd}`);

    const env = sessionEnv ? { ...process.env, ...sessionEnv } : process.env;
    const proc = spawn(cmd, [], { cwd, shell: true, env });
    let stdout = '';
    let stderr = '';
    const onData = (data) => {
      const text = data.toString();
      if (emitTaskConsole) emitTaskConsole(taskId, text.trimEnd());
      stdout += text;
    };
    const onErrorData = (data) => {
      const text = data.toString();
      if (emitTaskConsole) emitTaskConsole(taskId, text.trimEnd());
      stderr += text;
    };
    
    proc.stdout.on('data', onData);
    proc.stderr.on('data', onErrorData);

    const tail = (text) => {
      const normalized = String(text || '').trim();
      if (!normalized) return '';
      const lines = normalized.split(/\r?\n/).filter(Boolean);
      return lines.slice(-6).join(' | ');
    };
    
    const timeoutId = setTimeout(() => {
      proc.kill('SIGTERM');
      const stderrTail = tail(stderr);
      const stdoutTail = tail(stdout);
      const detail = stderrTail
        ? `；stderr: ${stderrTail}`
        : (stdoutTail ? `；stdout: ${stdoutTail}` : '');
      const msg = `⚠️ 命令运行超时（${timeout / 1000} 秒），已强制终止${detail}`;
      if (emitTaskConsole) emitTaskConsole(taskId, msg);
      resolve({ success: false, message: msg, timedOut: true, exitCode: -1, stdout: '', stderr });
    }, timeout);
    
    proc.on('close', (code) => {
      clearTimeout(timeoutId);
      const stderrTail = tail(stderr);
      const stdoutTail = tail(stdout);
      const detail = Number(code) === 0
        ? ''
        : (stderrTail
          ? `；stderr: ${stderrTail}`
          : (stdoutTail ? `；stdout: ${stdoutTail}` : ''));
      const msg = `命令退出码: ${code}${detail}`;
      if (emitTaskConsole) emitTaskConsole(taskId, msg);
      resolve({ success: code === 0, message: msg, timedOut: false, exitCode: Number(code), stdout, stderr });
    });
    
    proc.on('error', (err) => {
      clearTimeout(timeoutId);
      const msg = `❌ 命令执行失败: ${err?.message}`;
      if (emitTaskConsole) emitTaskConsole(taskId, msg);
      resolve({ success: false, message: msg, timedOut: false, exitCode: -1, stdout, stderr });
    });
  });
}

/**
 * Parse AI response text, write files, and run specified commands.
 * @param {string} aiText
 * @param {string} workDir
 * @param {string} taskId
 * @param {string} taskDescription
 * @param {object} callbacks - { emitTaskConsole, emitTaskTrace }
 * @param {object} options - { shellTimeout, javaTimeout, nonFatalCommandPrefixes }
 * @returns {Promise<object>} execution result
 */
async function executeAiResponse(aiText, workDir, taskId, taskDescription = '', callbacks = {}, options = {}) {
  const { emitTaskConsole, emitTaskTrace } = callbacks;
  const {
    shellTimeout = DEFAULT_SHELL_TIMEOUT,
    javaTimeout = DEFAULT_JAVA_TIMEOUT,
    nonFatalCommandPrefixes = TASK_NON_FATAL_COMMAND_PREFIXES,
    blockedCommandPatterns = [],
    dbMaxAffectedRows,
    preflightImpactEstimates = null,
  } = options;
  const normalizedNonFatalCommandPrefixes = normalizeNonFatalCommandPrefixes(nonFatalCommandPrefixes);
  const blockedCommandRegexes = compileBlockedCommandPatterns(blockedCommandPatterns);
  const workspaceRoot = path.resolve(workDir);
  const impactEstimates = new Map();
  if (preflightImpactEstimates && typeof preflightImpactEstimates === 'object') {
    for (const [table, value] of Object.entries(preflightImpactEstimates)) {
      const n = Number(value);
      if (!table || !Number.isFinite(n) || n < 0) continue;
      impactEstimates.set(String(table).toLowerCase(), Math.floor(n));
    }
  }
  
  const execution = {
    messages: [],
    writtenFiles: [],
    runCommands: [],
    commandResults: [],
    fatal: null,
  };
  const writtenFiles = [];
  const writtenFileContents = new Map();
  let match;

  // Pattern 1: FILE:/文件: <path> followed by a fenced code block
  const fileBlockRe = /(?:^|\n)\s*(?:#+\s*)?(?:FILE|文件)\s*:\s*(\S+)\s*\n```[^\n]*\n([\s\S]*?)```/g;
  while ((match = fileBlockRe.exec(aiText)) !== null) {
    const relPath = match[1].trim();
    const content = match[2];
    const absPath = resolveTaskScopedPath(workspaceRoot, relPath);
    if (!absPath) {
      const msg = `⚠️ 已阻止越界写入: ${relPath}`;
      execution.messages.push(msg);
      if (emitTaskConsole) emitTaskConsole(taskId, msg);
      if (emitTaskTrace) emitTaskTrace(taskId, `🛡️ ${msg}`);
      continue;
    }
    registerWrittenFile(execution, writtenFiles, writtenFileContents, absPath, content, taskId, emitTaskConsole);
  }

  const aiTextWithoutFileBlocks = aiText.replace(/(?:^|\n)\s*(?:#+\s*)?(?:FILE|文件)\s*:\s*(\S+)\s*\n```[^\n]*\n[\s\S]*?```/g, '');

  const runCommands = [];
  const runRe = /^RUN:\s*(.+)$/gm;
  while ((match = runRe.exec(aiText)) !== null) addUniqueString(runCommands, match[1].trim());
  for (const shellCommand of extractStandaloneShellCommands(aiTextWithoutFileBlocks)) {
    addUniqueString(runCommands, shellCommand);
  }

  const normalizedRunCommands = [];
  for (const command of runCommands) {
    const serviceNormalized = normalizeServiceProbeCommand(command);
    const interactiveNormalized = normalizeInteractiveCliCommand(serviceNormalized.command);
    const pythonInlineNormalized = normalizeInlinePythonCommand(interactiveNormalized);
    const failFastNormalized = normalizeFailFastCompositeCommand(pythonInlineNormalized.command);
    const diagnosticRedirectNormalized = normalizeDbRedirectForDiagnostics(failFastNormalized.command);
    const rewritten = normalizeOpenCommandForTask(diagnosticRedirectNormalized.command, workDir, taskId);
    const added = addUniqueString(normalizedRunCommands, rewritten.command);
    if (added && rewritten.rewritten) {
      const message = `ℹ️ 已将本地文件打开命令转换为 HTTP 预览（避免 file:// 安全限制）: ${rewritten.previewUrl}`;
      execution.messages.push(message);
      if (emitTaskConsole) emitTaskConsole(taskId, message);
      if (emitTaskTrace) emitTaskTrace(taskId, `🌐 ${message}`);
    }
    if (added && serviceNormalized.rewritten) {
      const message = `ℹ️ 已将非兼容平台命令改写为本机可执行探测命令: ${serviceNormalized.command}`;
      execution.messages.push(message);
      if (emitTaskConsole) emitTaskConsole(taskId, message);
      if (emitTaskTrace) emitTaskTrace(taskId, `🛠️ ${message}`);
    }
    if (added && interactiveNormalized !== command) {
      const message = `ℹ️ 已增强 psql 命令为非交互/失败即停模式: ${interactiveNormalized}`;
      execution.messages.push(message);
      if (emitTaskConsole) emitTaskConsole(taskId, message);
      if (emitTaskTrace) emitTaskTrace(taskId, `🛠️ ${message}`);
    }
    if (added && pythonInlineNormalized.rewritten) {
      const message = 'ℹ️ 已将 Python -c 复合语句改写为可执行形式，避免单行语法错误';
      execution.messages.push(message);
      if (emitTaskConsole) emitTaskConsole(taskId, message);
      if (emitTaskTrace) emitTaskTrace(taskId, `🛠️ ${message}`);
    }
    if (added && failFastNormalized.rewritten) {
      const message = 'ℹ️ 已为复合命令启用 fail-fast（set -e），避免前序失败被后续命令掩盖';
      execution.messages.push(message);
      if (emitTaskConsole) emitTaskConsole(taskId, message);
      if (emitTaskTrace) emitTaskTrace(taskId, `🛠️ ${message}`);
    }
    if (added && diagnosticRedirectNormalized.rewritten) {
      const message = 'ℹ️ 已为数据库日志重定向补充 stderr 捕获（2>&1）';
      execution.messages.push(message);
      if (emitTaskConsole) emitTaskConsole(taskId, message);
      if (emitTaskTrace) emitTaskTrace(taskId, `🛠️ ${message}`);
    }
  }

  // Pattern 2: standalone fenced code block (no FILE: marker)
  if (writtenFiles.length === 0) {
    const fencedRe = /```(\w+)\n([\s\S]*?)```/g;
    const langExtMap = {
      java: 'java', python: 'py', javascript: 'js', typescript: 'ts',
      go: 'go', rust: 'rs', c: 'c', cpp: 'cpp',
      html: 'html', css: 'css', sh: 'sh', bash: 'sh', zsh: 'sh', shell: 'sh',
    };
    const fileCandidates = new Map();
    while ((match = fencedRe.exec(aiText)) !== null) {
      const lang = match[1].toLowerCase();
      const content = match[2];
      if (lang === 'sh' || lang === 'bash' || lang === 'zsh' || lang === 'shell') continue;
      const ext = langExtMap[lang];
      if (!ext) continue;
      // Skip Python pseudocode blocks that only contain file_write() calls
      if ((lang === 'python' || lang === 'py') && content.includes('file_write(')) continue;
      const hintedFilename = inferScriptFilenameFromRunCommands(normalizedRunCommands, ext);
      const filename = hintedFilename || inferStandaloneFilename(lang, ext, content);
      const existing = fileCandidates.get(filename);
      const candidate = { content, score: scoreStandaloneCodeBlock(lang, content), lang, ext };
      if (!existing || candidate.score > existing.score) {
        fileCandidates.set(filename, candidate);
      }
    }

    for (const [filename, candidate] of fileCandidates.entries()) {
      registerWrittenFile(
        execution,
        writtenFiles,
        writtenFileContents,
        path.join(workDir, filename),
        candidate.content,
        taskId,
        emitTaskConsole,
      );
    }
  }

  // Pattern 3: legacy file_write("path", content_or_variable) pseudocode
  const varAssignments = new Map();
  const tripleQuoteRe = /(\w+)\s*=\s*"""([\s\S]*?)"""/g;
  while ((match = tripleQuoteRe.exec(aiText)) !== null) varAssignments.set(match[1], match[2]);
  const tripleQuoteSingleRe = /(\w+)\s*=\s*'''([\s\S]*?)'''/g;
  while ((match = tripleQuoteSingleRe.exec(aiText)) !== null) varAssignments.set(match[1], match[2]);

  const fileWriteRe = /^\s*file_write\(\s*["'`](.*?)["'`]\s*,\s*([\s\S]*?)\)\s*$/gm;
  while ((match = fileWriteRe.exec(aiText)) !== null) {
    const relPath = match[1].trim();
    const expr = match[2].trim();
    let content = null;
    const tripleMatch =
      expr.match(/^"""([\s\S]*?)"""/)
      || expr.match(/^'''([\s\S]*?)'''/);
    if (tripleMatch) {
      content = tripleMatch[1];
    } else if (varAssignments.has(expr)) {
      content = varAssignments.get(expr);
    } else {
      const strMatch = expr.match(/^"([\s\S]*?)"/) || expr.match(/^'([\s\S]*?)'/);
      if (strMatch) content = strMatch[1];
    }
    if (content === null) continue;
    const absPath = resolveTaskScopedPath(workspaceRoot, relPath);
    if (!absPath) {
      const msg = `⚠️ 已阻止越界写入: ${relPath}`;
      execution.messages.push(msg);
      if (emitTaskConsole) emitTaskConsole(taskId, msg);
      if (emitTaskTrace) emitTaskTrace(taskId, `🛡️ ${msg}`);
      continue;
    }
    registerWrittenFile(execution, writtenFiles, writtenFileContents, absPath, content, taskId, emitTaskConsole);
  }
  execution.runCommands = normalizedRunCommands.slice();

  applyGeneratedHtmlHealthChecks(execution, writtenFiles, writtenFileContents, taskId, emitTaskConsole, emitTaskTrace);
  applyGeneratedPythonHealthChecks(execution, writtenFiles, writtenFileContents, taskId, emitTaskConsole, emitTaskTrace);

  // Compile and run any Java files that were written
  for (const filePath of writtenFiles) {
    if (filePath.endsWith('.java')) {
      const javaSource = writtenFileContents.get(filePath) || '';
      const guiIssue = detectJavaGuiCompletenessIssue(javaSource);
      if (guiIssue) {
        execution.messages.push(`❌ ${guiIssue}`);
        execution.fatal = {
          command: `java ${path.basename(filePath, '.java')}`,
          message: guiIssue,
          reason: guiIssue,
        };
        if (emitTaskTrace) emitTaskTrace(taskId, `🛑 ${guiIssue}`);
        break;
      }

      if (hasExplicitJavaRunCommand(runCommands, filePath)) {
        const msg = `ℹ️ 已跳过自动执行 ${path.basename(filePath)}，因为 AI 已提供显式 RUN 命令`;
        execution.messages.push(msg);
        if (emitTaskConsole) emitTaskConsole(taskId, msg);
        continue;
      }
      try {
        const compileResult = await compileJava(filePath, taskId, emitTaskConsole, javaTimeout);
        execution.messages.push(compileResult.message);
        execution.commandResults.push({
          type: 'java-compile',
          command: `javac ${path.basename(filePath)}`,
          success: compileResult.success,
          allowedFailure: false,
          exitCode: compileResult.success ? 0 : 1,
          timedOut: false,
          message: compileResult.message,
        });
        if (!compileResult.success) {
          const reason = `关键命令执行失败: javac ${path.basename(filePath)}（${compileResult.message}）`;
          execution.messages.push(`❌ ${reason}`);
          execution.fatal = {
            command: `javac ${path.basename(filePath)}`,
            message: compileResult.message,
            reason,
          };
          break;
        }

        const runResult = await runJava(
          path.basename(filePath, '.java'),
          workDir,
          taskId,
          emitTaskConsole,
          { backgroundOnStart: isLikelyGuiJavaSource(writtenFileContents.get(filePath) || ''), timeout: javaTimeout },
        );
        execution.messages.push(runResult.message);
        execution.commandResults.push({
          type: 'java-run',
          command: `java ${path.basename(filePath, '.java')}`,
          success: runResult.success,
          allowedFailure: false,
          exitCode: runResult.success ? 0 : 1,
          timedOut: false,
          message: runResult.message,
        });
        if (!runResult.success) {
          const reason = `关键命令执行失败: java ${path.basename(filePath, '.java')}（${runResult.message}）`;
          execution.messages.push(`❌ ${reason}`);
          execution.fatal = {
            command: `java ${path.basename(filePath, '.java')}`,
            message: runResult.message,
            reason,
          };
          break;
        }
      } catch (err) {
        const msg = `❌ Java 处理失败: ${err?.message || String(err)}`;
        execution.messages.push(msg);
        if (emitTaskConsole) emitTaskConsole(taskId, msg);
        execution.fatal = {
          command: `java ${path.basename(filePath, '.java')}`,
          message: msg,
          reason: msg,
        };
        break;
      }
    }
  }

  // Execute explicit RUN: commands from AI response
  let successfulOpenCommand = false;
  const shellSession = {
    cwd: workDir,
    workspaceRoot,
    allowSessionEscapeWorkDir: false,
    env: {},
  };

  for (const command of normalizedRunCommands) {
    if (execution.fatal) break;

    const blockedByPolicy = blockedCommandRegexes.find((rule) => rule.test(String(command || '')));
    if (blockedByPolicy) {
      const reason = `命令触发安全策略拦截: ${command}`;
      execution.messages.push(`❌ ${reason}`);
      execution.fatal = {
        command,
        message: reason,
        reason,
        exitCode: 1,
      };
      if (emitTaskTrace) emitTaskTrace(taskId, `🛑 ${reason}`);
      execution.commandResults.push({
        type: 'policy-block',
        command,
        success: false,
        allowedFailure: false,
        exitCode: 1,
        timedOut: false,
        message: reason,
      });
      break;
    }

    const sqlText = extractSqlFromCliCommand(command);
    const writeSql = parseSimpleWriteSql(sqlText);
    if (writeSql) {
      if (!writeSql.hasWhere) {
        const reason = `SQL 写入缺少 WHERE，已按安全策略拦截: ${writeSql.type.toUpperCase()} ${writeSql.table}`;
        execution.messages.push(`❌ ${reason}`);
        execution.fatal = {
          command,
          message: reason,
          reason,
          exitCode: 1,
        };
        if (emitTaskTrace) emitTaskTrace(taskId, `🛑 ${reason}`);
        execution.commandResults.push({
          type: 'policy-block',
          command,
          success: false,
          allowedFailure: false,
          exitCode: 1,
          timedOut: false,
          message: reason,
        });
        break;
      }

      if (Number.isFinite(dbMaxAffectedRows)) {
        const estimate = impactEstimates.get(writeSql.table);
        if (!Number.isFinite(estimate)) {
          const reason = `SQL 写入缺少影响行数预估，已按安全策略拦截（表: ${writeSql.table}）`;
          execution.messages.push(`❌ ${reason}`);
          execution.fatal = {
            command,
            message: reason,
            reason,
            exitCode: 1,
          };
          if (emitTaskTrace) emitTaskTrace(taskId, `🛑 ${reason}`);
          execution.commandResults.push({
            type: 'policy-block',
            command,
            success: false,
            allowedFailure: false,
            exitCode: 1,
            timedOut: false,
            message: reason,
          });
          break;
        }
        if (estimate > dbMaxAffectedRows) {
          const reason = `SQL 影响行数预估 ${estimate} 超过阈值 ${dbMaxAffectedRows}，已拦截（表: ${writeSql.table}）`;
          execution.messages.push(`❌ ${reason}`);
          execution.fatal = {
            command,
            message: reason,
            reason,
            exitCode: 1,
          };
          if (emitTaskTrace) emitTaskTrace(taskId, `🛑 ${reason}`);
          execution.commandResults.push({
            type: 'policy-block',
            command,
            success: false,
            allowedFailure: false,
            exitCode: 1,
            timedOut: false,
            message: reason,
          });
          break;
        }
      }
    }

    const builtin = runSessionBuiltin(command, shellSession, taskId, emitTaskConsole);
    const runResult = builtin.handled
      ? builtin.result
      : await runShellCommand(command, shellSession.cwd, taskId, emitTaskConsole, shellTimeout, shellSession.env);
    execution.messages.push(runResult.message);

    const allowedFailure = !runResult.success
      && isNonFatalCommandFailure(command, normalizedNonFatalCommandPrefixes);

    execution.commandResults.push({
      type: builtin.handled ? 'builtin' : 'shell',
      command,
      success: runResult.success,
      allowedFailure,
      exitCode: Number.isFinite(runResult.exitCode) ? Number(runResult.exitCode) : (runResult.success ? 0 : -1),
      timedOut: Boolean(runResult.timedOut),
      message: runResult.message,
    });

    if (!runResult.success) {
      if (allowedFailure) {
        const warn = `⚠️ 命令失败已按白名单放行: ${command}`;
        execution.messages.push(warn);
        if (emitTaskTrace) emitTaskTrace(taskId, warn);
      } else {
        const reason = `关键命令执行失败: ${command}（${runResult.message}）`;
        execution.messages.push(`❌ ${reason}`);
        execution.fatal = {
          command,
          message: runResult.message,
          reason,
          exitCode: runResult.exitCode,
        };
        if (emitTaskTrace) emitTaskTrace(taskId, `🛑 ${reason}`);
        break;
      }
    }

    if (runResult.success) {
      const estimateSql = parseSimpleCountEstimateSql(sqlText);
      if (estimateSql) {
        const parsedCount = parseCountFromOutputText(runResult.stdout);
        if (Number.isFinite(parsedCount)) {
          impactEstimates.set(estimateSql.table, parsedCount);
          const note = `ℹ️ 已记录影响行数预估: ${estimateSql.table}=${parsedCount}`;
          execution.messages.push(note);
          if (emitTaskTrace) emitTaskTrace(taskId, `🧮 ${note}`);
        }
      }

      const emptyCheck = detectEmptyDataArtifact(command, shellSession.cwd, taskDescription);
      if (emptyCheck.empty) {
        const reason = `关键数据结果为空: ${emptyCheck.reason}`;
        execution.messages.push(`❌ ${reason}`);
        execution.fatal = {
          command,
          message: emptyCheck.reason,
          reason,
          exitCode: 1,
        }
        if (emitTaskTrace) emitTaskTrace(taskId, `🛑 ${reason}`);
        break;
      }
    }

    if (isOpenCommand(command) && runResult.success) {
      successfulOpenCommand = true;
    }
  }

  // Auto-open if task looks like open request
  if (!execution.fatal && looksLikeOpenRequest(taskDescription) && writtenFiles.length > 0 && !successfulOpenCommand) {
    const path = require('path');
    const autoOpenTarget = selectAutoOpenTarget(taskDescription, writtenFiles);
    if (autoOpenTarget) {
      const relativeTargetCandidate = path.relative(workDir, autoOpenTarget);
      const relativeTarget = relativeTargetCandidate && !relativeTargetCandidate.startsWith('..') && !path.isAbsolute(relativeTargetCandidate)
        ? relativeTargetCandidate
        : path.basename(autoOpenTarget);
      const autoOpenUrl = buildTaskFileServeUrl(taskId, relativeTarget);
      const autoOpenCommand = buildOpenCommand(autoOpenUrl);
      const note = `ℹ️ 未检测到成功的打开命令，已改为通过本地服务预览打开: ${autoOpenUrl}`;
      if (emitTaskTrace) emitTaskTrace(taskId, note);
      execution.messages.push(note);
      addUniqueString(execution.runCommands, autoOpenCommand);
      const runResult = await runShellCommand(autoOpenCommand, shellSession.cwd, taskId, emitTaskConsole, shellTimeout, shellSession.env);
      execution.messages.push(runResult.message);
      execution.commandResults.push({
        type: 'auto-open',
        command: autoOpenCommand,
        success: runResult.success,
        allowedFailure: false,
        exitCode: Number.isFinite(runResult.exitCode) ? Number(runResult.exitCode) : (runResult.success ? 0 : -1),
        timedOut: Boolean(runResult.timedOut),
        message: runResult.message,
      });
      if (!runResult.success && !isNonFatalCommandFailure(autoOpenCommand, normalizedNonFatalCommandPrefixes)) {
        const reason = `关键命令执行失败: ${autoOpenCommand}（${runResult.message}）`;
        execution.messages.push(`❌ ${reason}`);
        execution.fatal = {
          command: autoOpenCommand,
          message: runResult.message,
          reason,
          exitCode: runResult.exitCode,
        };
      }
    }
  }

  if (execution.messages.length === 0) {
    execution.messages.push('任务已完成（无可执行的文件或命令被识别）');
  }
  return execution;
}

module.exports = {
  executeAiResponse,
  compileJava,
  runJava,
  runShellCommand,
  DEFAULT_SHELL_TIMEOUT,
  DEFAULT_JAVA_TIMEOUT,
};
