// @ts-check
'use strict';

const fs = require('fs');
const path = require('path');
const { APP_ROOT } = require('./config');

const ANALYSIS_INTENT_RE = /(分析|评估|审查|诊断|排查|梳理|理解|阅读|查看|调研|研究|报告|总结|能力|问题|review|audit|analy[sz]e|analysis|report)/i;
const ANALYSIS_TARGET_RE = /(源码|源代码|代码|仓库|repo|repository|模块|测试|实现|architecture|arch|codebase)/i;
const DB_TASK_RE = /(postgres|postgresql|mysql|sqlite|数据库|sql|表结构|字段|关系|宽表|csv|html|可视化|业务数据流|lawyer|sakila)/i;
const REPORT_RE = /(报告|report|总结|评估)/i;
const SOURCE_READ_COMMAND_RE = /(^|\s)(cat|grep|rg|sed|awk|head|tail|git\s+show|git\s+diff|git\s+grep|node\s+-e|python\d?\s+-c)\b/i;
const SPECULATION_RE = /(假设|示例|仅为示例|只是示例|可能是一个典型|无法提供更详细|没有具体[\s\S]{0,40}无法|仅根据目录结构)/i;
const SOURCE_FILE_RE = /([A-Za-z0-9_.-]+(?:\/[A-Za-z0-9_.-]+)*\.(?:js|mjs|cjs|ts|tsx|jsx|json|md|java|py|yml|yaml|css|html|sh))/g;
const EVIDENCE_SECTION_RE = /(证据清单|证据来源|事实依据|evidence\s+checklist|evidence\s+log|evidence\s+section)/i;
const COMMAND_EVIDENCE_RE = /(RUN\s*:|\$\s*[A-Za-z0-9_.-]+|命令\s*[:：]|执行命令\s*[:：]|commands?\s*[:：])/i;
const OUTPUT_EVIDENCE_RE = /(输出片段|命令输出|退出码|exit\s*code|stdout|stderr|结果片段|output\s*snippet)/i;

function escapeRegExp(value) {
  return String(value || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function isFactCheckTask(objectiveText) {
  const text = objectiveText || '';
  if (!ANALYSIS_INTENT_RE.test(text) || !ANALYSIS_TARGET_RE.test(text)) {
    return false;
  }

  // Avoid misclassifying database analytics tasks as source-code fact-check tasks.
  const hasExplicitCodeScope = /(源码|源代码|代码|codebase|repository|repo|模块|文件|实现)/i.test(text);
  if (DB_TASK_RE.test(text) && !hasExplicitCodeScope) {
    return false;
  }

  return true;
}

function isReportTask(objectiveText) {
  return REPORT_RE.test(objectiveText || '');
}

function hasSourceReadEvidence(runCommands) {
  return (runCommands || []).some((cmd) => SOURCE_READ_COMMAND_RE.test(String(cmd || '')));
}

function pickReportFiles(writtenFiles) {
  return (writtenFiles || []).filter((filePath) => /\.(html?|md|txt)$/i.test(String(filePath || '')));
}

function toTextForInspection(content, filePath) {
  if (/\.html?$/i.test(filePath)) {
    return content
      .replace(/<script[\s\S]*?<\/script>/gi, ' ')
      .replace(/<style[\s\S]*?<\/style>/gi, ' ')
      .replace(/<[^>]+>/g, ' ');
  }
  return content;
}

function isProjectSourcePath(absPath, projectRoot) {
  if (!absPath || !projectRoot) return false;
  const relative = path.relative(projectRoot, absPath).replace(/\\/g, '/');
  if (!relative || relative.startsWith('..') || path.isAbsolute(relative)) return false;
  if (relative.startsWith('.cloudwbot_workspace/')) return false;
  return fs.existsSync(absPath) && fs.statSync(absPath).isFile();
}

function collectReferencedSourceFiles(text, projectRoot) {
  const refs = new Set();
  let match;
  while ((match = SOURCE_FILE_RE.exec(text)) !== null) {
    const candidate = (match[1] || '').trim();
    if (!candidate || /^https?:\/\//i.test(candidate)) continue;

    const absPath = path.isAbsolute(candidate)
      ? path.resolve(candidate)
      : path.resolve(projectRoot, candidate);

    if (isProjectSourcePath(absPath, projectRoot)) {
      refs.add(path.relative(projectRoot, absPath).replace(/\\/g, '/'));
    }
  }
  return [...refs];
}

function toCommandToken(command) {
  const tokenMatch = String(command || '').trim().match(/^([A-Za-z0-9_.-]+)/);
  return tokenMatch ? tokenMatch[1] : '';
}

function hasExecutedCommandMention(text, runCommands) {
  const commandTokens = [...new Set((runCommands || []).map(toCommandToken).filter(Boolean))];
  return commandTokens.some((token) => new RegExp(`\\b${escapeRegExp(token)}\\b`, 'i').test(text));
}

function inspectEvidenceBlock(reportText, runCommands) {
  const hasEvidenceSection = EVIDENCE_SECTION_RE.test(reportText);
  const hasCommandEvidence = COMMAND_EVIDENCE_RE.test(reportText);
  const hasOutputEvidence = OUTPUT_EVIDENCE_RE.test(reportText);
  const mentionsExecutedCommand = hasExecutedCommandMention(reportText, runCommands);
  return {
    hasEvidenceSection,
    hasCommandEvidence,
    hasOutputEvidence,
    mentionsExecutedCommand,
  };
}

function validateTaskExecutionResult(payload, options = {}) {
  const {
    objectiveText = '',
    runCommands = [],
    writtenFiles = [],
    aiResponse = '',
  } = payload || {};
  const projectRoot = path.resolve(options.projectRoot || APP_ROOT);

  if (!isFactCheckTask(objectiveText)) {
    return {
      checked: false,
      ok: true,
      reasons: [],
      summary: '当前任务未命中源码事实核查策略',
      sourceRefs: [],
    };
  }

  const reasons = [];
  const sourceReadOk = hasSourceReadEvidence(runCommands);
  if (!sourceReadOk) {
    reasons.push('缺少源码内容读取命令（如 cat/grep/sed/awk/head/tail/git show）。仅列目录不足以支撑结论。');
  }

  const reportFiles = pickReportFiles(writtenFiles);
  let sourceRefs = [];
  let hasSpeculation = SPECULATION_RE.test(aiResponse || '');
  const reportTextChunks = [];

  for (const reportFile of reportFiles.slice(0, 3)) {
    try {
      const content = fs.readFileSync(reportFile, 'utf8');
      const text = toTextForInspection(content, reportFile);
      reportTextChunks.push(text);
      if (SPECULATION_RE.test(text)) {
        hasSpeculation = true;
      }
      for (const ref of collectReferencedSourceFiles(text, projectRoot)) {
        sourceRefs.push(ref);
      }
    } catch {
      reasons.push(`无法读取报告文件进行验收: ${reportFile}`);
    }
  }

  sourceRefs = [...new Set(sourceRefs)];
  const reportTask = isReportTask(objectiveText);
  if (reportTask && sourceRefs.length === 0) {
    reasons.push('报告未引用可验证的项目源码文件路径，缺乏事实锚点。');
  }

  if (reportTask) {
    const reportText = reportTextChunks.join('\n');
    const evidence = inspectEvidenceBlock(reportText, runCommands);
    if (!evidence.hasEvidenceSection) {
      reasons.push('报告缺少“证据清单/Evidence”区块。');
    }
    if (!evidence.hasCommandEvidence) {
      reasons.push('报告缺少命令证据（命令列表或 RUN/$ 记录）。');
    }
    if (!evidence.hasOutputEvidence) {
      reasons.push('报告缺少输出片段证据（如退出码、stdout/stderr、输出摘要）。');
    }
    if (!evidence.mentionsExecutedCommand) {
      reasons.push('报告未提及本任务实际执行过的命令，证据与执行链路未对齐。');
    }
  }

  if (hasSpeculation) {
    reasons.push('输出中包含“假设/示例/仅根据目录结构”等推测性表述，不满足事实核查要求。');
  }

  const ok = reasons.length === 0;
  const summary = ok
    ? `事实核查通过：检测到源码读取命令 ${runCommands.length} 条，报告引用源码文件 ${sourceRefs.length} 个。`
    : `事实核查未通过：${reasons.join('；')}`;

  return {
    checked: true,
    ok,
    reasons,
    summary,
    sourceRefs,
  };
}

module.exports = {
  isFactCheckTask,
  validateTaskExecutionResult,
};
