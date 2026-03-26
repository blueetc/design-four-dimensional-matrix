// @ts-check
'use strict';

/**
 * Utility functions for CloudWBot
 */

/**
 * Escape HTML special characters
 * @param {string} text
 * @returns {string}
 */
function escHtml(text) {
  return text
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

/**
 * Truncate text to maximum length
 * @param {string} text
 * @param {number} maxChars
 * @returns {string}
 */
function truncateText(text, maxChars) {
  if (!text || text.length <= maxChars) return text;
  return `${text.slice(0, maxChars)}\n...[truncated]`;
}

/**
 * Generate a unique task ID
 * @returns {string}
 */
function generateTaskId() {
  return Date.now().toString(16) + Math.floor(Math.random() * 0xffffffff).toString(16).padStart(8, '0');
}

/**
 * Add unique string to array
 * @param {string[]} list
 * @param {string} value
 * @returns {boolean}
 */
function addUniqueString(list, value) {
  if (!value || list.includes(value)) return false;
  list.push(value);
  return true;
}

/**
 * Strip shell token quotes
 * @param {string} token
 * @returns {string}
 */
function stripShellToken(token) {
  return token.replace(/^['"]|['"]$/g, '');
}

/**
 * Human readable task status
 * @param {string} status
 * @returns {string}
 */
function humanTaskRunStatus(status) {
  switch (status) {
    case 'completed': return '已完成';
    case 'failed': return '失败';
    case 'blocked': return '受阻';
    case 'interrupted': return '已中断';
    case 'running': return '运行中';
    case 'pending': return '等待中';
    default: return status;
  }
}

/**
 * Check if task looks like actionable follow-up
 * @param {string} question
 * @returns {boolean}
 */
function looksLikeActionableFollowUp(question) {
  const text = question || '';
  const directAction = /(修复|修改|更新|保存|写回|写入|覆盖|替换|生成|创建|新增|补充|补全|完善|重做|重写|重建|重新执行|再执行|打开|运行|执行|启动|发布|另存|fix|update|modify|rewrite|write|save|create|generate|open|run|execute|launch|patch|edit)/i;
  if (directAction.test(text)) return true;

  const questionOnlyIntent = /(为什么|为何|是什么|怎么回事|能否解释|请解释|请说明|原因是|原理是|what|why|explain|reason)/i;
  if (questionOnlyIntent.test(text)) return false;

  const inspectionIntent = /(读取|阅读|查看|分析|审查|检查|排查|诊断|评估|梳理|总结|调研|研究|理解|深入)/i;
  const inspectionTarget = /(源码|源代码|代码|项目|目录|仓库|repo|repository|文件|模块|测试|日志|配置)/i;
  const executionCue = /(请执行|去执行|继续任务|继续处理|根据结果修复|按结果修改|并修复|并落地|命令|run|execute|apply|patch|改代码)/i;
  return inspectionIntent.test(text) && inspectionTarget.test(text) && executionCue.test(text);
}

/**
 * Check if text looks like open request
 * @param {string} text
 * @returns {boolean}
 */
function looksLikeOpenRequest(text) {
  return /(打开|点开|预览|查看页面|查看文件|show|open|launch|preview|view)/i.test(text || '');
}

/**
 * Check if command is an open command
 * @param {string} command
 * @returns {boolean}
 */
function isOpenCommand(command) {
  return /^(open|xdg-open|start|explorer\.exe|code|code-insiders)\b/i.test((command || '').trim());
}

/**
 * Build open command for platform
 * @param {string} targetPath
 * @returns {string}
 */
function buildOpenCommand(targetPath) {
  const quotedPath = JSON.stringify(targetPath);
  if (process.platform === 'win32') {
    return `start "" ${quotedPath}`;
  }
  if (process.platform === 'darwin') {
    return `open ${quotedPath}`;
  }
  return `xdg-open ${quotedPath}`;
}

/**
 * Select auto-open target from written files
 * @param {string} taskDescription
 * @param {string[]} writtenFiles
 * @returns {string}
 */
function selectAutoOpenTarget(taskDescription, writtenFiles) {
  if (!Array.isArray(writtenFiles) || writtenFiles.length === 0) return '';
  const path = require('path');
  const lowerDescription = (taskDescription || '').toLowerCase();
  const preferredExtensions = /html|网页|页面|browser|web/.test(lowerDescription)
    ? ['.html', '.htm']
    : ['.html', '.htm', '.md', '.txt', '.json'];

  for (const ext of preferredExtensions) {
    const match = writtenFiles.find((filePath) => path.extname(filePath).toLowerCase() === ext);
    if (match) return match;
  }
  return writtenFiles[0] || '';
}

/**
 * Check if Java source is likely GUI
 * @param {string} sourceText
 * @returns {boolean}
 */
function isLikelyGuiJavaSource(sourceText) {
  return /javax\.swing|java\.awt|javafx\./.test(sourceText);
}

/**
 * Score standalone code block for quality
 * @param {string} lang
 * @param {string} content
 * @returns {number}
 */
function scoreStandaloneCodeBlock(lang, content) {
  let score = content.length;
  if (lang === 'html') {
    if (/<!DOCTYPE html>/i.test(content)) score += 8000;
    if (/<html[\s>]/i.test(content)) score += 4000;
    if (/<body[\s>]/i.test(content)) score += 1000;
    if (/^<script[\s>]/i.test(content.trim())) score -= 5000;
  }
  if (lang === 'java' && /public\s+class\s+\w+/.test(content)) score += 3000;
  if ((lang === 'javascript' || lang === 'typescript') && /(function\s+\w+|const\s+\w+|class\s+\w+)/.test(content)) score += 1200;
  return score;
}

/**
 * Infer filename from standalone code block
 * @param {string} lang
 * @param {string} ext
 * @param {string} content
 * @returns {string}
 */
function inferStandaloneFilename(lang, ext, content) {
  if (lang === 'java') {
    const classMatch = content.match(/public\s+class\s+(\w+)/);
    if (classMatch) return `${classMatch[1]}.java`;
  }
  return `output.${ext}`;
}

module.exports = {
  escHtml,
  truncateText,
  generateTaskId,
  addUniqueString,
  stripShellToken,
  humanTaskRunStatus,
  looksLikeActionableFollowUp,
  looksLikeOpenRequest,
  isOpenCommand,
  buildOpenCommand,
  selectAutoOpenTarget,
  isLikelyGuiJavaSource,
  scoreStandaloneCodeBlock,
  inferStandaloneFilename,
};
