// @ts-check
'use strict';

const { isFactCheckTask } = require('./resultVerifier');

const CREATE_OR_EDIT_RE = /(创建|生成|写入|输出|保存|修改|修复|补全|新增|重写|重构|create|generate|write|save|update|edit|fix|patch|rewrite|refactor)/i;
const EXECUTION_RE = /(运行|执行|启动|编译|测试|校验|验证|run|execute|launch|compile|test|verify|preview|open)/i;
const OPEN_RE = /(打开|预览|open|preview|launch)/i;
const UNCERTAIN_RE = /(可能|大概|猜测|假设|也许|暂不确定|无法确认|不确定|maybe|probably|possibly|assume|uncertain)/i;

const CREDENTIAL_BLOCK_RE = /(access denied|authentication failed|permission denied|forbidden|invalid credential|using password: no|password required|密码|凭据|认证失败|权限不足|拒绝访问)/i;
const DEPENDENCY_BLOCK_RE = /(command not found|cannot find module|no module named|not installed|missing dependency|未安装|缺少依赖|找不到命令|命令不存在)/i;
const CONNECTIVITY_BLOCK_RE = /(econnrefused|timed out|network is unreachable|connection reset|连接失败|连接被拒绝|网络不可达)/i;
const INTERACTIVE_INPUT_BLOCK_RE = /(命令运行超时|timed out)[\s\S]{0,180}(请确认|输入|read\s+[A-Za-z_][A-Za-z0-9_]*|interactive|prompt)/i;

/**
 * @param {number} value
 * @returns {number}
 */
function clamp01(value) {
  if (!Number.isFinite(value)) return 0;
  if (value <= 0) return 0;
  if (value >= 1) return 1;
  return value;
}

/**
 * @param {string} objectiveText
 */
function inferObjectiveIntent(objectiveText) {
  const text = String(objectiveText || '');
  const analysisTask = isFactCheckTask(text);
  return {
    analysisTask,
    needsFileOutput: CREATE_OR_EDIT_RE.test(text) && !analysisTask,
    needsExecution: EXECUTION_RE.test(text),
    needsOpenPreview: OPEN_RE.test(text),
  };
}

/**
 * @param {string[]} runCommands
 */
function hasOpenCommand(runCommands) {
  return (runCommands || []).some((command) => /^(open|xdg-open|start|explorer\.exe|code|code-insiders)\b/i.test(String(command || '').trim()));
}

/**
 * @param {string[]} writtenFiles
 */
function hasPreviewableArtifact(writtenFiles) {
  return (writtenFiles || []).some((filePath) => /\.(html?|md|txt|csv|json)$/i.test(String(filePath || '')));
}

/**
 * @param {string[]} reasons
 */
function formatReasons(reasons) {
  if (!Array.isArray(reasons) || reasons.length === 0) return '';
  return reasons.map((reason, index) => `${index + 1}. ${reason}`).join('；');
}

/**
 * @param {string} text
 * @returns {{ type: string, reason: string, hint: string } | null}
 */
function detectBlockingCondition(text) {
  const haystack = String(text || '');
  if (CREDENTIAL_BLOCK_RE.test(haystack)) {
    return {
      type: 'credentials',
      reason: '执行链路命中权限或凭据阻塞，当前无法继续自动完成。',
      hint: '请补充可用账号/密码/Token 或放宽本地访问权限后重试。',
    };
  }
  if (DEPENDENCY_BLOCK_RE.test(haystack)) {
    return {
      type: 'dependency',
      reason: '执行链路缺少必要命令或依赖，当前环境无法继续自动完成。',
      hint: '请先安装缺失依赖或调整命令后再重试。',
    };
  }
  if (CONNECTIVITY_BLOCK_RE.test(haystack)) {
    return {
      type: 'connectivity',
      reason: '执行链路命中连接阻塞，当前无法可靠访问目标资源。',
      hint: '请检查服务是否启动、端口是否可达，再继续执行。',
    };
  }
  if (INTERACTIVE_INPUT_BLOCK_RE.test(haystack)) {
    return {
      type: 'interactive-input',
      reason: '执行链路命中交互式输入阻塞（脚本等待人工输入后超时）。',
      hint: '请改为非交互执行：为 read 提供默认值，或使用环境变量/管道输入（例如 echo 或 here-doc）后重试。',
    };
  }
  return null;
}

/**
 * @param {object} payload
 * @param {string} payload.objectiveText
 * @param {object} payload.execution
 * @param {object} payload.verification
 * @param {string} payload.aiResponse
 * @param {number} [payload.minConfidence]
 * @returns {{ ok: boolean, status: 'pass'|'retryable'|'blocked', confidence: number, reasons: string[], summary: string, blockerType: string, blockerHint: string }}
 */
function evaluateTaskOutcome(payload) {
  const objectiveText = String(payload?.objectiveText || '');
  const execution = payload?.execution || {};
  const verification = payload?.verification || { checked: false, ok: true, summary: '' };
  const aiResponse = String(payload?.aiResponse || '');
  const minConfidence = Number.isFinite(Number(payload?.minConfidence))
    ? Number(payload.minConfidence)
    : 0.55;

  const intent = inferObjectiveIntent(objectiveText);
  const runCommands = Array.isArray(execution.runCommands) ? execution.runCommands : [];
  const writtenFiles = Array.isArray(execution.writtenFiles) ? execution.writtenFiles : [];
  const commandResults = Array.isArray(execution.commandResults) ? execution.commandResults : [];
  const messages = Array.isArray(execution.messages) ? execution.messages : [];

  const reasons = [];
  let confidence = 1;
  const hasOpenEvidence = hasOpenCommand(runCommands) || hasPreviewableArtifact(writtenFiles);

  if (execution.fatal) {
    reasons.push(execution.fatal.reason || execution.fatal.message || '存在关键命令执行失败。');
    confidence -= 0.45;
  }

  if (intent.needsFileOutput && writtenFiles.length === 0 && !(intent.needsOpenPreview && hasOpenEvidence)) {
    reasons.push('目标包含创建/修改诉求，但未检测到写入文件。');
    confidence -= 0.35;
  }

  if (intent.needsExecution && runCommands.length === 0) {
    reasons.push('目标包含运行/验证诉求，但未检测到执行命令。');
    confidence -= 0.3;
  }

  if (intent.needsOpenPreview && !hasOpenEvidence) {
    reasons.push('目标包含打开/预览诉求，但未检测到打开命令。');
    confidence -= 0.15;
  }

  if (verification.checked && !verification.ok) {
    reasons.push(verification.summary || '事实核查未通过。');
    confidence -= 0.4;
  }

  const allowedFailures = commandResults.filter((item) => !item.success && item.allowedFailure).length;
  if (allowedFailures > 0) {
    reasons.push(`存在 ${allowedFailures} 条白名单放行失败命令，结论可靠性下降。`);
    confidence -= Math.min(0.25, allowedFailures * 0.1);
  }

  const successfulCommandCount = commandResults.filter((item) => item && item.success).length;
  const hasEvidenceAnchors = successfulCommandCount > 0
    || (runCommands.length > 0 && messages.some((line) => /命令退出码:\s*0/.test(String(line || ''))));

  if (UNCERTAIN_RE.test(aiResponse)) {
    if (!hasEvidenceAnchors) {
      reasons.push('输出包含不确定性表述，缺少充分事实锚点。');
      confidence -= 0.2;
    } else {
      // Keep a mild confidence penalty when evidence exists to avoid over-blocking on wording.
      confidence -= 0.05;
    }
  }

  confidence = clamp01(confidence);

  const blockerSource = [
    execution.fatal?.reason || '',
    execution.fatal?.message || '',
    messages.join('\n'),
    verification.summary || '',
  ].join('\n');
  const blocker = detectBlockingCondition(blockerSource);
  if (blocker) {
    return {
      ok: false,
      status: 'blocked',
      confidence,
      reasons: reasons.length > 0 ? reasons : [blocker.reason],
      summary: `${blocker.reason}（置信度 ${Math.round(confidence * 100)}%）`,
      blockerType: blocker.type,
      blockerHint: blocker.hint,
    };
  }

  if (confidence < minConfidence) {
    const lowConfidenceReason = `目标判定置信度 ${Math.round(confidence * 100)}% 低于阈值 ${Math.round(minConfidence * 100)}%。`;
    const finalReasons = reasons.length > 0 ? reasons : [lowConfidenceReason];
    if (reasons.length > 0) {
      return {
        ok: false,
        status: 'retryable',
        confidence,
        reasons: finalReasons,
        summary: `目标判定未通过，建议自动重规划重试（置信度 ${Math.round(confidence * 100)}%）：${formatReasons(finalReasons)}`,
        blockerType: '',
        blockerHint: '',
      };
    }
    return {
      ok: false,
      status: 'blocked',
      confidence,
      reasons: finalReasons,
      summary: `任务进入阻塞状态：${lowConfidenceReason}`,
      blockerType: 'low-confidence',
      blockerHint: '请补充更明确的目标或执行约束后再重试。',
    };
  }

  if (reasons.length > 0) {
    return {
      ok: false,
      status: 'retryable',
      confidence,
      reasons,
      summary: `目标判定未通过，建议自动重规划重试（置信度 ${Math.round(confidence * 100)}%）：${formatReasons(reasons)}`,
      blockerType: '',
      blockerHint: '',
    };
  }

  return {
    ok: true,
    status: 'pass',
    confidence,
    reasons: [],
    summary: `目标判定通过（置信度 ${Math.round(confidence * 100)}%）。`,
    blockerType: '',
    blockerHint: '',
  };
}

module.exports = {
  evaluateTaskOutcome,
};
