// @ts-check
'use strict';

const path = require('path');
const { validateTaskExecutionResult } = require('./resultVerifier');
const { evaluateTaskOutcome } = require('./outcomeJudge');

/**
 * @param {object} payload
 * @param {string} payload.objectiveText
 * @param {object} payload.execution
 * @param {string} payload.aiResponse
 * @param {number} [payload.minConfidence]
 * @param {string} [payload.projectRoot]
 * @param {{id: string, pass: boolean, detail: string}[]} [payload.acceptanceChecklist]
 * @returns {{
 *   ok: boolean,
 *   reasons: string[],
 *   verification: {checked: boolean, ok: boolean, summary: string, reasons?: string[], sourceRefs?: string[]},
 *   outcome: {ok: boolean, status: 'pass'|'retryable'|'blocked', confidence: number, reasons: string[], summary: string, blockerType: string, blockerHint: string},
 *   acceptanceChecklist: {id: string, pass: boolean, detail: string}[]
 * }}
 */
function evaluateExpertExecutionGate(payload) {
  const objectiveText = String(payload?.objectiveText || '');
  const execution = payload?.execution || {};
  const aiResponse = String(payload?.aiResponse || '');
  const minConfidence = Number.isFinite(Number(payload?.minConfidence))
    ? Number(payload.minConfidence)
    : 0.55;
  const projectRoot = path.resolve(String(payload?.projectRoot || process.cwd()));
  const acceptanceChecklist = Array.isArray(payload?.acceptanceChecklist)
    ? payload.acceptanceChecklist
    : [];

  const verification = validateTaskExecutionResult({
    objectiveText,
    runCommands: Array.isArray(execution.runCommands) ? execution.runCommands : [],
    writtenFiles: Array.isArray(execution.writtenFiles) ? execution.writtenFiles : [],
    aiResponse,
  }, {
    projectRoot,
  });

  const outcome = evaluateTaskOutcome({
    objectiveText,
    execution,
    verification,
    aiResponse,
    minConfidence,
  });

  const failedAcceptanceItems = acceptanceChecklist.filter((item) => item && !item.pass);
  const reasons = [];

  if (verification.checked && !verification.ok) {
    reasons.push(verification.summary || '事实核查未通过');
  }
  if (!outcome.ok) {
    reasons.push(outcome.summary || '目标判定未通过');
  }
  for (const item of failedAcceptanceItems) {
    reasons.push(`验收项未通过(${item.id}): ${item.detail}`);
  }

  return {
    ok: reasons.length === 0,
    reasons,
    verification,
    outcome,
    acceptanceChecklist,
  };
}

module.exports = {
  evaluateExpertExecutionGate,
};
