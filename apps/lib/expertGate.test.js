// @ts-check
'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert');
const { evaluateExpertExecutionGate } = require('./expertGate');

describe('expertGate', () => {
  it('should pass when outcome and acceptance are both satisfied', () => {
    const result = evaluateExpertExecutionGate({
      objectiveText: '请生成报告并运行验证命令',
      execution: {
        runCommands: ['node -v'],
        writtenFiles: ['/tmp/report.md'],
        commandResults: [{ success: true, command: 'node -v', allowedFailure: false, message: '命令退出码: 0' }],
        messages: ['命令退出码: 0'],
        fatal: null,
      },
      aiResponse: '已完成并验证。',
      acceptanceChecklist: [
        { id: 'file-output', pass: true, detail: '检测到输出文件 1 个' },
        { id: 'execution-evidence', pass: true, detail: '检测到执行命令 1 条' },
      ],
    });

    assert.strictEqual(result.ok, true);
    assert.deepStrictEqual(result.reasons, []);
  });

  it('should fail when outcome judge does not pass', () => {
    const result = evaluateExpertExecutionGate({
      objectiveText: '请运行测试并验证结果',
      execution: {
        runCommands: [],
        writtenFiles: [],
        commandResults: [],
        messages: [],
        fatal: null,
      },
      aiResponse: '已完成。',
      acceptanceChecklist: [],
    });

    assert.strictEqual(result.ok, false);
    assert.ok(result.reasons.some((reason) => reason.includes('目标判定未通过')));
  });

  it('should include failed acceptance checklist reasons', () => {
    const result = evaluateExpertExecutionGate({
      objectiveText: '请生成网页并验证可渲染',
      execution: {
        runCommands: ['echo done'],
        writtenFiles: ['/tmp/output.html'],
        commandResults: [{ success: true, command: 'echo done', allowedFailure: false, message: '命令退出码: 0' }],
        messages: ['命令退出码: 0'],
        fatal: null,
      },
      aiResponse: '已完成。',
      acceptanceChecklist: [
        { id: 'html-output', pass: true, detail: '检测到 HTML 产物 1 个' },
        { id: 'html-renderable', pass: false, detail: 'HTML 缺少可渲染结构' },
      ],
    });

    assert.strictEqual(result.ok, false);
    assert.ok(result.reasons.some((reason) => reason.includes('验收项未通过(html-renderable)')));
  });
});
