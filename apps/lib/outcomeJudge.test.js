// @ts-check
'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert');
const { evaluateTaskOutcome } = require('./outcomeJudge');

describe('outcomeJudge', () => {
  it('should pass when evidence and execution are sufficient', () => {
    const result = evaluateTaskOutcome({
      objectiveText: '运行并验证脚本输出',
      execution: {
        messages: ['命令退出码: 0'],
        writtenFiles: ['output.txt'],
        runCommands: ['node app.js'],
        commandResults: [{ command: 'node app.js', success: true, allowedFailure: false }],
        fatal: null,
      },
      verification: { checked: false, ok: true, summary: '' },
      aiResponse: '已执行并完成。',
    });

    assert.strictEqual(result.ok, true);
    assert.strictEqual(result.status, 'pass');
    assert.ok(result.confidence >= 0.55);
  });

  it('should mark retryable when execution intent has no command evidence', () => {
    const result = evaluateTaskOutcome({
      objectiveText: '运行项目并验证结果',
      execution: {
        messages: [],
        writtenFiles: [],
        runCommands: [],
        commandResults: [],
        fatal: null,
      },
      verification: { checked: false, ok: true, summary: '' },
      aiResponse: '我完成了。',
      minConfidence: 0.4,
    });

    assert.strictEqual(result.ok, false);
    assert.strictEqual(result.status, 'retryable');
    assert.ok(result.reasons.some((reason) => reason.includes('执行命令')));
  });

  it('should block when credentials are missing', () => {
    const result = evaluateTaskOutcome({
      objectiveText: '列出本地 MySQL 数据库实例',
      execution: {
        messages: ['ERROR 1045 (28000): Access denied for user \'root\'@\'localhost\' (using password: NO)'],
        writtenFiles: [],
        runCommands: ['mysql -e "SHOW DATABASES;"'],
        commandResults: [{ command: 'mysql -e "SHOW DATABASES;"', success: false, allowedFailure: false }],
        fatal: {
          command: 'mysql -e "SHOW DATABASES;"',
          message: 'Access denied',
          reason: '关键命令执行失败: mysql -e "SHOW DATABASES;"（命令退出码: 1）',
        },
      },
      verification: { checked: false, ok: true, summary: '' },
      aiResponse: '命令执行失败。',
    });

    assert.strictEqual(result.ok, false);
    assert.strictEqual(result.status, 'blocked');
    assert.strictEqual(result.blockerType, 'credentials');
    assert.ok(result.blockerHint.includes('账号') || result.blockerHint.includes('Token') || result.blockerHint.includes('权限'));
  });

  it('should mark retryable when confidence is below threshold with actionable reasons', () => {
    const result = evaluateTaskOutcome({
      objectiveText: '分析项目源码并输出报告',
      execution: {
        messages: ['命令退出码: 0'],
        writtenFiles: ['report.md'],
        runCommands: ['ls .'],
        commandResults: [{ command: 'ls .', success: true, allowedFailure: false }],
        fatal: null,
      },
      verification: {
        checked: true,
        ok: false,
        summary: '事实核查未通过：缺少源码内容读取命令。',
      },
      aiResponse: '可能这个项目大致如此。',
      minConfidence: 0.7,
    });

    assert.strictEqual(result.ok, false);
    assert.strictEqual(result.status, 'retryable');
    assert.strictEqual(result.blockerType, '');
  });

  it('should pass when wording is uncertain but command evidence is sufficient', () => {
    const result = evaluateTaskOutcome({
      objectiveText: '运行并验证数据处理脚本',
      execution: {
        messages: ['命令退出码: 0', '命令退出码: 0'],
        writtenFiles: ['flight_wide_table.csv', 'data_flow_view.html'],
        runCommands: ['psql -U blue -d sakila -c "SELECT 1"', 'open "http://localhost:3000/..."'],
        commandResults: [
          { command: 'psql -U blue -d sakila -c "SELECT 1"', success: true, allowedFailure: false },
          { command: 'open "http://localhost:3000/..."', success: true, allowedFailure: false },
        ],
        fatal: null,
      },
      verification: { checked: false, ok: true, summary: '' },
      aiResponse: '可能还需要进一步观察，但当前命令已执行完成。',
      minConfidence: 0.55,
    });

    assert.strictEqual(result.ok, true);
    assert.strictEqual(result.status, 'pass');
  });

  it('should not require open command when previewable artifact exists', () => {
    const result = evaluateTaskOutcome({
      objectiveText: '请预览并检查生成的可视化结果',
      execution: {
        messages: ['命令退出码: 0'],
        writtenFiles: ['data_flow_view.html'],
        runCommands: ['python3 generate_view.py'],
        commandResults: [{ command: 'python3 generate_view.py', success: true, allowedFailure: false }],
        fatal: null,
      },
      verification: { checked: false, ok: true, summary: '' },
      aiResponse: '已生成可视化产物。',
      minConfidence: 0.55,
    });

    assert.strictEqual(result.ok, true);
    assert.strictEqual(result.status, 'pass');
  });

  it('should block when interactive input timeout is detected', () => {
    const result = evaluateTaskOutcome({
      objectiveText: '请自动完成配置收集',
      execution: {
        messages: [
          '⚠️ 命令运行超时（60 秒），已强制终止；stdout: 请确认优先支持哪些数据库（PostgreSQL / MySQL / SQLite / SQL Server）：',
        ],
        writtenFiles: ['ask_user.sh'],
        runCommands: ['bash ask_user.sh'],
        commandResults: [{ command: 'bash ask_user.sh', success: false, allowedFailure: false }],
        fatal: {
          command: 'bash ask_user.sh',
          reason: '关键命令执行失败: bash ask_user.sh（命令运行超时）',
          message: '命令运行超时',
        },
      },
      verification: { checked: false, ok: true, summary: '' },
      aiResponse: '命令已执行。',
    });

    assert.strictEqual(result.ok, false);
    assert.strictEqual(result.status, 'blocked');
    assert.strictEqual(result.blockerType, 'interactive-input');
    assert.ok(result.blockerHint.includes('非交互'));
  });

  it('should treat VS Code CLI as open command evidence', () => {
    const result = evaluateTaskOutcome({
      objectiveText: '请打开并预览生成的配置文件',
      execution: {
        messages: ['命令退出码: 0'],
        writtenFiles: [],
        runCommands: ['code system_prompt.txt'],
        commandResults: [{ command: 'code system_prompt.txt', success: true, allowedFailure: false }],
        fatal: null,
      },
      verification: { checked: false, ok: true, summary: '' },
      aiResponse: '已使用 VS Code 打开文件。',
      minConfidence: 0.55,
    });

    assert.strictEqual(result.ok, true);
    assert.strictEqual(result.status, 'pass');
  });
});
