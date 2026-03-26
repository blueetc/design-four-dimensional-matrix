// @ts-check
'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { isFactCheckTask, validateTaskExecutionResult } = require('./resultVerifier');

function makeTempProject() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'cloudwbot-fact-check-'));
  fs.mkdirSync(path.join(root, 'apps', 'lib'), { recursive: true });
  fs.writeFileSync(path.join(root, 'apps', 'server.js'), '// server', 'utf8');
  fs.writeFileSync(path.join(root, 'apps', 'lib', 'executor.js'), '// executor', 'utf8');
  return root;
}

describe('resultVerifier', () => {
  it('should detect source-analysis tasks', () => {
    assert.strictEqual(isFactCheckTask('深入阅读项目源码并输出问题报告'), true);
    assert.strictEqual(isFactCheckTask('查看今天电脑环境配置'), false);
  });

  it('should not classify database analytics task as source-analysis fact-check task', () => {
    const objective = '读取 PostgreSQL 数据库 lawyer 的所有表结构与字段关系，生成宽表并输出 HTML 可视化报告';
    assert.strictEqual(isFactCheckTask(objective), false);
  });

  it('should fail analysis tasks with only shallow listing commands', () => {
    const projectRoot = makeTempProject();
    const reportPath = path.join(projectRoot, 'report.html');
    fs.writeFileSync(reportPath, '<html><body>只列目录，不含源码文件引用</body></html>', 'utf8');

    const result = validateTaskExecutionResult({
      objectiveText: '阅读源码并写报告',
      runCommands: ['ls .'],
      writtenFiles: [reportPath],
      aiResponse: '这是一个示例输出',
    }, { projectRoot });

    assert.strictEqual(result.checked, true);
    assert.strictEqual(result.ok, false);
    assert.ok(result.reasons.some((reason) => reason.includes('缺少源码内容读取命令')));
    assert.ok(result.reasons.some((reason) => reason.includes('报告未引用可验证的项目源码文件路径')));
  });

  it('should fail report tasks when evidence section is missing', () => {
    const projectRoot = makeTempProject();
    const reportPath = path.join(projectRoot, 'report.md');
    fs.writeFileSync(
      reportPath,
      '# 项目报告\n已查看 apps/server.js 与 apps/lib/executor.js，但这里未提供命令与输出记录。',
      'utf8',
    );

    const result = validateTaskExecutionResult({
      objectiveText: '分析项目源码并输出报告',
      runCommands: ['cat apps/server.js', 'grep -n "executeAiResponse" apps/lib/executor.js'],
      writtenFiles: [reportPath],
      aiResponse: '已分析。',
    }, { projectRoot });

    assert.strictEqual(result.checked, true);
    assert.strictEqual(result.ok, false);
    assert.ok(result.reasons.some((reason) => reason.includes('命令证据') || reason.includes('输出片段证据')));
  });

  it('should pass analysis tasks with read evidence and source references', () => {
    const projectRoot = makeTempProject();
    const reportPath = path.join(projectRoot, 'report.md');
    fs.writeFileSync(
      reportPath,
      [
        '# 报告',
        '已核查 apps/server.js 与 apps/lib/executor.js 的实现。',
        '',
        '## 证据清单',
        '- 命令: cat apps/server.js',
        '- 命令: grep -n "executeAiResponse" apps/lib/executor.js',
        '- 输出片段: exit code 0，命中 executeAiResponse 定义。',
      ].join('\n'),
      'utf8',
    );

    const result = validateTaskExecutionResult({
      objectiveText: '分析项目源码并输出报告',
      runCommands: ['grep -n "executeAiResponse" apps/lib/executor.js', 'cat apps/server.js'],
      writtenFiles: [reportPath],
      aiResponse: '已基于实际文件执行命令并生成结论。',
    }, { projectRoot });

    assert.strictEqual(result.checked, true);
    assert.strictEqual(result.ok, true);
    assert.ok(result.sourceRefs.includes('apps/server.js'));
    assert.ok(result.sourceRefs.includes('apps/lib/executor.js'));
  });

  it('should skip verification for non-analysis tasks', () => {
    const result = validateTaskExecutionResult({
      objectiveText: '运行 Java 程序并打开窗口',
      runCommands: ['javac Main.java', 'java Main'],
      writtenFiles: [],
      aiResponse: '',
    });

    assert.strictEqual(result.checked, false);
    assert.strictEqual(result.ok, true);
  });

  it('should not classify project-folder submission task as source-analysis fact-check task', () => {
    const objective = '按照 VS Code 开发环境提交项目文件夹，并提供工具协议与执行器护栏配置';
    assert.strictEqual(isFactCheckTask(objective), false);
  });
});
