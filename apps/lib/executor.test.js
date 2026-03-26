// @ts-check
'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { executeAiResponse } = require('./executor');

function createTempDir(prefix) {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

describe('executor', () => {
  it('should normalize RUN-prefixed commands inside shell fences', async () => {
    const workDir = createTempDir('cloudwbot-exec-run-');
    const aiText = [
      '```shell',
      'RUN: echo one > first.txt',
      'RUN: echo two > second.txt',
      '```',
    ].join('\n');

    const execution = await executeAiResponse(aiText, workDir, 'test-run-prefix');

    assert.ok(execution.runCommands.includes('echo one > first.txt'));
    assert.ok(execution.runCommands.includes('echo two > second.txt'));
    assert.ok(execution.runCommands.every((command) => !/^RUN:/i.test(command)));
    assert.ok(fs.existsSync(path.join(workDir, 'first.txt')));
    assert.ok(fs.existsSync(path.join(workDir, 'second.txt')));
  });

  it('should preserve cwd across sequential shell commands', async () => {
    const workDir = createTempDir('cloudwbot-exec-cd-');
    const nestedDir = path.join(workDir, 'nested');
    fs.mkdirSync(nestedDir, { recursive: true });
    const beforeFile = path.join(workDir, 'before_pwd.txt');
    const afterFile = path.join(nestedDir, 'after_pwd.txt');
    if (fs.existsSync(afterFile)) fs.rmSync(afterFile, { force: true });

    const aiText = [
      'RUN: pwd > before_pwd.txt',
      'RUN: cd nested',
      'RUN: pwd > after_pwd.txt',
    ].join('\n');

    const execution = await executeAiResponse(aiText, workDir, 'test-cd-session');

    assert.strictEqual(execution.fatal, null);
    assert.ok(fs.existsSync(beforeFile));
    assert.ok(fs.existsSync(afterFile));

    const beforePwd = fs.readFileSync(beforeFile, 'utf8').trim();
    const afterPwd = fs.readFileSync(afterFile, 'utf8').trim();
    assert.strictEqual(fs.realpathSync(path.resolve(beforePwd)), fs.realpathSync(path.resolve(workDir)));
    assert.strictEqual(fs.realpathSync(path.resolve(afterPwd)), fs.realpathSync(path.resolve(nestedDir)));
  });

  it('should parse Chinese 文件 marker blocks as writable files', async () => {
    const workDir = createTempDir('cloudwbot-exec-cn-file-marker-');
    const aiText = [
      '文件: report.md',
      '```markdown',
      '# 验证报告',
      '内容有效',
      '```',
    ].join('\n');

    const execution = await executeAiResponse(aiText, workDir, 'test-cn-file-marker');

    assert.strictEqual(execution.fatal, null);
    assert.ok(fs.existsSync(path.join(workDir, 'report.md')));
    assert.ok(execution.writtenFiles.some((filePath) => filePath.endsWith('report.md')));
  });

  it('should mark non-whitelisted command failures as fatal', async () => {
    const workDir = createTempDir('cloudwbot-exec-fatal-');
    const aiText = 'RUN: node -e "process.exit(1)"';

    const execution = await executeAiResponse(aiText, workDir, 'test-fatal-command', '', {}, {
      nonFatalCommandPrefixes: [],
    });

    assert.ok(execution.fatal);
    assert.ok(String(execution.fatal.command || '').includes('node -e'));
    assert.ok(Array.isArray(execution.commandResults));
    assert.ok(execution.commandResults.some((result) => result.command.includes('node -e') && result.success === false));
  });

  it('should include stderr evidence in fatal shell failure message', async () => {
    const workDir = createTempDir('cloudwbot-exec-fatal-stderr-');
    const aiText = 'RUN: node -e "console.error(\'fk_violation_evidence\'); process.exit(1)"';

    const execution = await executeAiResponse(aiText, workDir, 'test-fatal-stderr', '', {}, {
      nonFatalCommandPrefixes: [],
    });

    assert.ok(execution.fatal);
    assert.ok(String(execution.fatal.reason || '').includes('stderr'));
    assert.ok(String(execution.fatal.reason || '').includes('fk_violation_evidence'));
  });

  it('should allow whitelisted command failures to continue', async () => {
    const workDir = createTempDir('cloudwbot-exec-nonfatal-');
    const aiText = [
      'RUN: node -e "process.exit(1)"',
      'RUN: echo ok > survived.txt',
    ].join('\n');

    const execution = await executeAiResponse(aiText, workDir, 'test-nonfatal-command', '', {}, {
      nonFatalCommandPrefixes: ['node -e'],
    });

    assert.strictEqual(execution.fatal, null);
    assert.ok(fs.existsSync(path.join(workDir, 'survived.txt')));
    assert.ok(execution.commandResults.some((result) => result.command.includes('node -e') && result.allowedFailure === true));
  });

  it('should block FILE path traversal outside task workspace', async () => {
    const workDir = createTempDir('cloudwbot-exec-sandbox-file-');
    const outsideFile = path.join(path.dirname(workDir), 'outside.txt');
    if (fs.existsSync(outsideFile)) fs.rmSync(outsideFile, { force: true });

    const aiText = [
      'FILE: ../outside.txt',
      '```txt',
      'should not be written',
      '```',
    ].join('\n');

    const execution = await executeAiResponse(aiText, workDir, 'test-sandbox-file');

    assert.strictEqual(fs.existsSync(outsideFile), false);
    assert.ok(execution.messages.some((msg) => msg.includes('已阻止越界写入')));
  });

  it('should block file_write path traversal outside task workspace', async () => {
    const workDir = createTempDir('cloudwbot-exec-sandbox-write-');
    const outsideFile = path.join(path.dirname(workDir), 'outside-legacy.txt');
    if (fs.existsSync(outsideFile)) fs.rmSync(outsideFile, { force: true });

    const aiText = 'file_write("../outside-legacy.txt", "blocked")';
    const execution = await executeAiResponse(aiText, workDir, 'test-sandbox-filewrite');

    assert.strictEqual(fs.existsSync(outsideFile), false);
    assert.ok(execution.messages.some((msg) => msg.includes('已阻止越界写入')));
  });

  it('should block cd escape outside task workspace', async () => {
    const workDir = createTempDir('cloudwbot-exec-sandbox-cd-');
    const aiText = [
      'RUN: cd ..',
      'RUN: pwd > still_inside.txt',
    ].join('\n');

    const execution = await executeAiResponse(aiText, workDir, 'test-sandbox-cd');

    assert.ok(execution.fatal);
    assert.ok(String(execution.fatal.reason || '').includes('关键命令执行失败'));
    assert.strictEqual(fs.existsSync(path.join(path.dirname(workDir), 'still_inside.txt')), false);
  });

  it('should infer standalone python filename from RUN command', async () => {
    const workDir = createTempDir('cloudwbot-exec-python-hint-');
    const aiText = [
      '```python',
      'with open("ok.txt", "w", encoding="utf-8") as f:',
      '    f.write("done")',
      '```',
      'RUN: python3 generate_wide_table.py',
    ].join('\n');

    const execution = await executeAiResponse(aiText, workDir, 'test-python-hint');

    assert.strictEqual(execution.fatal, null);
    assert.ok(fs.existsSync(path.join(workDir, 'generate_wide_table.py')));
    assert.ok(fs.existsSync(path.join(workDir, 'ok.txt')));
  });

  it('should auto-fix missing import os in generated python file', async () => {
    const workDir = createTempDir('cloudwbot-exec-python-os-');
    const aiText = [
      'FILE: generate_html_from_db.py',
      '```python',
      'conn_password = os.getenv("DB_PASSWORD") or "ok"',
      'with open("python_healthcheck_ok.txt", "w", encoding="utf-8") as f:',
      '    f.write(conn_password)',
      '```',
      'RUN: python3 generate_html_from_db.py',
    ].join('\n');

    const execution = await executeAiResponse(aiText, workDir, 'test-python-missing-import-os');

    assert.strictEqual(execution.fatal, null);
    assert.ok(fs.existsSync(path.join(workDir, 'python_healthcheck_ok.txt')));
    assert.ok(execution.messages.some((msg) => msg.includes('Python 健康检查已修复')));
  });

  it('should auto-fix unescaped @ in PostgreSQL URI password', async () => {
    const workDir = createTempDir('cloudwbot-exec-python-pg-uri-');
    const aiText = [
      'FILE: uri_repair.py',
      '```python',
      'uri = "postgresql://blue:Tdsipass@@1234@localhost/sakila"',
      'with open("uri_value.txt", "w", encoding="utf-8") as f:',
      '    f.write(uri)',
      '```',
      'RUN: python3 uri_repair.py',
    ].join('\n');

    const execution = await executeAiResponse(aiText, workDir, 'test-python-pg-uri-repair');

    assert.strictEqual(execution.fatal, null);
    const uriValue = fs.readFileSync(path.join(workDir, 'uri_value.txt'), 'utf8');
    assert.ok(uriValue.includes('Tdsipass%40%401234'));
    assert.ok(execution.messages.some((msg) => msg.includes('PostgreSQL URI')));
  });

  it('should auto-rewrite placeholder table query for business flow script', async () => {
    const workDir = createTempDir('cloudwbot-exec-python-placeholder-sql-');
    const scriptPath = path.join(workDir, 'generate_wide_table.py');
    const aiText = [
      'FILE: generate_wide_table.py',
      '```python',
      'import pandas as pd',
      'from sqlalchemy import create_engine',
      'engine = create_engine("postgresql://blue:Tdsipass@@1234@localhost/sakila")',
      'df = pd.read_sql_query("SELECT * FROM your_table_name WHERE business_time IS NOT NULL AND category IS NOT NULL", con=engine)',
      '```',
    ].join('\n');

    const execution = await executeAiResponse(aiText, workDir, 'test-python-placeholder-sql');

    assert.strictEqual(execution.fatal, null);
    const rewritten = fs.readFileSync(scriptPath, 'utf8');
    assert.ok(!rewritten.includes('your_table_name'));
    assert.ok(rewritten.includes('FROM payment p'));
    assert.ok(execution.messages.some((msg) => msg.includes('占位表 your_table_name')));
  });

  it('should rewrite psql -W command to non-interactive mode', async () => {
    const workDir = createTempDir('cloudwbot-exec-psql-nowait-');
    const aiText = 'RUN: psql -U blue -W sakila -c "\\dt"';

    const execution = await executeAiResponse(aiText, workDir, 'test-psql-non-interactive', '', {}, {
      nonFatalCommandPrefixes: ['psql'],
    });

    assert.ok(execution.runCommands.some((cmd) => cmd.includes(' -w')));
    assert.ok(execution.runCommands.some((cmd) => /ON_ERROR_STOP=1/.test(cmd)));
    assert.ok(execution.runCommands.every((cmd) => !/\s-W(\s|$)/.test(cmd)));
    assert.ok(execution.messages.some((msg) => msg.includes('非交互/失败即停模式')));
  });

  it('should force ON_ERROR_STOP for psql scripts without -W', async () => {
    const workDir = createTempDir('cloudwbot-exec-psql-error-stop-');
    const aiText = 'RUN: psql -U blue -d sakila -f create_wide_table.sql';

    const execution = await executeAiResponse(aiText, workDir, 'test-psql-on-error-stop', '', {}, {
      nonFatalCommandPrefixes: ['psql'],
    });

    assert.strictEqual(execution.fatal, null);
    assert.ok(execution.runCommands.some((cmd) => /psql\s+-U\s+blue\s+-d\s+sakila\s+-f\s+create_wide_table\.sql\s+-v\s+ON_ERROR_STOP=1/.test(cmd)));
  });

  it('should normalize psql table discovery query for script-friendly output', async () => {
    const workDir = createTempDir('cloudwbot-exec-psql-table-discovery-');
    const aiText = 'RUN: psql -U blue -d lawyer -c "SELECT table_name FROM information_schema.tables WHERE table_schema = \'public\';" > tables.txt';

    const execution = await executeAiResponse(aiText, workDir, 'test-psql-table-discovery', '', {}, {
      nonFatalCommandPrefixes: ['psql'],
    });

    assert.strictEqual(execution.fatal, null);
    const normalized = execution.runCommands.find((cmd) => /information_schema\.tables/.test(cmd)) || '';
    assert.ok(/\s-t(\s|$)/.test(normalized));
    assert.ok(/\s-A(\s|$)/.test(normalized));
    assert.ok(/ON_ERROR_STOP=1/.test(normalized));
  });

  it('should append explicit postgres database for psql commands without db target', async () => {
    const workDir = createTempDir('cloudwbot-exec-psql-default-db-');
    const aiText = 'RUN: psql -c "\\dt"';

    const execution = await executeAiResponse(aiText, workDir, 'test-psql-default-db', '', {}, {
      nonFatalCommandPrefixes: ['psql'],
    });

    const normalized = execution.runCommands.find((cmd) => /^psql\b/.test(cmd)) || '';
    assert.ok(/\s-d\s+postgres(\s|$)/.test(normalized));
    assert.ok(/ON_ERROR_STOP=1/.test(normalized));
  });

  it('should rewrite Linux postgres service probe on macOS', async () => {
    const workDir = createTempDir('cloudwbot-exec-macos-service-probe-');
    const aiText = 'RUN: systemctl status postgresql';

    const execution = await executeAiResponse(aiText, workDir, 'test-macos-service-probe', '', {}, {
      nonFatalCommandPrefixes: ['systemctl', 'pg_isready'],
    });

    const normalized = execution.runCommands.find((cmd) => cmd.includes('systemctl') || cmd.includes('pg_isready')) || '';
    if (process.platform === 'darwin') {
      assert.strictEqual(normalized, 'pg_isready');
    } else {
      assert.strictEqual(normalized, 'systemctl status postgresql');
    }
  });

  it('should enable fail-fast for composite shell commands', async () => {
    const workDir = createTempDir('cloudwbot-exec-failfast-');
    const aiText = 'RUN: node -e "process.exit(1)"; echo ok > should_not_exist.txt';

    const execution = await executeAiResponse(aiText, workDir, 'test-failfast-composite', '', {}, {
      nonFatalCommandPrefixes: [],
    });

    assert.ok(execution.fatal);
    assert.ok(execution.runCommands.some((cmd) => cmd.startsWith('set -e; ')));
    assert.strictEqual(fs.existsSync(path.join(workDir, 'should_not_exist.txt')), false);
  });

  it('should add stderr redirect to db log files', async () => {
    const workDir = createTempDir('cloudwbot-exec-db-log-redirect-');
    const aiText = 'RUN: psql -U blue -d sakila -c "SELECT 1" > insert_flights_log.txt';

    const execution = await executeAiResponse(aiText, workDir, 'test-db-log-stderr-redirect', '', {}, {
      nonFatalCommandPrefixes: ['psql'],
    });

    assert.strictEqual(execution.fatal, null);
    assert.ok(execution.runCommands.some((cmd) => /insert_flights_log\.txt\s+2>&1/.test(cmd)));
  });

  it('should rewrite invalid python -c compound statement into executable form', async () => {
    const workDir = createTempDir('cloudwbot-exec-python-inline-fix-');
    const aiText = 'RUN: python3 -c "import pathlib; p=pathlib.Path(\'inline_ok.txt\'); with p.open(\'w\', encoding=\'utf-8\') as f: f.write(\'ok\')"';

    const execution = await executeAiResponse(aiText, workDir, 'test-python-inline-compound-fix');

    assert.strictEqual(execution.fatal, null);
    assert.ok(fs.existsSync(path.join(workDir, 'inline_ok.txt')));
    assert.ok(execution.runCommands.some((cmd) => cmd.includes('python3 -c')));
    assert.ok(execution.messages.some((msg) => msg.includes('Python -c 复合语句改写')));
  });

  it('should auto-rewrite flight_table placeholder query', async () => {
    const workDir = createTempDir('cloudwbot-exec-flight-table-');
    const scriptPath = path.join(workDir, 'extract_and_format_data.py');
    const aiText = [
      'FILE: extract_and_format_data.py',
      '```python',
      'import pandas as pd',
      'query = "SELECT * FROM flight_table;"',
      'print(query)',
      '```',
    ].join('\n');

    const execution = await executeAiResponse(aiText, workDir, 'test-flight-table-rewrite');

    assert.strictEqual(execution.fatal, null);
    const rewritten = fs.readFileSync(scriptPath, 'utf8');
    assert.ok(!rewritten.includes('flight_table'));
    assert.ok(rewritten.includes('FROM payment p'));
    assert.ok(execution.messages.some((msg) => msg.includes('占位表 flight_table')));
  });

  it('should fail java gui source missing setVisible true', async () => {
    const workDir = createTempDir('cloudwbot-exec-java-gui-visible-');
    const aiText = [
      'FILE: HelloWindow.java',
      '```java',
      'import javax.swing.JFrame;',
      'public class HelloWindow {',
      '  public static void main(String[] args) {',
      '    JFrame frame = new JFrame("Demo");',
      '    frame.setSize(320, 240);',
      '  }',
      '}',
      '```',
    ].join('\n');

    const execution = await executeAiResponse(aiText, workDir, 'test-java-gui-visible');

    assert.ok(execution.fatal);
    assert.ok(String(execution.fatal.reason || '').includes('setVisible(true)'));
  });

  it('should ignore explanatory inline file_write text', async () => {
    const workDir = createTempDir('cloudwbot-exec-filewrite-inline-');
    const aiText = '这是说明文本：例如 file_write("note.txt", "demo") 只是伪代码，不要执行。';

    const execution = await executeAiResponse(aiText, workDir, 'test-filewrite-inline-ignore');

    assert.strictEqual(execution.fatal, null);
    assert.strictEqual(fs.existsSync(path.join(workDir, 'note.txt')), false);
    assert.ok(execution.messages.some((msg) => msg.includes('无可执行')));
  });

  it('should fail data-flow task when redirected csv artifact is empty', async () => {
    const workDir = createTempDir('cloudwbot-exec-empty-csv-fatal-');
    const aiText = "RUN: printf 'id|departure_time|category_name\\n(0 rows)\\n' > flight_wide_table.csv";

    const execution = await executeAiResponse(
      aiText,
      workDir,
      'test-empty-csv-fatal',
      '生成一个宽表并做数据流可视化',
    );

    assert.ok(execution.fatal);
    assert.ok(String(execution.fatal.reason || '').includes('关键数据结果为空'));
  });

  it('should not enforce empty-csv guard for non-data task', async () => {
    const workDir = createTempDir('cloudwbot-exec-empty-csv-non-data-');
    const aiText = "RUN: printf 'col\\n' > tmp.csv";

    const execution = await executeAiResponse(
      aiText,
      workDir,
      'test-empty-csv-non-data',
      '仅创建一个临时文件',
    );

    assert.strictEqual(execution.fatal, null);
    assert.ok(fs.existsSync(path.join(workDir, 'tmp.csv')));
  });

  it('should block command when matching blocked policy pattern', async () => {
    const workDir = createTempDir('cloudwbot-exec-policy-block-');
    const aiText = 'RUN: rm -rf /';

    const execution = await executeAiResponse(aiText, workDir, 'test-policy-block', '', {}, {
      blockedCommandPatterns: ['(^|\\s)rm\\s+-rf\\s+/'],
    });

    assert.ok(execution.fatal);
    assert.ok(String(execution.fatal.reason || '').includes('安全策略拦截'));
    assert.ok(execution.commandResults.some((item) => item.type === 'policy-block'));
  });

  it('should block SQL write without preflight estimate when threshold policy is enabled', async () => {
    const workDir = createTempDir('cloudwbot-exec-sql-threshold-no-estimate-');
    const aiText = 'RUN: psql -d postgres -c "UPDATE users SET status = \'inactive\' WHERE status = \'active\'"';

    const execution = await executeAiResponse(aiText, workDir, 'test-sql-threshold-no-estimate', '', {}, {
      dbMaxAffectedRows: 200,
      nonFatalCommandPrefixes: ['psql'],
    });

    assert.ok(execution.fatal);
    assert.ok(String(execution.fatal.reason || '').includes('缺少影响行数预估'));
    assert.ok(execution.commandResults.some((item) => item.type === 'policy-block'));
  });

  it('should block SQL write when preflight estimate exceeds threshold', async () => {
    const workDir = createTempDir('cloudwbot-exec-sql-threshold-exceed-');
    const aiText = 'RUN: psql -d postgres -c "DELETE FROM users WHERE status = \'inactive\'"';

    const execution = await executeAiResponse(aiText, workDir, 'test-sql-threshold-exceed', '', {}, {
      dbMaxAffectedRows: 100,
      preflightImpactEstimates: { users: 180 },
      nonFatalCommandPrefixes: ['psql'],
    });

    assert.ok(execution.fatal);
    assert.ok(String(execution.fatal.reason || '').includes('超过阈值'));
    assert.ok(execution.commandResults.some((item) => item.type === 'policy-block'));
  });

  it('should allow SQL write when preflight estimate is within threshold', async () => {
    const workDir = createTempDir('cloudwbot-exec-sql-threshold-allow-');
    const aiText = 'RUN: psql -d postgres -c "UPDATE users SET status = \'active\' WHERE id = 1"';

    const execution = await executeAiResponse(aiText, workDir, 'test-sql-threshold-allow', '', {}, {
      dbMaxAffectedRows: 100,
      preflightImpactEstimates: { users: 1 },
      nonFatalCommandPrefixes: ['psql'],
    });

    assert.strictEqual(execution.fatal, null);
    assert.ok(execution.commandResults.some((item) => item.command.includes('UPDATE users SET status')));
  });
});
