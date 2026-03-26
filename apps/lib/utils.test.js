// @ts-check
'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert');
const utils = require('./utils');

describe('utils', () => {
  describe('escHtml', () => {
    it('should escape HTML special characters', () => {
      assert.strictEqual(utils.escHtml('<script>'), '&lt;script&gt;');
      assert.strictEqual(utils.escHtml('a & b'), 'a &amp; b');
      assert.strictEqual(utils.escHtml('"quoted"'), '&quot;quoted&quot;');
    });

    it('should handle empty strings', () => {
      assert.strictEqual(utils.escHtml(''), '');
    });
  });

  describe('truncateText', () => {
    it('should not truncate short text', () => {
      assert.strictEqual(utils.truncateText('hello', 100), 'hello');
    });

    it('should truncate long text', () => {
      const long = 'a'.repeat(200);
      const result = utils.truncateText(long, 100);
      assert.ok(result.includes('[truncated]'));
      assert.ok(result.length < long.length);
    });
  });

  describe('generateTaskId', () => {
    it('should generate unique IDs', () => {
      const id1 = utils.generateTaskId();
      const id2 = utils.generateTaskId();
      assert.notStrictEqual(id1, id2);
      assert.ok(id1.length > 0);
      assert.ok(id2.length > 0);
    });
  });

  describe('addUniqueString', () => {
    it('should add unique strings', () => {
      const list = [];
      assert.strictEqual(utils.addUniqueString(list, 'a'), true);
      assert.strictEqual(utils.addUniqueString(list, 'a'), false);
      assert.deepStrictEqual(list, ['a']);
    });

    it('should not add empty strings', () => {
      const list = [];
      assert.strictEqual(utils.addUniqueString(list, ''), false);
      assert.deepStrictEqual(list, []);
    });
  });

  describe('humanTaskRunStatus', () => {
    it('should return correct status labels', () => {
      assert.strictEqual(utils.humanTaskRunStatus('completed'), '已完成');
      assert.strictEqual(utils.humanTaskRunStatus('failed'), '失败');
      assert.strictEqual(utils.humanTaskRunStatus('blocked'), '受阻');
      assert.strictEqual(utils.humanTaskRunStatus('running'), '运行中');
      assert.strictEqual(utils.humanTaskRunStatus('unknown'), 'unknown');
    });
  });

  describe('looksLikeActionableFollowUp', () => {
    it('should detect actionable keywords', () => {
      assert.strictEqual(utils.looksLikeActionableFollowUp('修复这个bug'), true);
      assert.strictEqual(utils.looksLikeActionableFollowUp('修改文件'), true);
      assert.strictEqual(utils.looksLikeActionableFollowUp('run the code'), true);
      assert.strictEqual(utils.looksLikeActionableFollowUp('遇到报错了，帮我解决这个问题'), false);
      assert.strictEqual(utils.looksLikeActionableFollowUp('这是什么'), false);
    });

    it('should treat source inspection requests as actionable follow-up', () => {
      assert.strictEqual(utils.looksLikeActionableFollowUp('深入阅读该目录的源代码并分析问题'), false);
      assert.strictEqual(utils.looksLikeActionableFollowUp('查看这个项目代码里有哪些能力'), false);
      assert.strictEqual(utils.looksLikeActionableFollowUp('分析一下这个目录并继续修复问题'), true);
      assert.strictEqual(utils.looksLikeActionableFollowUp('请执行源码排查并根据结果修改代码'), true);
    });
  });

  describe('looksLikeOpenRequest', () => {
    it('should detect open keywords', () => {
      assert.strictEqual(utils.looksLikeOpenRequest('打开文件'), true);
      assert.strictEqual(utils.looksLikeOpenRequest('preview the page'), true);
      assert.strictEqual(utils.looksLikeOpenRequest('这是什么'), false);
    });
  });

  describe('isOpenCommand', () => {
    it('should detect open commands', () => {
      assert.strictEqual(utils.isOpenCommand('open file.txt'), true);
      assert.strictEqual(utils.isOpenCommand('xdg-open file.txt'), true);
      assert.strictEqual(utils.isOpenCommand('start file.txt'), true);
      assert.strictEqual(utils.isOpenCommand('code file.txt'), true);
      assert.strictEqual(utils.isOpenCommand('code-insiders file.txt'), true);
      assert.strictEqual(utils.isOpenCommand('cat file.txt'), false);
    });
  });

  describe('selectAutoOpenTarget', () => {
    it('should prefer HTML files for web tasks', () => {
      const files = ['/path/file.js', '/path/index.html', '/path/style.css'];
      assert.strictEqual(utils.selectAutoOpenTarget('创建网页', files), '/path/index.html');
    });

    it('should return first file if no preference', () => {
      const files = ['/path/file.js', '/path/style.css'];
      assert.strictEqual(utils.selectAutoOpenTarget('创建脚本', files), '/path/file.js');
    });

    it('should return empty for empty list', () => {
      assert.strictEqual(utils.selectAutoOpenTarget('test', []), '');
    });
  });

  describe('isLikelyGuiJavaSource', () => {
    it('should detect GUI Java code', () => {
      assert.strictEqual(utils.isLikelyGuiJavaSource('import javax.swing.JFrame;'), true);
      assert.strictEqual(utils.isLikelyGuiJavaSource('import java.awt.Color;'), true);
      assert.strictEqual(utils.isLikelyGuiJavaSource('public class Main {'), false);
    });
  });

  describe('scoreStandaloneCodeBlock', () => {
    it('should score HTML by completeness', () => {
      const html1 = '<!DOCTYPE html><html><body></body></html>';
      const html2 = '<div>hello</div>';
      assert.ok(utils.scoreStandaloneCodeBlock('html', html1) > utils.scoreStandaloneCodeBlock('html', html2));
    });

    it('should score Java by public class', () => {
      const java1 = 'public class Main {}';
      const java2 = 'class Main {}';
      assert.ok(utils.scoreStandaloneCodeBlock('java', java1) > utils.scoreStandaloneCodeBlock('java', java2));
    });
  });

  describe('inferStandaloneFilename', () => {
    it('should infer Java filename from class name', () => {
      assert.strictEqual(utils.inferStandaloneFilename('java', 'java', 'public class HelloWorld {}'), 'HelloWorld.java');
    });

    it('should use default for other languages', () => {
      assert.strictEqual(utils.inferStandaloneFilename('python', 'py', 'print("hello")'), 'output.py');
    });
  });
});
