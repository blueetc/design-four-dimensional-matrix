// @ts-check
'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert');
const { redactSensitiveText } = require('./taskManager');

describe('taskManager', () => {
  it('should redact URI credentials and password assignments', () => {
    const input = [
      'postgresql://blue:Tdsipass@@1234@localhost/sakila',
      'password=Tdsipass@@1234',
      '密码: Tdsipass@@1234',
    ].join('\n');

    const redacted = redactSensitiveText(input);

    assert.ok(!redacted.includes('Tdsipass@@1234'));
    assert.ok(redacted.includes('postgresql://blue:***@localhost/sakila'));
    assert.ok(redacted.includes('password=***'));
    assert.ok(redacted.includes('密码: ***'));
  });
});
