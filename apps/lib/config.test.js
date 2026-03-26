// @ts-check
'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert');

describe('config', () => {
  it('should load without errors', () => {
    const config = require('./config');
    assert.ok(typeof config.PORT === 'number');
    assert.ok(typeof config.OLLAMA_URL === 'string');
    assert.ok(typeof config.TASK_MAX_COUNT === 'number');
    assert.ok(typeof config.TASK_MAX_AGE_DAYS === 'number');
    assert.ok(Array.isArray(config.TASK_NON_FATAL_COMMAND_PREFIXES));
  });

  it('should have MIME types', () => {
    const { MIME } = require('./config');
    assert.ok(MIME['.html']);
    assert.ok(MIME['.js']);
    assert.ok(MIME['.css']);
    assert.ok(MIME['.json']);
  });
});
