// @ts-check
'use strict';

const http = require('http');
const { OLLAMA_URL, OLLAMA_MODEL } = require('./config');

function parseOllamaUrl(base) {
  const u = new URL(base);
  return {
    hostname: u.hostname,
    port: parseInt(u.port || (u.protocol === 'https:' ? '443' : '80'), 10),
  };
}

function isCloudModel(model) {
  const name = typeof model === 'string' ? model : model?.name || '';
  return Boolean(
    model?.remote_host
      || model?.remote_model
      || name.endsWith(':cloud')
      || name.endsWith('-cloud'),
  );
}

function isEmbeddingModel(model) {
  const name = typeof model === 'string' ? model : model?.name || '';
  const families = Array.isArray(model?.details?.families)
    ? model.details.families.join(' ')
    : model?.details?.family || '';
  return /embed|bert/i.test(name) || /embed|bert/i.test(families);
}

function listModelDetails() {
  return new Promise((resolve) => {
    const req = http.get(`${OLLAMA_URL}/api/tags`, (res) => {
      let data = '';
      res.on('data', (c) => (data += c));
      res.on('end', () => {
        try { resolve(JSON.parse(data).models || []); }
        catch { resolve([]); }
      });
    });
    req.on('error', () => resolve([]));
    req.setTimeout(5000, () => { req.destroy(); resolve([]); });
  });
}

async function getModel() {
  if (OLLAMA_MODEL) return OLLAMA_MODEL;
  const models = await listModelDetails();
  const runnableLocalModel = models.find(
    (model) => !isCloudModel(model) && !isEmbeddingModel(model) && model?.name,
  );
  const fallbackLocalModel = models.find((model) => !isCloudModel(model) && model?.name);
  return runnableLocalModel?.name || fallbackLocalModel?.name || models[0]?.name || 'qwen2.5:7b';
}

function checkOllama() {
  return new Promise((resolve) => {
    const req = http.get(`${OLLAMA_URL}/api/tags`, (res) => {
      resolve(res.statusCode === 200);
      res.resume();
    });
    req.on('error', () => resolve(false));
    req.setTimeout(3000, () => { req.destroy(); resolve(false); });
  });
}

function listModels() {
  return listModelDetails().then((models) => models.map((model) => model.name));
}

/**
 * Stream a chat completion from Ollama.
 * @param {string} prompt
 * @param {(token: string) => void} onToken
 * @param {string} [modelOverride]
 * @param {AbortSignal} [signal]
 * @returns {Promise<string>} full response text
 */
async function streamOllama(prompt, onToken, modelOverride, signal) {
  const model = modelOverride || (await getModel());
  const { hostname, port } = parseOllamaUrl(OLLAMA_URL);
  const body = JSON.stringify({
    model,
    messages: [{ role: 'user', content: prompt }],
    stream: true,
  });

  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        hostname,
        port,
        path: '/api/chat',
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(body),
        },
      },
      (res) => {
        if (res.statusCode !== 200) {
          let errBody = '';
          res.on('data', (c) => (errBody += c));
          res.on('end', () => {
            try { reject(new Error(JSON.parse(errBody).error || `HTTP ${res.statusCode}`)); }
            catch { reject(new Error(`HTTP ${res.statusCode}`)); }
          });
          return;
        }
        let full = '';
        res.on('data', (chunk) => {
          for (const line of chunk.toString().split('\n').filter(Boolean)) {
            try {
              const token = JSON.parse(line)?.message?.content ?? '';
              if (token) { full += token; onToken(token); }
            } catch { /* ignore partial lines */ }
          }
        });
        res.on('end', () => resolve(full));
      },
    );
    req.on('error', reject);
    
    if (signal) {
      signal.addEventListener('abort', () => {
        req.destroy();
        reject(new Error('Request aborted'));
      });
    }
    
    req.write(body);
    req.end();
  });
}

module.exports = {
  getModel,
  checkOllama,
  listModels,
  listModelDetails,
  streamOllama,
};
