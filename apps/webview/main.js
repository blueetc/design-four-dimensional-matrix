// @ts-check
'use strict';

// ---------------------------------------------------------------------------
// Transport shim: replaces acquireVsCodeApi() with fetch + Server-Sent Events.
// Features: auto-reconnect with exponential backoff
// ---------------------------------------------------------------------------

const vscode = (() => {
  let es = null;
  let reconnectTimer = null;
  let reconnectAttempts = 0;
  const MAX_RECONNECT_ATTEMPTS = 10;
  const BASE_RECONNECT_DELAY = 1000;
  
  function connect() {
    if (es) {
      try {
        es.close();
      } catch { /* ignore */ }
    }
    
    es = new EventSource('/api/events');
    
    es.onopen = () => {
      console.log('[SSE] Connected');
      reconnectAttempts = 0;
      if (reconnectTimer) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
      }
      // Notify user if this was a reconnect
      if (reconnectAttempts > 0) {
        appendText('system', '✅ 已重新连接到服务器');
      }
    };
    
    es.onmessage = (event) => {
      try {
        window.dispatchEvent(new MessageEvent('message', { data: JSON.parse(event.data) }));
      } catch { /* ignore malformed frames */ }
    };
    
    es.onerror = (err) => {
      console.error('[SSE] Connection error:', err);
      es.close();
      
      if (reconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
        const delay = Math.min(BASE_RECONNECT_DELAY * Math.pow(2, reconnectAttempts), 30000);
        reconnectAttempts++;
        console.log(`[SSE] Reconnecting in ${delay}ms (attempt ${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS})`);
        reconnectTimer = setTimeout(connect, delay);
        
        if (reconnectAttempts === 1) {
          appendText('system', '⚠️ 与服务器的连接断开，正在尝试重连...');
        }
      } else {
        appendText('system', '❌ 无法重新连接到服务器，请刷新页面重试。');
      }
    };
  }
  
  // Initial connection
  connect();
  
  return {
    postMessage(msg) {
      fetch('/api/message', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(msg),
      }).catch(console.error);
    },
    reconnect: connect,
  };
})();

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

/** @type {Map<string, {id: string, description: string, status: string, createdAt: string, rerunOfTaskId?: string, rerunOfDescription?: string, followUpOfTaskId?: string, followUpOfDescription?: string, result?: string, error?: string, summary?: string, workspaceDir?: string, aiOutput?: string, writtenFiles: string[], previewFilePath?: string, previewDisplayPath?: string, previewContent?: string, previewError?: string, previewLoading?: boolean, previewTruncated?: boolean, trace: string[], consoleLines: string[], planProposal?: string, planStatus?: string, planDueAt?: number, planModel?: string, planError?: string, expertGate?: { reasons: string[], categories: string[], categoryTotals: Record<string, number>, lastCategory: string, round: number }}>} */
const tasks = new Map();

let selectedTaskId = null;
let chatStreaming = false;
let pendingFollowUpTaskId = null;
let currentTheme = 'dark'; // 'dark' | 'light'

// ---------------------------------------------------------------------------
// DOM helpers
// ---------------------------------------------------------------------------

/** @param {string} id @returns {HTMLElement} */
const el = (id) => document.getElementById(id);

/** @param {string} text @returns {string} */
function escHtml(text) {
  return text
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

/** @param {string} iso @returns {string} */
function fmtDate(iso) {
  return new Date(iso).toLocaleString('zh-CN');
}

/** @param {string} status @returns {string} */
function humanTaskStatus(status) {
  switch (status) {
    case 'pending': return '等待中';
    case 'running': return '运行中';
    case 'completed': return '已完成';
    case 'failed': return '失败';
    case 'blocked': return '受阻';
    case 'interrupted': return '已中断';
    default: return status;
  }
}

/** @param {{status: string}|undefined} task @returns {boolean} */
function isTaskFinished(task) {
  return Boolean(task) && (
    task.status === 'completed'
    || task.status === 'failed'
    || task.status === 'blocked'
    || task.status === 'interrupted'
  );
}

/** @param {{status: string, planStatus?: string}|undefined} task @returns {boolean} */
function canRerunTask(task) {
  return Boolean(task) && isTaskFinished(task) && !isTaskAwaitingPlanConfirmation(task);
}

/** @param {any} task @returns {boolean} */
function isTaskAwaitingPlanConfirmation(task) {
  return Boolean(task) && task.planStatus === 'awaiting-confirmation';
}

/** @param {any} task @returns {number} */
function getPlanCountdownSeconds(task) {
  if (!task?.planDueAt) return 0;
  return Math.max(0, Math.ceil((task.planDueAt - Date.now()) / 1000));
}

function requestTaskPlanApproval(taskId) {
  const task = tasks.get(taskId);
  if (!isTaskAwaitingPlanConfirmation(task)) return;
  task.planStatus = 'approving';
  updateTaskDetail(taskId);
  updateApprovalList();
  vscode.postMessage({ command: 'confirmTaskPlan', taskId });
}

function requestTaskPlanCancellation(taskId) {
  const task = tasks.get(taskId);
  if (!isTaskAwaitingPlanConfirmation(task)) return;
  task.planStatus = 'cancelling';
  updateTaskDetail(taskId);
  updateApprovalList();
  vscode.postMessage({ command: 'cancelTaskPlan', taskId });
}

function updateApprovalList() {
  const approvalList = el('approval-list');
  const awaiting = [...tasks.values()]
    .filter(isTaskAwaitingPlanConfirmation)
    .sort((left, right) => {
      const leftDueAt = left.planDueAt || 0;
      const rightDueAt = right.planDueAt || 0;
      return leftDueAt - rightDueAt;
    });

  if (awaiting.length === 0) {
    approvalList.innerHTML = '<div class="empty-hint">暂无待审批动作</div>';
    return;
  }

  approvalList.innerHTML = awaiting
    .map((task) => {
      const remain = getPlanCountdownSeconds(task);
      const preview = (task.planProposal || '').replace(/\s+/g, ' ').trim();
      const previewText = preview ? `${preview.slice(0, 120)}${preview.length > 120 ? '...' : ''}` : '方案生成中...';
      return `<div class="approval-item"><div class="approval-title">${escHtml(task.description)}</div><div class="approval-meta">倒计时 ${remain}s · ${escHtml(task.planModel || '模型未标记')}</div><div class="approval-preview">${escHtml(previewText)}</div><div class="approval-actions"><button type="button" class="small-btn" data-approve-task="${escHtml(task.id)}">确认执行</button><button type="button" class="small-btn" data-cancel-task="${escHtml(task.id)}">取消任务</button></div></div>`;
    })
    .join('');
}

function clearTaskFollowUp() {
  pendingFollowUpTaskId = null;
  updateTaskActionState();
}

function renderRerunSource(task) {
  if (!task?.rerunOfTaskId) return '';
  if (task.rerunOfDescription) {
    return `任务"${task.rerunOfDescription}" (${task.rerunOfTaskId})`;
  }
  return `任务 ${task.rerunOfTaskId}`;
}

function renderFollowUpSource(task) {
  if (!task?.followUpOfTaskId) return '';
  if (task.followUpOfDescription) {
    return `任务"${task.followUpOfDescription}" (${task.followUpOfTaskId})`;
  }
  return `任务 ${task.followUpOfTaskId}`;
}

function updateTaskActionState() {
  const followUpButton = el('btn-follow-up-task');
  const rerunButton = el('btn-rerun-task');
  const banner = el('chat-context-banner');
  const bannerText = el('chat-context-text');
  const input = /** @type {HTMLTextAreaElement} */ (el('chat-input'));
  const selectedTask = tasks.get(selectedTaskId);
  const contextTask = pendingFollowUpTaskId ? tasks.get(pendingFollowUpTaskId) : null;

  if (followUpButton) {
    followUpButton.disabled = !selectedTask || !isTaskFinished(selectedTask) || chatStreaming;
  }

  if (rerunButton) {
    rerunButton.disabled = !canRerunTask(selectedTask) || chatStreaming;
  }

  if (contextTask) {
    banner.classList.remove('hidden');
    bannerText.textContent = `下一条消息将基于任务"${contextTask.description}"继续追问`;
    input.placeholder = '输入你想继续追问的问题...';
  } else {
    banner.classList.add('hidden');
    bannerText.textContent = '';
    input.placeholder = '输入消息或命令...';
  }
}

function armTaskFollowUp() {
  const task = tasks.get(selectedTaskId);
  if (!task) {
    appendText('system', '请先在右侧选中一个任务，再进行追问。');
    return;
  }
  if (!isTaskFinished(task)) {
    appendText('system', '该任务尚未完成，暂时不能追问。');
    return;
  }
  pendingFollowUpTaskId = task.id;
  updateTaskActionState();
  el('chat-input').focus();
}

function rerunSelectedTask() {
  const task = tasks.get(selectedTaskId);
  if (!task) {
    appendText('system', '请先在右侧选中一个任务，再重新执行。');
    return;
  }
  if (!canRerunTask(task)) {
    appendText('system', '仅已结束任务支持重新执行（已完成/失败/受阻/已中断）。');
    return;
  }

  appendMessage('system', `♻️ 正在重新执行任务"${escHtml(task.description)}"，并保留来源任务标记`);
  createTask(task.description, {
    rerunOfTaskId: task.id,
    rerunOfDescription: task.description,
  });
}

/** @param {string} title @param {string} bodyHtml @returns {string} */
function renderTaskSection(title, bodyHtml) {
  return `<div class="task-detail-section"><div class="task-detail-title">${escHtml(title)}</div>${bodyHtml}</div>`;
}

/** @param {string} text @param {string} [className] @returns {string} */
function renderTaskPre(text, className = 'task-detail-pre') {
  return `<pre class="${className}">${escHtml(text)}</pre>`;
}

/** @param {string} text @param {string} lang @returns {string} */
function renderHighlightedCode(text, lang = '') {
  // Simple syntax highlighting for common languages
  let highlighted = escHtml(text);
  
  if (lang === 'javascript' || lang === 'typescript' || lang === 'java' || lang === 'python') {
    // Keywords
    highlighted = highlighted.replace(/\b(const|let|var|function|class|import|export|from|return|if|else|for|while|switch|case|break|continue|try|catch|finally|async|await|new|this|typeof|instanceof|void|delete|in|of)\b/g, '<span class="token-keyword">$1</span>');
    // Strings
    highlighted = highlighted.replace(/("[^"]*"|'[^']*'|`[^`]*`)/g, '<span class="token-string">$1</span>');
    // Comments
    highlighted = highlighted.replace(/(\/\/.*$|\/\*[\s\S]*?\*\/|#.*$)/gm, '<span class="token-comment">$1</span>');
    // Numbers
    highlighted = highlighted.replace(/\b(\d+\.?\d*)\b/g, '<span class="token-number">$1</span>');
    // Functions
    highlighted = highlighted.replace(/\b([a-zA-Z_$][a-zA-Z0-9_$]*)\s*(?=\()/g, '<span class="token-function">$1</span>');
  }
  
  return highlighted;
}

function createTaskRecord(id, overrides = {}) {
  const initialExpertGate = overrides.expertGate && typeof overrides.expertGate === 'object'
    ? overrides.expertGate
    : {};
  return {
    id,
    description: overrides.description || '未命名任务',
    status: overrides.status || 'pending',
    createdAt: overrides.createdAt || new Date().toISOString(),
    rerunOfTaskId: overrides.rerunOfTaskId || '',
    rerunOfDescription: overrides.rerunOfDescription || '',
    followUpOfTaskId: overrides.followUpOfTaskId || '',
    followUpOfDescription: overrides.followUpOfDescription || '',
    result: overrides.result || '',
    error: overrides.error || '',
    summary: overrides.summary || '',
    workspaceDir: overrides.workspaceDir || '',
    aiOutput: overrides.aiOutput || '',
    writtenFiles: Array.isArray(overrides.writtenFiles) ? overrides.writtenFiles.slice() : [],
    previewFilePath: overrides.previewFilePath || '',
    previewDisplayPath: overrides.previewDisplayPath || '',
    previewContent: overrides.previewContent || '',
    previewError: overrides.previewError || '',
    previewLoading: Boolean(overrides.previewLoading),
    previewTruncated: Boolean(overrides.previewTruncated),
    trace: Array.isArray(overrides.trace) ? overrides.trace.slice() : [],
    consoleLines: Array.isArray(overrides.consoleLines) ? overrides.consoleLines.slice() : [],
    expertGate: {
      reasons: Array.isArray(initialExpertGate.reasons) ? initialExpertGate.reasons.slice() : [],
      categories: Array.isArray(initialExpertGate.categories) ? initialExpertGate.categories.slice() : [],
      categoryTotals: initialExpertGate.categoryTotals && typeof initialExpertGate.categoryTotals === 'object'
        ? { ...initialExpertGate.categoryTotals }
        : {},
      lastCategory: typeof initialExpertGate.lastCategory === 'string' ? initialExpertGate.lastCategory : '',
      round: Number(initialExpertGate.round || 0),
    },
    planProposal: overrides.planProposal || '',
    planStatus: overrides.planStatus || 'idle',
    planDueAt: Number(overrides.planDueAt || 0),
    planModel: overrides.planModel || '',
    planError: overrides.planError || '',
  };
}

function upsertTask(payload) {
  const id = payload?.id || payload?.taskId;
  if (!id) return null;

  let task = tasks.get(id);
  if (!task) {
    task = createTaskRecord(id, payload);
    tasks.set(id, task);
    return task;
  }

  if (payload.description) task.description = payload.description;
  if (payload.status) task.status = payload.status;
  if (payload.createdAt) task.createdAt = payload.createdAt;
  if (typeof payload.rerunOfTaskId === 'string') task.rerunOfTaskId = payload.rerunOfTaskId;
  if (typeof payload.rerunOfDescription === 'string') task.rerunOfDescription = payload.rerunOfDescription;
  if (typeof payload.followUpOfTaskId === 'string') task.followUpOfTaskId = payload.followUpOfTaskId;
  if (typeof payload.followUpOfDescription === 'string') task.followUpOfDescription = payload.followUpOfDescription;
  if (typeof payload.result === 'string') task.result = payload.result;
  if (typeof payload.error === 'string') task.error = payload.error;
  if (typeof payload.summary === 'string') task.summary = payload.summary;
  if (typeof payload.workspaceDir === 'string') task.workspaceDir = payload.workspaceDir;
  if (typeof payload.aiOutput === 'string') task.aiOutput = payload.aiOutput;
  if (Array.isArray(payload.writtenFiles)) task.writtenFiles = payload.writtenFiles.slice();
  if (typeof payload.previewFilePath === 'string') task.previewFilePath = payload.previewFilePath;
  if (typeof payload.previewDisplayPath === 'string') task.previewDisplayPath = payload.previewDisplayPath;
  if (typeof payload.previewContent === 'string') task.previewContent = payload.previewContent;
  if (typeof payload.previewError === 'string') task.previewError = payload.previewError;
  if (typeof payload.previewLoading === 'boolean') task.previewLoading = payload.previewLoading;
  if (typeof payload.previewTruncated === 'boolean') task.previewTruncated = payload.previewTruncated;
  if (Array.isArray(payload.trace)) task.trace = payload.trace.slice();
  if (Array.isArray(payload.consoleLines)) task.consoleLines = payload.consoleLines.slice();
  if (payload.expertGate && typeof payload.expertGate === 'object') {
    task.expertGate = {
      reasons: Array.isArray(payload.expertGate.reasons) ? payload.expertGate.reasons.slice() : [],
      categories: Array.isArray(payload.expertGate.categories) ? payload.expertGate.categories.slice() : [],
      categoryTotals: payload.expertGate.categoryTotals && typeof payload.expertGate.categoryTotals === 'object'
        ? { ...payload.expertGate.categoryTotals }
        : {},
      lastCategory: typeof payload.expertGate.lastCategory === 'string' ? payload.expertGate.lastCategory : '',
      round: Number(payload.expertGate.round || 0),
    };
  }
  if (typeof payload.planProposal === 'string') task.planProposal = payload.planProposal;
  if (typeof payload.planStatus === 'string') task.planStatus = payload.planStatus;
  if (typeof payload.planDueAt === 'number') task.planDueAt = payload.planDueAt;
  if (typeof payload.planModel === 'string') task.planModel = payload.planModel;
  if (typeof payload.planError === 'string') task.planError = payload.planError;
  return task;
}

function requestTaskList() {
  vscode.postMessage({ command: 'listTasks' });
}

function syncTaskList(taskList) {
  const previousSelectedTaskId = selectedTaskId;
  tasks.clear();

  for (const payload of taskList || []) {
    upsertTask(payload);
  }

  if (previousSelectedTaskId && tasks.has(previousSelectedTaskId)) {
    selectedTaskId = previousSelectedTaskId;
  } else {
    selectedTaskId = tasks.size > 0 ? [...tasks.keys()][0] : null;
  }

  if (pendingFollowUpTaskId && !tasks.has(pendingFollowUpTaskId)) {
    clearTaskFollowUp();
  }

  updateTaskUI();
  updateTaskDetail(selectedTaskId);
  updateConsole(selectedTaskId);
  updateStats();
  updateTaskActionState();
  updateApprovalList();
}

function formatTaskFilePath(task, filePath) {
  if (!task?.workspaceDir || !filePath || !filePath.startsWith(task.workspaceDir)) return filePath;
  const relative = filePath.slice(task.workspaceDir.length).replace(/^\//, '');
  return relative || filePath;
}

function requestTaskFilePreview(taskId, filePath) {
  const task = tasks.get(taskId);
  if (!task) return;
  task.previewLoading = true;
  task.previewFilePath = filePath;
  task.previewDisplayPath = formatTaskFilePath(task, filePath);
  task.previewError = '';
  task.previewContent = '';
  task.previewTruncated = false;
  if (selectedTaskId === taskId) updateTaskDetail(taskId);
  vscode.postMessage({ command: 'previewTaskFile', taskId, filePath });
}

function buildTaskFileUrl(taskId, filePath, download = false) {
  const query = `taskId=${encodeURIComponent(taskId)}&amp;filePath=${encodeURIComponent(filePath)}`;
  return `/api/task-file?${query}${download ? '&amp;download=1' : ''}`;
}

async function copyTextToClipboard(text) {
  if (navigator.clipboard?.writeText) {
    await navigator.clipboard.writeText(text);
    return;
  }

  const textarea = document.createElement('textarea');
  textarea.value = text;
  textarea.setAttribute('readonly', 'true');
  textarea.style.position = 'fixed';
  textarea.style.opacity = '0';
  textarea.style.pointerEvents = 'none';
  document.body.appendChild(textarea);
  textarea.select();
  const copied = document.execCommand('copy');
  document.body.removeChild(textarea);

  if (!copied) {
    throw new Error('浏览器拒绝了复制请求');
  }
}

async function copyTaskFilePath(taskId, filePath) {
  const task = tasks.get(taskId);
  const displayPath = task ? formatTaskFilePath(task, filePath) : filePath;
  try {
    await copyTextToClipboard(filePath);
    appendText('system', `已复制文件路径: ${filePath}\n（任务内显示路径: ${displayPath}）`);
  } catch (error) {
    appendText('system', `复制路径失败: ${error?.message || String(error)}`);
  }
}

function revealTaskFile(taskId, filePath) {
  vscode.postMessage({ command: 'revealTaskFile', taskId, filePath });
}

// ---------------------------------------------------------------------------
// Welcome message
// ---------------------------------------------------------------------------

const WELCOME_TEXT = `欢迎使用 CloudWBot。

默认情况下，输入会被当成任务交给本地执行器；只有 /chat 会强制走普通问答。
每个任务会先生成更稳妥的执行方案，提供 5 秒确认/取消窗口；无操作将默认执行。

/task <描述> - 显式创建并执行任务
/chat <内容> - 显式进入普通问答模式
/status - 查看系统状态
/agent <任务> - 委托给 Agent
/search <查询> - RAG 知识检索
/github search <关键词> - 搜索 GitHub 仓库
/hints [查询词] - 搜索经验提示
/models - 查看本地已安装模型
/plan [任务描述] - 让 AI 推荐模型
/asktask <问题> - 基于当前选中的已完成任务继续追问；若内容包含"修改/打开/运行"等动作，会转成继续执行任务
/clear - 清空对话历史
/theme - 切换深色/浅色主题

⚙️  配置（环境变量，在 .env 文件或终端中设置）:
  OLLAMA_MODEL  — 指定 Ollama 模型（留空自动使用第一个已安装模型）
  OLLAMA_URL    — Ollama 服务地址（默认 http://localhost:11434）
  PORT          — 服务端口（默认 3000）
  TASK_NON_FATAL_COMMAND_PREFIXES — 允许失败但不终止任务的命令前缀（逗号分隔，默认 open,xdg-open,start,explorer.exe）`;

// ---------------------------------------------------------------------------
// Chat
// ---------------------------------------------------------------------------

function appendMessage(role, html, id) {
  const msgs = el('chat-messages');
  const div = document.createElement('div');
  div.className = `msg msg-${role}`;
  if (id) div.dataset.msgId = id;
  div.innerHTML = html;
  msgs.appendChild(div);
  msgs.scrollTop = msgs.scrollHeight;
  return div;
}

function appendText(role, text, id) {
  return appendMessage(role, escHtml(text).replace(/\n/g, '<br>'), id);
}

function initWelcome() {
  appendMessage('system', `<pre class="welcome">${escHtml(WELCOME_TEXT)}</pre>`);
}

/** @param {string} cmd @param {string} rest */
function handleCommand(cmd, rest) {
  switch (cmd) {
    case '/clear':
      clearTaskFollowUp();
      el('chat-messages').innerHTML = '';
      initWelcome();
      break;

    case '/status':
      appendText('system', '正在查询系统状态...');
      vscode.postMessage({ command: 'getStatus' });
      break;

    case '/models':
      appendText('system', '正在查询已安装模型...');
      vscode.postMessage({ command: 'listModels' });
      break;

    case '/hints':
      appendText('system', `正在搜索提示: ${rest || '(全部)'}`);
      vscode.postMessage({ command: 'getHints', query: rest });
      break;

    case '/chat':
      sendChat(rest);
      break;

    case '/task':
      createTask(rest || '未命名任务');
      break;

    case '/agent':
      createTask(rest || '未命名 Agent 任务');
      break;

    case '/search':
      appendText('user', `/search ${rest}`);
      appendText('system', `RAG 知识检索: ${rest}（功能待集成知识库）`);
      break;

    case '/github':
      if (rest.startsWith('search ')) {
        const q = rest.slice(7).trim();
        appendText('user', `/github search ${q}`);
        appendText('system', `正在搜索 GitHub: ${q}`);
        vscode.postMessage({ command: 'searchGithub', query: q });
      }
      break;

    case '/plan':
      sendChat(`请推荐适合以下任务的本地 AI 模型: ${rest}`);
      break;

    case '/asktask':
    case '/followup':
      if (rest) {
        sendTaskFollowUp(selectedTaskId, rest);
      } else {
        armTaskFollowUp();
      }
      break;

    case '/theme':
      toggleTheme();
      break;

    default:
      appendText('system', `未知命令: ${cmd}。输入 /clear 查看帮助。`);
  }
}

function sendInput() {
  const input = /** @type {HTMLTextAreaElement} */ (el('chat-input'));
  const text = input.value.trim();
  if (!text) return;
  if (chatStreaming) {
    appendText('system', '当前有 AI 响应正在生成，请等待完成后再继续。');
    return;
  }
  input.value = '';
  input.style.height = 'auto';

  if (text.startsWith('/')) {
    const [cmd, ...rest] = text.split(' ');
    handleCommand(cmd, rest.join(' ').trim());
    return;
  }

  if (pendingFollowUpTaskId) {
    const taskId = pendingFollowUpTaskId;
    clearTaskFollowUp();
    sendTaskFollowUp(taskId, text);
    return;
  }

  // Default: treat as a task
  createTask(text);
}

// ---------------------------------------------------------------------------
// Theme management
// ---------------------------------------------------------------------------

function toggleTheme() {
  currentTheme = currentTheme === 'dark' ? 'light' : 'dark';
  document.body.setAttribute('data-theme', currentTheme);
  localStorage.setItem('cloudwbot-theme', currentTheme);
  appendText('system', `已切换到${currentTheme === 'dark' ? '深色' : '浅色'}主题`);
}

function loadTheme() {
  const saved = localStorage.getItem('cloudwbot-theme');
  if (saved) {
    currentTheme = saved;
    document.body.setAttribute('data-theme', currentTheme);
  }
}

// ---------------------------------------------------------------------------
// Regular chat (via Ollama)
// ---------------------------------------------------------------------------

function sendChat(text) {
  if (!text) return;
  appendText('user', text);
  appendMessage('assistant', '<span class="cursor">▊</span>', `chat-reply-${Date.now()}`);
  chatStreaming = true;
  updateTaskActionState();

  vscode.postMessage({ command: 'chat', text });
}

function sendTaskFollowUp(taskId, question) {
  const task = tasks.get(taskId);
  if (!task) {
    appendText('system', '未找到可追问的任务。');
    return;
  }
  if (!isTaskFinished(task)) {
    appendText('system', '该任务尚未完成，暂时不能追问。');
    return;
  }

  appendText('user', question);
  appendMessage('system', `🧭 正在基于任务"${escHtml(task.description)}"处理追问`);
  appendMessage(
    'assistant',
    '<div class="assistant-kind">任务追问</div><span class="cursor">▊</span>',
    `task-follow-up-${Date.now()}`,
  );
  chatStreaming = true;
  updateTaskActionState();
  vscode.postMessage({ command: 'askTask', taskId, question });
}

// ---------------------------------------------------------------------------
// Task management
// ---------------------------------------------------------------------------

function generateTaskId() {
  return Date.now().toString(16) + Math.floor(Math.random() * 0xffffffff).toString(16).padStart(8, '0');
}

function createTask(description, options = {}) {
  const id = generateTaskId();
  const task = {
    id,
    description,
    status: 'pending',
    createdAt: new Date().toISOString(),
    rerunOfTaskId: options.rerunOfTaskId || '',
    rerunOfDescription: options.rerunOfDescription || '',
    result: '',
    summary: '',
    workspaceDir: '',
    aiOutput: '',
    writtenFiles: [],
    trace: [],
    consoleLines: [],
    planProposal: '',
    planStatus: 'planning',
    planDueAt: 0,
    planModel: '',
    planError: '',
  };
  tasks.set(id, task);
  selectedTaskId = id;
  clearTaskFollowUp();

  appendText('user', description);
  appendMessage(
    'system',
    `🤖 我已理解您的任务，已创建任务并进入执行前规划<br>
任务ID: <code>${escHtml(id)}</code><br>
描述: <em>${escHtml(description)}</em>...<br>
${task.rerunOfTaskId ? `重跑来源: <code>${escHtml(task.rerunOfTaskId)}</code><br>` : ''}
正在本地生成执行方案，5秒内可确认或取消；若无操作将默认执行。`,
  );

  updateTaskUI();
  updateTaskDetail(id);
  updateConsole(id);
  updateStats();
  updateTaskActionState();
  updateApprovalList();

  executeTask(id, description);
}

function executeTask(taskId, description) {
  const task = tasks.get(taskId);
  if (!task) return;
  task.status = 'pending';
  task.result = '';
  task.error = '';
  task.summary = '';
  task.aiOutput = '';
  task.trace = [];
  task.consoleLines = [];
  task.expertGate = { reasons: [], categories: [], categoryTotals: {}, lastCategory: '', round: 0 };
  task.planProposal = '';
  task.planStatus = 'planning';
  task.planDueAt = 0;
  task.planModel = '';
  task.planError = '';
  updateTaskUI();
  updateTaskDetail(taskId);
  updateConsole(taskId);
  updateStats();
  updateTaskActionState();
  updateApprovalList();

  vscode.postMessage({
    command: 'executeTask',
    taskId,
    description,
    rerunOfTaskId: task.rerunOfTaskId || '',
    rerunOfDescription: task.rerunOfDescription || '',
  });
}

function updateTaskUI() {
  const list = el('task-list');
  list.innerHTML = '';
  const sortedTasks = [...tasks.values()].sort((left, right) => {
    return new Date(right.createdAt).getTime() - new Date(left.createdAt).getTime();
  });
  for (const t of sortedTasks) {
    const id = t.id;
    const item = document.createElement('div');
    item.className = `task-item ${t.status} ${id === selectedTaskId ? 'selected' : ''}`;
    item.dataset.taskId = id;
    item.innerHTML = `
      <div class="task-name">${escHtml(`AI任务: ${t.description}`)}</div>
      <div class="task-meta">
        <span class="task-status status-${t.status}">${humanTaskStatus(t.status)}</span>
        <span class="task-time">${fmtDate(t.createdAt)}</span>
      </div>`;
    item.addEventListener('click', () => selectTask(id));
    list.appendChild(item);
  }
}

function selectTask(id) {
  selectedTaskId = id;
  updateTaskUI();
  updateTaskDetail(id);
  updateConsole(id);
  updateTaskActionState();
}

function updateTaskDetail(taskId) {
  const t = tasks.get(taskId || selectedTaskId);
  const traceEl = el('trace-content');
  if (!t) {
    traceEl.innerHTML = '<div class="empty-hint">选中任务后显示执行轨迹</div>';
    updateTaskActionState();
    return;
  }
  const sections = [];

  const planStateLabel = {
    planning: '方案生成中',
    'awaiting-confirmation': '待确认',
    approving: '确认中',
    approved: '已确认',
    'auto-approved': '自动确认',
    cancelling: '取消中',
    cancelled: '已取消',
    failed: '方案失败',
    idle: '未启用',
  };

  if (t.planStatus && t.planStatus !== 'idle') {
    const remain = getPlanCountdownSeconds(t);
    const statusLabel = planStateLabel[t.planStatus] || t.planStatus;
    const meta = [`状态: ${statusLabel}`];
    if (t.planModel) meta.push(`模型: ${t.planModel}`);
    if (isTaskAwaitingPlanConfirmation(t)) meta.push(`倒计时: ${remain}s`);
    const actionButtons = isTaskAwaitingPlanConfirmation(t)
      ? `<div class="task-plan-actions"><button type="button" class="small-btn" data-approve-task="${escHtml(t.id)}">确认执行</button><button type="button" class="small-btn" data-cancel-task="${escHtml(t.id)}">取消任务</button></div>`
      : '';
    const planBody = t.planProposal
      ? renderTaskPre(t.planProposal)
      : '<div class="empty-hint">正在生成任务方案...</div>';
    const planError = t.planError ? renderTaskPre(t.planError, 'task-error-pre') : '';
    sections.push(
      renderTaskSection(
        '预执行方案',
        `<div class="task-plan-meta">${escHtml(meta.join(' · '))}</div>${actionButtons}${planBody}${planError}`,
      ),
    );
  }

  if (t.summary) {
    sections.push(renderTaskSection('完成摘要', renderTaskPre(t.summary)));
  } else if (t.status === 'running') {
    sections.push(renderTaskSection('完成摘要', '<div class="empty-hint">任务完成后会自动生成摘要</div>'));
  }

  if (t.aiOutput) {
    sections.push(renderTaskSection('AI 实时输出', `<pre class="task-detail-pre task-ai-pre">${renderHighlightedCode(t.aiOutput)}</pre>`));
  } else if (t.status === 'running') {
    sections.push(renderTaskSection('AI 实时输出', '<div class="empty-hint">等待模型返回内容...</div>'));
  }

  if (t.writtenFiles.length > 0) {
    sections.push(
      renderTaskSection(
        '产物文件',
        `<div class="task-file-list">${t.writtenFiles
          .map((filePath) => {
            const displayPath = formatTaskFilePath(t, filePath);
            const activeClass = t.previewFilePath === filePath ? ' active' : '';
            const encodedFilePath = encodeURIComponent(filePath);
            return `<div class="task-file-row"><button type="button" class="task-file-item${activeClass}" data-task-id="${escHtml(t.id)}" data-preview-file="${encodedFilePath}" title="点击预览 ${escHtml(displayPath)}">${escHtml(displayPath)}</button><div class="task-file-links"><a class="task-file-link" href="${buildTaskFileUrl(t.id, filePath)}" target="_blank" rel="noopener noreferrer">打开</a><a class="task-file-link" href="${buildTaskFileUrl(t.id, filePath, true)}">下载</a><button type="button" class="task-file-link" data-task-id="${escHtml(t.id)}" data-copy-file="${encodedFilePath}">复制路径</button><button type="button" class="task-file-link" data-task-id="${escHtml(t.id)}" data-reveal-file="${encodedFilePath}">Finder</button></div></div>`;
          })
          .join('')}</div>`,
      ),
    );
  }

  if (t.previewLoading || t.previewContent || t.previewError) {
    let previewBody = '<div class="empty-hint">点击上方文件名可加载预览</div>';
    if (t.previewLoading) {
      previewBody = '<div class="empty-hint">正在加载文件预览...</div>';
    } else if (t.previewError) {
      previewBody = renderTaskPre(t.previewError, 'task-error-pre');
    } else if (t.previewContent) {
      const ext = t.previewFilePath ? t.previewFilePath.split('.').pop() : '';
      const highlighted = renderHighlightedCode(t.previewContent, ext);
      previewBody = `<pre class="task-detail-pre task-preview-pre">${highlighted}</pre>${t.previewTruncated ? '<div class="empty-hint">预览内容已截断，当前最多显示前 24 KB。</div>' : ''}`;
    }

    sections.push(
      renderTaskSection(
        `文件预览${t.previewDisplayPath ? `: ${escHtml(t.previewDisplayPath)}` : ''}`,
        previewBody,
      ),
    );
  }

  if (t.error) {
    sections.push(renderTaskSection('错误', renderTaskPre(t.error, 'task-error-pre')));
  }

  const expertGate = t.expertGate && typeof t.expertGate === 'object' ? t.expertGate : null;
  const gateReasons = expertGate && Array.isArray(expertGate.reasons) ? expertGate.reasons : [];
  if (gateReasons.length > 0) {
    const categoryTotals = expertGate.categoryTotals && typeof expertGate.categoryTotals === 'object'
      ? Object.entries(expertGate.categoryTotals)
        .map(([key, value]) => `${key}:${value}`)
        .join(' · ')
      : '';
    const gateMeta = [
      expertGate.lastCategory ? `最近类别: ${expertGate.lastCategory}` : '',
      Number(expertGate.round) > 0 ? `轮次: ${expertGate.round}` : '',
      categoryTotals ? `累计: ${categoryTotals}` : '',
    ].filter(Boolean).join(' · ');
    const gateBody = `${gateMeta ? `<div class="task-plan-meta">${escHtml(gateMeta)}</div>` : ''}${gateReasons.map((reason) => `<div class="trace-line">• ${escHtml(reason)}</div>`).join('')}`;
    sections.push(renderTaskSection('专家守门失败原因', gateBody));
  }

  if (t.result && t.status !== 'running') {
    sections.push(renderTaskSection('执行结果', renderTaskPre(t.result)));
  }

  sections.push(
    renderTaskSection(
      '执行轨迹',
      t.trace.length === 0
        ? '<div class="empty-hint">该任务暂未产生执行轨迹</div>'
        : t.trace.map((line) => `<div class="trace-line">${escHtml(line)}</div>`).join(''),
    ),
  );

  const rerunSource = renderRerunSource(t);
  const followUpSource = renderFollowUpSource(t);

  traceEl.innerHTML = `
    <div class="trace-header">
      <div>任务: <strong>${escHtml(`AI任务: ${t.description}`)}</strong></div>
      <div>状态: <span class="status-${t.status}">${humanTaskStatus(t.status)}</span></div>
      <div>创建时间: ${fmtDate(t.createdAt)}</div>
      ${followUpSource ? `<div class="task-source-note">继续来源: ${escHtml(followUpSource)}</div>` : ''}
      ${rerunSource ? `<div class="task-source-note">重跑来源: ${escHtml(rerunSource)}</div>` : ''}
      ${t.workspaceDir ? `<div>工作目录: <code>${escHtml(t.workspaceDir)}</code></div>` : ''}
      <div class="task-follow-up-hint">${isTaskAwaitingPlanConfirmation(t) ? '当前处于方案确认窗口：可确认执行或取消，超时会自动执行。' : canRerunTask(t) ? '该任务已结束，可点击"重新执行"重跑；也可点击"追问任务"继续分析。' : isTaskFinished(t) ? '可点击"追问任务"继续提问；如果输入里包含修改、打开、运行等动作，会自动转成继续执行任务。' : '任务完成后可继续追问'}</div>
    </div>
    ${sections.join('')}`;
  updateTaskActionState();
}

function updateConsole(taskId) {
  const t = tasks.get(taskId || selectedTaskId);
  const consoleEl = el('console-output');
  if (!t || t.consoleLines.length === 0) {
    consoleEl.innerHTML = '<div class="empty-hint">选中任务后自动显示 shell/code 工具的实时输出</div>';
    return;
  }
  consoleEl.innerHTML = t.consoleLines
    .map((l) => `<div class="console-line">${escHtml(l)}</div>`)
    .join('');
  consoleEl.scrollTop = consoleEl.scrollHeight;
}

function updateStats() {
  let total = 0, running = 0, done = 0, failed = 0;
  for (const t of tasks.values()) {
    total++;
    if (t.status === 'running') running++;
    else if (t.status === 'completed') done++;
    else if (t.status === 'failed' || t.status === 'blocked' || t.status === 'interrupted') failed++;
  }
  el('stat-total').textContent = String(total);
  el('stat-running').textContent = String(running);
  el('stat-done').textContent = String(done);
  el('stat-failed').textContent = String(failed);
}

// ---------------------------------------------------------------------------
// Message handler (from server)
// ---------------------------------------------------------------------------

window.addEventListener('message', (event) => {
  const msg = event.data;

  switch (msg.command) {
    case 'chatToken': {
      const msgs = el('chat-messages');
      const last = msgs.querySelector('.msg-assistant:last-child');
      if (last) {
        last.innerHTML = last.innerHTML.replace('<span class="cursor">▊</span>', '');
        last.innerHTML += escHtml(msg.token).replace(/\n/g, '<br>') + '<span class="cursor">▊</span>';
        msgs.scrollTop = msgs.scrollHeight;
      }
      break;
    }

    case 'chatDone': {
      const msgs = el('chat-messages');
      const last = msgs.querySelector('.msg-assistant:last-child');
      if (last) last.innerHTML = last.innerHTML.replace('<span class="cursor">▊</span>', '');
      chatStreaming = false;
      updateTaskActionState();
      break;
    }

    case 'chatError': {
      chatStreaming = false;
      const errText = msg.error || '未知错误';
      let hint = '';
      if (errText.includes('ECONNREFUSED') || errText.includes('connect')) {
        hint = '请确认 Ollama 已启动（运行 ollama serve）';
      } else if (errText.includes('model') || errText.includes('not found')) {
        hint = '请设置 OLLAMA_MODEL 环境变量或运行 ollama pull <模型名>';
      }
      appendMessage(
        'system',
        `❌ 对话请求失败: ${escHtml(errText)}${hint ? `<br>💡 ${escHtml(hint)}` : ''}`,
      );
      updateTaskActionState();
      break;
    }

    case 'taskPlanStart': {
      const t = upsertTask({
        taskId: msg.taskId,
        description: msg.description,
        status: 'pending',
        planStatus: 'planning',
        planProposal: '',
        planDueAt: 0,
        planError: '',
      });
      if (t) {
        t.trace.push('🧠 正在本地生成任务方案...');
        updateTaskUI();
        updateTaskDetail(msg.taskId);
        updateStats();
        updateApprovalList();
      }
      break;
    }

    case 'taskPlanMeta': {
      const t = upsertTask({ taskId: msg.taskId });
      if (t) {
        t.planModel = msg.model || '';
        if (msg.model) {
          t.trace.push(`🧠 方案模型: ${msg.model}`);
        }
        if (msg.taskId === selectedTaskId) updateTaskDetail(msg.taskId);
      }
      break;
    }

    case 'taskPlanToken': {
      const t = upsertTask({ taskId: msg.taskId });
      if (t) {
        t.planStatus = t.planStatus === 'idle' ? 'planning' : t.planStatus;
        t.planProposal = (t.planProposal || '') + (msg.token || '');
        if (msg.taskId === selectedTaskId) updateTaskDetail(msg.taskId);
      }
      break;
    }

    case 'taskPlanError': {
      const t = upsertTask({ taskId: msg.taskId });
      if (t) {
        t.planError = msg.error || '';
        t.trace.push(`⚠️ ${msg.error || '方案生成失败，已使用默认方案'}`);
        if (msg.taskId === selectedTaskId) updateTaskDetail(msg.taskId);
      }
      appendText('system', msg.error || '任务方案生成失败，已切换默认方案');
      break;
    }

    case 'taskPlanReady': {
      const t = upsertTask({ taskId: msg.taskId, description: msg.description, status: 'pending' });
      if (t) {
        t.planStatus = 'awaiting-confirmation';
        t.planProposal = typeof msg.plan === 'string' ? msg.plan : t.planProposal;
        t.planDueAt = Number(msg.dueAt || 0);
        t.planModel = msg.model || t.planModel || '';
        t.planError = '';
        t.trace.push(`✅ 任务方案已生成，等待确认（${msg.timeoutSeconds || 5}秒后自动执行）`);
        if (msg.taskId === selectedTaskId) updateTaskDetail(msg.taskId);
        updateTaskUI();
        updateStats();
        updateApprovalList();
      }
      appendMessage(
        'system',
        `任务 <code>${escHtml(msg.taskId)}</code> 的执行方案已生成，${escHtml(String(msg.timeoutSeconds || 5))}秒内可确认或取消，超时默认执行。`,
      );
      break;
    }

    case 'taskPlanDecision': {
      const t = upsertTask({ taskId: msg.taskId });
      if (t) {
        if (msg.decision === 'approved') {
          t.planStatus = 'approved';
          t.planDueAt = 0;
          t.trace.push('✅ 用户已确认执行任务方案，开始执行');
        } else if (msg.decision === 'auto-approved') {
          t.planStatus = 'auto-approved';
          t.planDueAt = 0;
          t.trace.push(`✅ ${msg.timeoutSeconds || 5}秒内未操作，已自动执行任务方案`);
        } else if (msg.decision === 'cancelled') {
          t.planStatus = 'cancelled';
          t.planDueAt = 0;
          t.trace.push(`⚠️ ${msg.reason || '任务已取消'}`);
        } else if (msg.decision === 'ignored') {
          t.planDueAt = 0;
          if (msg.reason) t.trace.push(`ℹ️ ${msg.reason}`);
        }
        if (msg.taskId === selectedTaskId) updateTaskDetail(msg.taskId);
        updateTaskUI();
        updateStats();
        updateApprovalList();
      }

      if (msg.decision === 'approved') {
        appendText('system', `任务 ${msg.taskId} 已确认，开始执行。`);
      } else if (msg.decision === 'auto-approved') {
        appendText('system', `任务 ${msg.taskId} 在 ${msg.timeoutSeconds || 5} 秒内未操作，已默认执行。`);
      } else if (msg.decision === 'cancelled') {
        appendText('system', `任务 ${msg.taskId} 已取消：${msg.reason || '用户取消'}`);
      } else if (msg.decision === 'ignored' && msg.reason) {
        appendText('system', msg.reason);
      }
      break;
    }

    case 'taskStream': {
      const t = upsertTask({ taskId: msg.taskId });
      if (t) {
        if (t.status === 'pending') t.status = 'running';
        t.aiOutput = (t.aiOutput || '') + msg.token;
        if (msg.taskId === selectedTaskId) updateTaskDetail(msg.taskId);
      }
      break;
    }

    case 'taskTrace': {
      const t = upsertTask({ taskId: msg.taskId });
      if (t) {
        if (t.status === 'pending') t.status = 'running';
        t.trace.push(msg.message);
        if (msg.taskId === selectedTaskId) updateTaskDetail(msg.taskId);
      }
      break;
    }

    case 'taskStatus': {
      const t = upsertTask(msg);
      if (t) {
        t.status = msg.status;
        if (msg.status === 'running' && isTaskAwaitingPlanConfirmation(t)) {
          t.planStatus = 'approved';
          t.planDueAt = 0;
        }
        if (msg.status === 'completed' || msg.status === 'failed' || msg.status === 'blocked' || msg.status === 'interrupted') {
          // Normalize stale planning states when terminal status arrives out-of-order.
          if (t.planStatus === 'planning' || t.planStatus === 'awaiting-confirmation' || t.planStatus === 'approving') {
            t.planStatus = 'approved';
          }
          t.planDueAt = 0;
        }
        if (msg.workspaceDir) t.workspaceDir = msg.workspaceDir;
        if (typeof msg.rerunOfTaskId === 'string') t.rerunOfTaskId = msg.rerunOfTaskId;
        if (typeof msg.rerunOfDescription === 'string') t.rerunOfDescription = msg.rerunOfDescription;
        if (typeof msg.followUpOfTaskId === 'string') t.followUpOfTaskId = msg.followUpOfTaskId;
        if (typeof msg.followUpOfDescription === 'string') t.followUpOfDescription = msg.followUpOfDescription;
        if (msg.summary) t.summary = msg.summary;
        if (msg.result) t.result = msg.result;
        if (Array.isArray(msg.writtenFiles)) t.writtenFiles = msg.writtenFiles.slice();
        if (msg.autoSelect) selectedTaskId = msg.taskId;
        if (msg.error) {
          t.error = msg.error;
          t.trace.push(`❌ ${msg.error}`);
        }

        if (msg.status === 'completed' || msg.status === 'failed' || msg.status === 'blocked' || msg.status === 'interrupted') {
          appendMessage(
            'system',
            `任务: <strong>${escHtml(`AI任务: ${t.description}`)}</strong><br>
状态: <span class="status-${t.status}">${humanTaskStatus(t.status)}</span><br>
创建时间: ${fmtDate(t.createdAt)}<br>
${t.followUpOfTaskId ? `继续来源: <code>${escHtml(renderFollowUpSource(t))}</code><br>` : ''}
${t.rerunOfTaskId ? `重跑来源: <code>${escHtml(renderRerunSource(t))}</code><br>` : ''}
${t.workspaceDir ? `工作目录: <code>${escHtml(t.workspaceDir)}</code><br>` : ''}
${t.summary ? `摘要:<br><pre class="result-pre">${escHtml(t.summary)}</pre>` : ''}
${t.error ? `错误:<br><pre class="result-pre">${escHtml(t.error)}</pre>` : ''}
<div class="task-chat-hint">详情见右侧"执行结果"和"实时控制台"。${isTaskFinished(t) ? ' 选中该任务后可继续点击"追问任务"，或输入 /asktask <问题>。' : ''}</div>`,
          );
        }

        updateTaskUI();
        updateTaskDetail(msg.taskId);
        updateStats();
        updateTaskActionState();
        updateApprovalList();
      }
      break;
    }

    case 'console': {
      const t = upsertTask({ taskId: msg.taskId });
      if (t) {
        if (t.status === 'pending') t.status = 'running';
        t.consoleLines.push(msg.line);
        if (msg.taskId === selectedTaskId) updateConsole(msg.taskId);
      }
      break;
    }

    case 'followUpTaskStarted': {
      appendText('system', `该追问已转为继续执行任务，正在基于原任务先生成执行方案并开启5秒确认窗口（超时默认执行）: ${msg.question || ''}`);
      break;
    }

    case 'taskList': {
      syncTaskList(msg.tasks || []);
      break;
    }

    case 'taskFilePreview': {
      const t = upsertTask({ taskId: msg.taskId });
      if (t) {
        t.previewLoading = false;
        t.previewFilePath = msg.filePath || t.previewFilePath || '';
        t.previewDisplayPath = msg.displayPath || formatTaskFilePath(t, t.previewFilePath);
        t.previewContent = msg.content || '';
        t.previewError = msg.error || '';
        t.previewTruncated = Boolean(msg.truncated);
        if (msg.taskId === selectedTaskId) updateTaskDetail(msg.taskId);
      }
      break;
    }

    case 'fileActionResult': {
      appendText('system', msg.message || '文件操作已完成');
      break;
    }

    case 'status': {
      const dot = el('system-status');
      dot.className = msg.ollamaOk ? 'status-dot green' : 'status-dot red';
      dot.textContent = msg.ollamaOk ? '● 系统正常' : '● Ollama 未运行';
      const ollamaLine = msg.ollamaOk
        ? '✅ 正常'
        : '❌ 未运行\n    请先安装并启动 Ollama: https://ollama.com\n    启动命令: ollama serve\n    然后拉取模型: ollama pull qwen2.5:7b（或其他模型）';
      appendText(
        'system',
        `系统状态:\n  Ollama (${msg.url || 'http://localhost:11434'}): ${ollamaLine}\n  已安装模型: ${msg.models.length > 0 ? msg.models.join(', ') : '无（请运行 ollama pull <模型名>）'}`,
      );
      break;
    }

    case 'models': {
      appendText('system', `本地已安装模型:\n${msg.models.length > 0 ? msg.models.map((m) => `  • ${m}`).join('\n') : '  (无已安装模型)'}`);
      break;
    }

    case 'hints': {
      if (!msg.hints || msg.hints.length === 0) {
        appendText('system', '未找到匹配的经验提示');
      } else {
        const lines = msg.hints.map((h) => `[${h.category}] ${h.content}`).join('\n');
        appendText('system', `经验提示:\n${lines}`);
      }
      break;
    }

    case 'githubResults': {
      if (!msg.results || msg.results.length === 0) {
        appendText('system', 'GitHub 搜索无结果');
      } else {
        const lines = msg.results
          .map((r) => `⭐${r.stars} ${r.name}\n  ${r.description}\n  ${r.url}`)
          .join('\n\n');
        appendText('system', `GitHub 搜索结果:\n${lines}`);
      }
      break;
    }
  }
});

// ---------------------------------------------------------------------------
// Event listeners
// ---------------------------------------------------------------------------

el('btn-send').addEventListener('click', sendInput);

el('chat-input').addEventListener('keydown', (e) => {
  if (e.key === 'Enter' && !e.shiftKey) {
    e.preventDefault();
    sendInput();
  }
});

el('chat-input').addEventListener('input', function () {
  this.style.height = 'auto';
  this.style.height = Math.min(this.scrollHeight, 120) + 'px';
});

el('btn-clear-chat').addEventListener('click', () => {
  el('chat-messages').innerHTML = '';
  initWelcome();
});

el('btn-clear-console').addEventListener('click', () => {
  if (selectedTaskId) {
    const t = tasks.get(selectedTaskId);
    if (t) t.consoleLines = [];
  }
  el('console-output').innerHTML = '<div class="empty-hint">已清空</div>';
});

el('btn-new-task').addEventListener('click', () => {
  clearTaskFollowUp();
  el('chat-input').focus();
  el('chat-input').placeholder = '输入任务描述后按 Enter 创建任务...';
});

el('btn-follow-up-task').addEventListener('click', armTaskFollowUp);
el('btn-rerun-task').addEventListener('click', rerunSelectedTask);
el('btn-cancel-context').addEventListener('click', clearTaskFollowUp);

el('trace-content').addEventListener('click', (event) => {
  const target = event.target;
  if (!(target instanceof HTMLElement)) return;

  const approveButton = target.closest('[data-approve-task]');
  if (approveButton instanceof HTMLElement) {
    const taskId = approveButton.getAttribute('data-approve-task');
    if (!taskId) return;
    requestTaskPlanApproval(taskId);
    return;
  }

  const cancelPlanButton = target.closest('[data-cancel-task]');
  if (cancelPlanButton instanceof HTMLElement) {
    const taskId = cancelPlanButton.getAttribute('data-cancel-task');
    if (!taskId) return;
    requestTaskPlanCancellation(taskId);
    return;
  }

  const previewButton = target.closest('[data-preview-file]');
  if (previewButton instanceof HTMLElement) {
    const taskId = previewButton.getAttribute('data-task-id');
    const encodedFilePath = previewButton.getAttribute('data-preview-file');
    if (!taskId || !encodedFilePath) return;
    requestTaskFilePreview(taskId, decodeURIComponent(encodedFilePath));
    return;
  }

  const copyButton = target.closest('[data-copy-file]');
  if (copyButton instanceof HTMLElement) {
    const taskId = copyButton.getAttribute('data-task-id');
    const encodedFilePath = copyButton.getAttribute('data-copy-file');
    if (!taskId || !encodedFilePath) return;
    copyTaskFilePath(taskId, decodeURIComponent(encodedFilePath));
    return;
  }

  const revealButton = target.closest('[data-reveal-file]');
  if (revealButton instanceof HTMLElement) {
    const taskId = revealButton.getAttribute('data-task-id');
    const encodedFilePath = revealButton.getAttribute('data-reveal-file');
    if (!taskId || !encodedFilePath) return;
    revealTaskFile(taskId, decodeURIComponent(encodedFilePath));
    return;
  }
});

el('btn-refresh-tasks').addEventListener('click', requestTaskList);
el('btn-refresh-approvals').addEventListener('click', updateApprovalList);
el('btn-refresh-supplement').addEventListener('click', () => {
  el('supplement-list').innerHTML = '<div class="empty-hint">暂无待补充信息任务</div>';
});

el('approval-list').addEventListener('click', (event) => {
  const target = event.target;
  if (!(target instanceof HTMLElement)) return;

  const approveButton = target.closest('[data-approve-task]');
  if (approveButton instanceof HTMLElement) {
    const taskId = approveButton.getAttribute('data-approve-task');
    if (!taskId) return;
    requestTaskPlanApproval(taskId);
    return;
  }

  const cancelButton = target.closest('[data-cancel-task]');
  if (cancelButton instanceof HTMLElement) {
    const taskId = cancelButton.getAttribute('data-cancel-task');
    if (!taskId) return;
    requestTaskPlanCancellation(taskId);
  }
});

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------

loadTheme();
initWelcome();
updateTaskActionState();
updateApprovalList();
vscode.postMessage({ command: 'getStatus' });
requestTaskList();

setInterval(() => {
  const hasAwaiting = [...tasks.values()].some((task) => isTaskAwaitingPlanConfirmation(task));
  if (!hasAwaiting) return;
  if (selectedTaskId && isTaskAwaitingPlanConfirmation(tasks.get(selectedTaskId))) {
    updateTaskDetail(selectedTaskId);
  }
  updateApprovalList();
}, 500);
