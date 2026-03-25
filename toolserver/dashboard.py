"""Self-contained HTML dashboard for the tool server.

The page is returned as a single string so we do not need any static-file
serving infrastructure.  The primary UI element is a **chat panel** where
users can talk to the Ollama agent in natural language.  Status cards,
available models, and the tool catalogue are kept as collapsible
reference sections below the chat.
"""

from __future__ import annotations

# All tool metadata referenced by the dashboard.
TOOLS = [
    ("get_system_info",    "POST /tool/get_system_info",    "OS、Shell、用户、工作空间、磁盘信息"),
    ("list_models",        "POST /tool/list_models",        "列出本地 Ollama 可用模型"),
    ("run_command",        "POST /tool/run_command",        "在工作空间内执行 Shell 命令（策略白名单）"),
    ("read_file",          "POST /tool/read_file",          "读取工作空间内的文件"),
    ("write_file",         "POST /tool/write_file",         "写入文件（自动备份 + 幂等检测）"),
    ("list_dir",           "POST /tool/list_dir",           "列出目录内容"),
    ("stat",               "POST /tool/stat",               "文件/目录元数据"),
    ("db_schema",          "POST /tool/db_schema",          "查看数据库结构"),
    ("db_query",           "POST /tool/db_query",           "执行只读 SQL 查询"),
    ("db_exec",            "POST /tool/db_exec",            "执行写 SQL（策略检查）"),
    ("analyze_fields",     "POST /tool/analyze_fields",     "采样表数据并推断字段语义"),
    ("design_wide_table",  "POST /tool/design_wide_table",  "根据分析结果自动设计宽表"),
    ("create_wide_table",  "POST /tool/create_wide_table",  "在数据库中创建宽表"),
    ("etl_to_wide_table",  "POST /tool/etl_to_wide_table",  "增量加载源数据到宽表"),
    ("visualize_3d",       "POST /tool/visualize_3d",       "生成交互式 3-D 散点 HTML"),
]


def render_dashboard() -> str:
    """Return the complete HTML string for the dashboard page."""
    tool_rows = "\n".join(
        f'<tr><td class="tool-name">{name}</td>'
        f'<td class="tool-endpoint"><code>{endpoint}</code></td>'
        f'<td>{desc}</td></tr>'
        for name, endpoint, desc in TOOLS
    )

    return f"""\
<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Ollama Local Agent</title>
<style>
  :root {{
    --bg: #0f1117; --card: #1a1d27; --border: #2d3140;
    --text: #e0e0e0; --muted: #8b8fa3; --accent: #6c8cff;
    --green: #2ecc71; --red: #e74c3c; --yellow: #f1c40f;
    --chat-user: #2a2d3a; --chat-bot: #1e2230;
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    background: var(--bg); color: var(--text); line-height: 1.6;
    padding: 0; height: 100vh; display: flex; flex-direction: column;
  }}

  /* --- Header --- */
  header {{
    background: var(--card); border-bottom: 1px solid var(--border);
    padding: 0.75rem 1.5rem; display: flex; align-items: center;
    justify-content: space-between; flex-shrink: 0;
  }}
  header h1 {{ font-size: 1.2rem; }}
  header h1 span {{ color: var(--accent); }}
  .header-right {{ display: flex; align-items: center; gap: 1rem; font-size: 0.85rem; }}
  .header-right select {{
    background: var(--bg); color: var(--text); border: 1px solid var(--border);
    border-radius: 6px; padding: 0.3rem 0.5rem; font-size: 0.85rem;
  }}
  .header-right .status {{ display: flex; align-items: center; gap: 0.3rem; }}
  .status-dot {{
    display: inline-block; width: 8px; height: 8px;
    border-radius: 50%; vertical-align: middle;
  }}
  .status-dot.ok {{ background: var(--green); }}
  .status-dot.err {{ background: var(--red); }}
  .status-dot.loading {{ background: var(--yellow); animation: pulse 1s infinite; }}
  @keyframes pulse {{ 0%,100%{{opacity:1}} 50%{{opacity:0.4}} }}

  /* --- Main area: chat + optional sidebar --- */
  .main {{ flex: 1; display: flex; overflow: hidden; }}
  .chat-area {{ flex: 1; display: flex; flex-direction: column; min-width: 0; }}

  /* --- Chat messages --- */
  .chat-messages {{
    flex: 1; overflow-y: auto; padding: 1rem 1.5rem;
    display: flex; flex-direction: column; gap: 0.75rem;
  }}
  .msg {{
    max-width: 85%; padding: 0.75rem 1rem; border-radius: 12px;
    font-size: 0.92rem; line-height: 1.55; white-space: pre-wrap;
    word-break: break-word;
  }}
  .msg.user {{
    align-self: flex-end; background: var(--chat-user);
    border-bottom-right-radius: 4px;
  }}
  .msg.bot {{
    align-self: flex-start; background: var(--chat-bot);
    border: 1px solid var(--border); border-bottom-left-radius: 4px;
  }}
  .msg.bot .tool-badge {{
    display: inline-block; background: var(--accent); color: #fff;
    font-size: 0.72rem; padding: 0.1rem 0.45rem; border-radius: 4px;
    margin-right: 0.3rem; font-weight: 600; opacity: 0.85;
  }}
  .msg.system {{
    align-self: center; color: var(--muted); font-size: 0.82rem;
    background: transparent; padding: 0.3rem;
  }}
  .msg.error {{
    align-self: center; color: var(--red); font-size: 0.85rem;
    background: rgba(231,76,60,0.1); border: 1px solid rgba(231,76,60,0.3);
    border-radius: 8px; padding: 0.6rem 1rem;
  }}
  .typing {{ color: var(--muted); font-style: italic; font-size: 0.85rem; padding: 0.5rem 1rem; }}

  /* --- Input bar --- */
  .chat-input {{
    border-top: 1px solid var(--border); background: var(--card);
    padding: 0.75rem 1.5rem; display: flex; gap: 0.5rem; flex-shrink: 0;
  }}
  .chat-input input {{
    flex: 1; background: var(--bg); color: var(--text);
    border: 1px solid var(--border); border-radius: 8px;
    padding: 0.6rem 1rem; font-size: 0.95rem; outline: none;
  }}
  .chat-input input:focus {{ border-color: var(--accent); }}
  .chat-input button {{
    background: var(--accent); color: #fff; border: none;
    border-radius: 8px; padding: 0.6rem 1.2rem; font-size: 0.95rem;
    cursor: pointer; font-weight: 600; white-space: nowrap;
    transition: opacity 0.15s;
  }}
  .chat-input button:hover {{ opacity: 0.85; }}
  .chat-input button:disabled {{ opacity: 0.4; cursor: not-allowed; }}
  .chat-input .btn-secondary {{
    background: transparent; color: var(--muted); border: 1px solid var(--border);
    font-weight: 400; padding: 0.6rem 0.8rem;
  }}
  .chat-input .btn-stop {{
    background: var(--red); display: none;
  }}
  .chat-input .btn-stop.visible {{
    display: inline-block;
  }}

  /* --- Progress steps --- */
  .progress-step {{
    display: flex; align-items: center; gap: 0.5rem;
    padding: 0.35rem 0.8rem; font-size: 0.82rem; color: var(--muted);
    border-left: 2px solid var(--border); margin-left: 0.5rem;
  }}
  .progress-step.running {{
    border-left-color: var(--yellow); color: var(--yellow);
  }}
  .progress-step.done {{
    border-left-color: var(--green); color: var(--text);
  }}
  .progress-step.failed {{
    border-left-color: var(--red); color: var(--red);
  }}
  .progress-step .step-icon {{
    font-size: 0.9rem; flex-shrink: 0;
  }}
  .progress-step .step-detail {{
    font-size: 0.75rem; color: var(--muted);
    max-height: 0; overflow: hidden; transition: max-height 0.2s;
    white-space: pre-wrap; word-break: break-all;
  }}
  .progress-step .step-detail.open {{
    max-height: 200px; overflow-y: auto;
    margin-top: 0.2rem; padding: 0.3rem;
    background: var(--bg); border-radius: 4px;
  }}
  .progress-step .step-toggle {{
    cursor: pointer; font-size: 0.72rem; color: var(--accent);
    margin-left: auto; flex-shrink: 0;
  }}

  /* --- Reference panel (collapsible sidebar on wide screens) --- */
  .ref-panel {{
    width: 340px; background: var(--card); border-left: 1px solid var(--border);
    overflow-y: auto; flex-shrink: 0; padding: 1rem; font-size: 0.85rem;
  }}
  @media (max-width: 800px) {{
    .ref-panel {{ display: none; }}
  }}
  .ref-panel h3 {{
    font-size: 0.8rem; color: var(--muted); text-transform: uppercase;
    letter-spacing: 0.04em; margin-bottom: 0.5rem; margin-top: 1rem;
    cursor: pointer; user-select: none;
  }}
  .ref-panel h3:first-child {{ margin-top: 0; }}
  .ref-panel h3::before {{ content: "▸ "; }}
  .ref-panel h3.open::before {{ content: "▾ "; }}
  .ref-panel .section-body {{ display: none; }}
  .ref-panel h3.open + .section-body {{ display: block; }}

  .cards {{ display: grid; grid-template-columns: 1fr 1fr; gap: 0.5rem; margin-bottom: 0.5rem; }}
  .card {{
    background: var(--bg); border: 1px solid var(--border);
    border-radius: 8px; padding: 0.6rem 0.8rem;
  }}
  .card .label {{ font-size: 0.72rem; color: var(--muted); text-transform: uppercase; }}
  .card .value {{ font-size: 0.95rem; font-weight: 600; }}

  .model-list {{ display: flex; flex-wrap: wrap; gap: 0.4rem; }}
  .model-tag {{
    background: var(--bg); border: 1px solid var(--border);
    border-radius: 5px; padding: 0.15rem 0.5rem; font-size: 0.8rem;
  }}

  table {{ width: 100%; border-collapse: collapse; font-size: 0.8rem; }}
  th, td {{ text-align: left; padding: 0.4rem 0.5rem; border-bottom: 1px solid var(--border); }}
  th {{ color: var(--muted); font-weight: 500; font-size: 0.72rem; text-transform: uppercase; }}
  .tool-name {{ font-weight: 600; white-space: nowrap; }}
  .tool-endpoint code {{ color: var(--accent); font-size: 0.78rem; }}

  .links {{ display: flex; gap: 0.75rem; margin-top: 0.5rem; }}
  .links a {{ color: var(--accent); text-decoration: none; font-size: 0.82rem; }}
  .links a:hover {{ text-decoration: underline; }}

  /* Welcome message */
  .welcome {{ text-align: center; padding: 2rem 1rem; color: var(--muted); }}
  .welcome h2 {{ font-size: 1.3rem; color: var(--text); margin-bottom: 0.5rem; }}
  .welcome p {{ font-size: 0.9rem; max-width: 500px; margin: 0 auto 1rem; }}
  .examples {{ display: flex; flex-wrap: wrap; gap: 0.5rem; justify-content: center; }}
  .examples button {{
    background: var(--card); color: var(--text); border: 1px solid var(--border);
    border-radius: 8px; padding: 0.5rem 1rem; font-size: 0.85rem;
    cursor: pointer; transition: border-color 0.15s;
  }}
  .examples button:hover {{ border-color: var(--accent); }}
</style>
</head>
<body>

<!-- Header -->
<header>
  <h1>🤖 <span>Ollama Local Agent</span></h1>
  <div class="header-right">
    <div class="status" id="status-bar">
      <span class="status-dot loading"></span>
      <span>连接中…</span>
    </div>
    <select id="model-select" title="选择模型">
      <option value="">加载中…</option>
    </select>
  </div>
</header>

<!-- Main layout -->
<div class="main">
  <!-- Chat area -->
  <div class="chat-area">
    <div class="chat-messages" id="chat-messages">
      <div class="welcome" id="welcome">
        <h2>👋 你好！我是本地 AI 助手</h2>
        <p>我可以帮你执行命令、读写文件、查询数据库、分析数据——所有操作都在本地安全执行。</p>
        <p style="font-size:0.82rem; color:var(--muted)">试试下面的问题，或直接输入你的需求：</p>
        <div class="examples">
          <button onclick="sendExample(this)">我的系统是什么环境？</button>
          <button onclick="sendExample(this)">工作空间里有哪些文件？</button>
          <button onclick="sendExample(this)">查看数据库结构</button>
          <button onclick="sendExample(this)">本地有哪些模型可用？</button>
        </div>
      </div>
    </div>
    <div class="chat-input">
      <input type="text" id="chat-input" placeholder="输入消息… (Enter 发送)" autocomplete="off" />
      <button id="send-btn" onclick="sendMessage()">发送</button>
      <button id="stop-btn" class="btn-stop" onclick="cancelTask()">⏹ 停止</button>
      <button class="btn-secondary" onclick="resetChat()" title="清空对话">🗑</button>
    </div>
  </div>

  <!-- Reference sidebar -->
  <div class="ref-panel" id="ref-panel">
    <h3 class="open" onclick="toggleSection(this)">服务状态</h3>
    <div class="section-body">
      <div class="cards">
        <div class="card"><div class="label">Server</div><div class="value"><span class="status-dot ok"></span> 运行中</div></div>
        <div class="card"><div class="label">Ollama</div><div class="value" id="ollama-status"><span class="status-dot loading"></span> …</div></div>
        <div class="card"><div class="label">系统</div><div class="value" id="sys-os">—</div></div>
        <div class="card"><div class="label">工作空间</div><div class="value" id="sys-workspace" style="font-size:0.75rem;word-break:break-all">—</div></div>
      </div>
    </div>

    <h3 onclick="toggleSection(this)">可用模型</h3>
    <div class="section-body">
      <div class="model-list" id="model-list">
        <span class="model-tag" style="color:var(--muted)">加载中…</span>
      </div>
    </div>

    <h3 onclick="toggleSection(this)">可用工具 ({len(TOOLS)})</h3>
    <div class="section-body">
      <table>
        <thead><tr><th>工具</th><th>说明</th></tr></thead>
        <tbody>
          {"".join(f'<tr><td class="tool-name">{n}</td><td>{d}</td></tr>' for n, _, d in TOOLS)}
        </tbody>
      </table>
    </div>

    <h3 onclick="toggleSection(this)">链接</h3>
    <div class="section-body">
      <div class="links">
        <a href="/docs">📖 API 文档</a>
        <a href="/openapi.json">📄 OpenAPI</a>
      </div>
    </div>
  </div>
</div>

<script>
const chatBox = document.getElementById("chat-messages");
const chatInput = document.getElementById("chat-input");
const sendBtn = document.getElementById("send-btn");
const stopBtn = document.getElementById("stop-btn");
const modelSel = document.getElementById("model-select");
let sessionId = "web-" + Date.now();
let currentEventSource = null;

// --- UI helpers ---
function addMsg(role, text) {{
  const w = document.getElementById("welcome");
  if (w) w.style.display = "none";

  const div = document.createElement("div");
  div.className = "msg " + role;
  if (role === "bot") {{
    div.innerHTML = escapeHtml(text)
      .replace(/```([\\s\\S]*?)```/g, '<pre style="background:var(--bg);padding:0.5rem;border-radius:6px;overflow-x:auto;margin:0.3rem 0">$1</pre>')
      .replace(/`([^`]+)`/g, '<code style="background:var(--bg);padding:0.1rem 0.3rem;border-radius:3px">$1</code>')
      .replace(/\\n/g, '<br>');
  }} else {{
    div.textContent = text;
  }}
  chatBox.appendChild(div);
  chatBox.scrollTop = chatBox.scrollHeight;
  return div;
}}

function escapeHtml(s) {{
  const d = document.createElement("div");
  d.textContent = s;
  return d.innerHTML;
}}

function addProgressStep(icon, text, cls) {{
  const div = document.createElement("div");
  div.className = "progress-step " + (cls || "");
  div.innerHTML = '<span class="step-icon">' + icon + '</span><span>' + escapeHtml(text) + '</span>';
  chatBox.appendChild(div);
  chatBox.scrollTop = chatBox.scrollHeight;
  return div;
}}

function updateProgressStep(div, icon, text, cls, preview) {{
  div.className = "progress-step " + (cls || "");
  let html = '<span class="step-icon">' + icon + '</span><span>' + escapeHtml(text) + '</span>';
  if (preview) {{
    const id = "detail-" + Date.now();
    html += '<span class="step-toggle" onclick="toggleDetail(\'' + id + '\')">详情</span>';
    html += '<div class="step-detail" id="' + id + '">' + escapeHtml(preview) + '</div>';
  }}
  div.innerHTML = html;
  chatBox.scrollTop = chatBox.scrollHeight;
}}

function toggleDetail(id) {{
  const el = document.getElementById(id);
  if (el) el.classList.toggle("open");
}}

function setRunning(running) {{
  chatInput.disabled = running;
  sendBtn.disabled = running;
  sendBtn.style.display = running ? "none" : "";
  stopBtn.classList.toggle("visible", running);
}}

function toggleSection(h3) {{
  h3.classList.toggle("open");
}}

// --- Cancel ---
async function cancelTask() {{
  try {{
    await fetch("/api/chat/cancel?session_id=" + encodeURIComponent(sessionId), {{method: "POST"}});
  }} catch (_) {{}}
}}

// --- Send with streaming ---
async function sendMessage() {{
  const text = chatInput.value.trim();
  if (!text) return;
  chatInput.value = "";
  addMsg("user", text);
  setRunning(true);

  // Map of step number → progress DOM element for live updates
  const stepElements = {{}};

  try {{
    const res = await fetch("/api/chat/stream", {{
      method: "POST",
      headers: {{"Content-Type": "application/json"}},
      body: JSON.stringify({{
        message: text,
        model: modelSel.value || "",
        session_id: sessionId,
      }}),
    }});

    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";

    while (true) {{
      const {{ done, value }} = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, {{ stream: true }});

      // Parse SSE events from buffer
      const lines = buffer.split("\\n");
      buffer = lines.pop() || "";

      let eventType = null;
      let dataStr = "";

      for (const line of lines) {{
        if (line.startsWith("event: ")) {{
          eventType = line.slice(7).trim();
        }} else if (line.startsWith("data: ")) {{
          dataStr = line.slice(6);
          if (eventType && dataStr) {{
            try {{
              const data = JSON.parse(dataStr);
              handleSSE(eventType, data, stepElements);
            }} catch (_) {{}}
          }}
          eventType = null;
          dataStr = "";
        }}
      }}
    }}
  }} catch (e) {{
    const errDiv = document.createElement("div");
    errDiv.className = "msg error";
    errDiv.textContent = "⚠️ 连接失败: " + e.message;
    chatBox.appendChild(errDiv);
  }} finally {{
    setRunning(false);
    chatInput.focus();
  }}
}}

function handleSSE(event, data, stepElements) {{
  switch (event) {{
    case "thinking":
      if (!stepElements[data.step]) {{
        stepElements[data.step] = addProgressStep("🤔", "第 " + data.step + " 步：思考中…", "running");
      }}
      break;

    case "tool_start":
      if (stepElements[data.step]) {{
        updateProgressStep(stepElements[data.step], "🔧", "调用 " + data.tool + "…", "running");
      }} else {{
        stepElements[data.step] = addProgressStep("🔧", "调用 " + data.tool + "…", "running");
      }}
      break;

    case "tool_done":
      if (stepElements[data.step]) {{
        const icon = data.ok ? "✅" : "❌";
        const label = data.ok ? (data.tool + " 完成") : (data.tool + " 失败");
        updateProgressStep(stepElements[data.step], icon, label, data.ok ? "done" : "failed", data.preview);
      }}
      break;

    case "reply":
      // Remove any lingering "thinking" step
      Object.values(stepElements).forEach(el => {{
        if (el.classList.contains("running")) el.remove();
      }});
      addMsg("bot", data.reply);
      break;

    case "error":
      const errDiv = document.createElement("div");
      errDiv.className = "msg error";
      errDiv.textContent = "⚠️ " + (data.error || "未知错误");
      chatBox.appendChild(errDiv);
      chatBox.scrollTop = chatBox.scrollHeight;
      break;

    case "cancelled":
      const cancelDiv = document.createElement("div");
      cancelDiv.className = "msg system";
      cancelDiv.textContent = "⏹ " + (data.message || "已取消");
      chatBox.appendChild(cancelDiv);
      chatBox.scrollTop = chatBox.scrollHeight;
      break;

    case "done":
      break;
  }}
}}

function sendExample(btn) {{
  chatInput.value = btn.textContent;
  sendMessage();
}}

async function resetChat() {{
  try {{
    await fetch("/api/chat/reset?session_id=" + encodeURIComponent(sessionId), {{method: "POST"}});
  }} catch (_) {{}}
  sessionId = "web-" + Date.now();
  chatBox.innerHTML = "";
  chatBox.innerHTML = `
    <div class="welcome" id="welcome">
      <h2>👋 你好！我是本地 AI 助手</h2>
      <p>我可以帮你执行命令、读写文件、查询数据库、分析数据——所有操作都在本地安全执行。</p>
      <p style="font-size:0.82rem; color:var(--muted)">试试下面的问题，或直接输入你的需求：</p>
      <div class="examples">
        <button onclick="sendExample(this)">我的系统是什么环境？</button>
        <button onclick="sendExample(this)">工作空间里有哪些文件？</button>
        <button onclick="sendExample(this)">查看数据库结构</button>
        <button onclick="sendExample(this)">本地有哪些模型可用？</button>
      </div>
    </div>`;
}}

chatInput.addEventListener("keydown", (e) => {{
  if (e.key === "Enter" && !e.shiftKey && !sendBtn.disabled) {{
    e.preventDefault();
    sendMessage();
  }}
}});

// --- Boot: populate status & models ---
(async () => {{
  try {{
    const r = await fetch("/tool/get_system_info", {{method:"POST", headers:{{"Content-Type":"application/json"}}, body:"{{}}"}});
    const info = await r.json();
    if (info.ok) {{
      document.getElementById("sys-os").textContent = info.result.platform || info.result.system;
      document.getElementById("sys-workspace").textContent = info.result.workspace_root;
    }}
  }} catch (_) {{}}

  try {{
    const r = await fetch("/api/chat/models");
    const data = await r.json();
    const sel = document.getElementById("model-select");
    const modelListEl = document.getElementById("model-list");
    const statusEl = document.getElementById("ollama-status");
    const statusBar = document.getElementById("status-bar");

    if (data.models && data.models.length > 0) {{
      sel.innerHTML = data.models.map(m =>
        '<option value="' + m + '"' + (m === data.default ? ' selected' : '') + '>' + m + '</option>'
      ).join("");
      statusEl.innerHTML = '<span class="status-dot ok"></span> ' + data.models.length + ' 模型';
      statusBar.innerHTML = '<span class="status-dot ok"></span><span>已连接</span>';
      modelListEl.innerHTML = data.models.map(m => '<span class="model-tag">' + m + '</span>').join("");
    }} else {{
      sel.innerHTML = '<option value="">无可用模型</option>';
      statusEl.innerHTML = '<span class="status-dot err"></span> 未连接';
      statusBar.innerHTML = '<span class="status-dot err"></span><span>Ollama 未连接</span>';
      modelListEl.innerHTML = '<span class="model-tag" style="color:var(--muted)">无</span>';
    }}
  }} catch (_) {{
    document.getElementById("status-bar").innerHTML = '<span class="status-dot err"></span><span>Ollama 未连接</span>';
  }}

  chatInput.focus();
}})();
</script>
</body>
</html>"""
