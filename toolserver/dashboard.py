"""Self-contained HTML dashboard for the tool server.

The page is returned as a single string so we do not need any static-file
serving infrastructure.  The primary UI element is a **chat panel** where
users can talk to the Ollama agent in natural language.  Status cards,
available models, and the tool catalogue are kept as collapsible
reference sections below the chat.

Professional UX features:
- Guided onboarding with categorized example cards
- Keyboard shortcuts (Ctrl+K focus, ↑/↓ history, Escape cancel)
- Accessibility: aria attributes, focus outlines, semantic roles
- Message actions: copy, retry on error, response time
- Responsive design: mobile-friendly touch targets
- Help tooltip and shortcut reference
- Light/dark theme toggle
- Loading states and smooth transitions
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
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1" />
<title>Ollama Local Agent</title>
<style>
  /* --- Theme variables --- */
  :root {{
    --bg: #0f1117; --card: #1a1d27; --border: #2d3140;
    --text: #e0e0e0; --muted: #8b8fa3; --accent: #6c8cff;
    --green: #2ecc71; --red: #e74c3c; --yellow: #f1c40f;
    --chat-user: #2a2d3a; --chat-bot: #1e2230;
    --focus-ring: rgba(108,140,255,0.5);
    --hover-bg: rgba(108,140,255,0.08);
  }}
  html.light {{
    --bg: #f5f6fa; --card: #ffffff; --border: #e0e2eb;
    --text: #1a1d27; --muted: #6b7085; --accent: #4a6cf7;
    --green: #27ae60; --red: #e74c3c; --yellow: #f39c12;
    --chat-user: #e8eaf6; --chat-bot: #f0f1f5;
    --focus-ring: rgba(74,108,247,0.4);
    --hover-bg: rgba(74,108,247,0.06);
  }}

  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    background: var(--bg); color: var(--text); line-height: 1.6;
    padding: 0; height: 100vh; display: flex; flex-direction: column;
    transition: background 0.2s, color 0.2s;
  }}

  /* --- Focus visible for keyboard navigation --- */
  :focus-visible {{
    outline: 2px solid var(--focus-ring);
    outline-offset: 2px;
    border-radius: 4px;
  }}

  /* --- Header --- */
  header {{
    background: var(--card); border-bottom: 1px solid var(--border);
    padding: 0.6rem 1.2rem; display: flex; align-items: center;
    justify-content: space-between; flex-shrink: 0;
    transition: background 0.2s;
  }}
  header h1 {{ font-size: 1.15rem; display: flex; align-items: center; gap: 0.4rem; }}
  header h1 span {{ color: var(--accent); }}
  .header-right {{ display: flex; align-items: center; gap: 0.6rem; font-size: 0.82rem; }}
  .header-right select {{
    background: var(--bg); color: var(--text); border: 1px solid var(--border);
    border-radius: 6px; padding: 0.3rem 0.5rem; font-size: 0.82rem;
    min-height: 32px; cursor: pointer;
  }}
  .header-right .status {{ display: flex; align-items: center; gap: 0.3rem; }}
  .header-btn {{
    background: transparent; border: 1px solid var(--border); color: var(--muted);
    border-radius: 6px; padding: 0.25rem 0.5rem; font-size: 0.82rem;
    cursor: pointer; display: flex; align-items: center; gap: 0.25rem;
    min-height: 32px; min-width: 32px; justify-content: center;
    transition: border-color 0.15s, color 0.15s;
  }}
  .header-btn:hover {{ border-color: var(--accent); color: var(--text); }}
  .header-btn[aria-pressed="true"] {{ border-color: var(--accent); color: var(--accent); }}
  .status-dot {{
    display: inline-block; width: 8px; height: 8px;
    border-radius: 50%; vertical-align: middle; flex-shrink: 0;
  }}
  .status-dot.ok {{ background: var(--green); }}
  .status-dot.err {{ background: var(--red); }}
  .status-dot.loading {{ background: var(--yellow); animation: pulse 1s infinite; }}
  @keyframes pulse {{ 0%,100%{{opacity:1}} 50%{{opacity:0.4}} }}

  /* --- Main area: chat + optional sidebar --- */
  .main {{ flex: 1; display: flex; overflow: hidden; }}
  .chat-area {{
    flex: 1; display: flex; flex-direction: column; min-width: 0;
  }}

  /* --- Chat messages --- */
  .chat-messages {{
    flex: 1; overflow-y: auto; padding: 1rem 1.2rem;
    display: flex; flex-direction: column; gap: 0.6rem;
    scroll-behavior: smooth;
  }}
  .msg-wrapper {{
    display: flex; flex-direction: column; gap: 0.15rem;
    max-width: 85%; position: relative;
  }}
  .msg-wrapper.user {{ align-self: flex-end; }}
  .msg-wrapper.bot {{ align-self: flex-start; }}
  .msg {{
    padding: 0.65rem 0.9rem; border-radius: 12px;
    font-size: 0.9rem; line-height: 1.55; white-space: pre-wrap;
    word-break: break-word; position: relative;
  }}
  .msg.user {{
    background: var(--chat-user); border-bottom-right-radius: 4px;
  }}
  .msg.bot {{
    background: var(--chat-bot); border: 1px solid var(--border);
    border-bottom-left-radius: 4px;
  }}
  .msg.bot .tool-badge {{
    display: inline-block; background: var(--accent); color: #fff;
    font-size: 0.72rem; padding: 0.1rem 0.45rem; border-radius: 4px;
    margin-right: 0.3rem; font-weight: 600; opacity: 0.85;
  }}
  .msg.system {{
    align-self: center; color: var(--muted); font-size: 0.8rem;
    background: transparent; padding: 0.3rem; max-width: 100%;
  }}
  .msg.error {{
    align-self: center; color: var(--red); font-size: 0.82rem;
    background: rgba(231,76,60,0.08); border: 1px solid rgba(231,76,60,0.25);
    border-radius: 8px; padding: 0.5rem 0.9rem; max-width: 100%;
  }}

  /* --- Message meta (timestamp, actions) --- */
  .msg-meta {{
    display: flex; align-items: center; gap: 0.4rem;
    font-size: 0.7rem; color: var(--muted); padding: 0 0.3rem;
    opacity: 0; transition: opacity 0.15s;
  }}
  .msg-wrapper:hover .msg-meta {{ opacity: 1; }}
  .msg-wrapper.user .msg-meta {{ justify-content: flex-end; }}
  .msg-action {{
    background: none; border: none; color: var(--muted); cursor: pointer;
    font-size: 0.72rem; padding: 0.1rem 0.3rem; border-radius: 3px;
    transition: color 0.1s, background 0.1s;
  }}
  .msg-action:hover {{ color: var(--accent); background: var(--hover-bg); }}
  .msg-action.copied {{ color: var(--green); }}

  /* --- Input bar --- */
  .chat-input {{
    border-top: 1px solid var(--border); background: var(--card);
    padding: 0.6rem 1.2rem; display: flex; gap: 0.4rem; flex-shrink: 0;
    align-items: center; transition: background 0.2s;
  }}
  .chat-input input {{
    flex: 1; background: var(--bg); color: var(--text);
    border: 1px solid var(--border); border-radius: 8px;
    padding: 0.55rem 0.9rem; font-size: 0.9rem; outline: none;
    min-height: 40px; transition: border-color 0.15s;
  }}
  .chat-input input:focus {{ border-color: var(--accent); box-shadow: 0 0 0 2px var(--focus-ring); }}
  .chat-input button {{
    background: var(--accent); color: #fff; border: none;
    border-radius: 8px; padding: 0.5rem 1rem; font-size: 0.9rem;
    cursor: pointer; font-weight: 600; white-space: nowrap;
    min-height: 40px; min-width: 44px;
    transition: opacity 0.15s, transform 0.1s;
  }}
  .chat-input button:hover {{ opacity: 0.88; }}
  .chat-input button:active {{ transform: scale(0.97); }}
  .chat-input button:disabled {{ opacity: 0.4; cursor: not-allowed; }}
  .chat-input .btn-secondary {{
    background: transparent; color: var(--muted); border: 1px solid var(--border);
    font-weight: 400; padding: 0.5rem 0.7rem;
  }}
  .chat-input .btn-secondary:hover {{ border-color: var(--accent); color: var(--text); }}
  .chat-input .btn-stop {{
    background: var(--red); display: none;
  }}
  .chat-input .btn-stop.visible {{
    display: inline-flex; align-items: center; gap: 0.3rem;
  }}
  .input-hint {{
    font-size: 0.68rem; color: var(--muted); padding: 0.15rem 1.2rem 0;
    display: flex; gap: 0.8rem; flex-shrink: 0;
  }}
  .input-hint kbd {{
    background: var(--bg); border: 1px solid var(--border); border-radius: 3px;
    padding: 0 0.3rem; font-size: 0.65rem; font-family: inherit;
  }}

  /* --- Progress steps --- */
  .progress-step {{
    display: flex; align-items: center; gap: 0.5rem;
    padding: 0.3rem 0.75rem; font-size: 0.8rem; color: var(--muted);
    border-left: 2px solid var(--border); margin-left: 0.5rem;
    transition: border-color 0.2s, color 0.2s;
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
  .progress-step .step-icon {{ font-size: 0.85rem; flex-shrink: 0; }}
  .progress-step .step-detail {{
    font-size: 0.72rem; color: var(--muted);
    max-height: 0; overflow: hidden; transition: max-height 0.2s;
    white-space: pre-wrap; word-break: break-all;
  }}
  .progress-step .step-detail.open {{
    max-height: 200px; overflow-y: auto;
    margin-top: 0.2rem; padding: 0.3rem;
    background: var(--bg); border-radius: 4px;
  }}
  .progress-step .step-toggle {{
    cursor: pointer; font-size: 0.7rem; color: var(--accent);
    margin-left: auto; flex-shrink: 0;
  }}
  .progress-step .step-toggle:hover {{ text-decoration: underline; }}

  /* --- Reference panel (collapsible sidebar) --- */
  .ref-panel {{
    width: 320px; background: var(--card); border-left: 1px solid var(--border);
    overflow-y: auto; flex-shrink: 0; padding: 0.8rem; font-size: 0.82rem;
    transition: background 0.2s;
  }}
  @media (max-width: 800px) {{
    .ref-panel {{ display: none; }}
  }}
  .ref-panel h3 {{
    font-size: 0.75rem; color: var(--muted); text-transform: uppercase;
    letter-spacing: 0.04em; margin-bottom: 0.4rem; margin-top: 0.9rem;
    cursor: pointer; user-select: none; padding: 0.2rem 0;
    transition: color 0.15s;
  }}
  .ref-panel h3:hover {{ color: var(--text); }}
  .ref-panel h3:first-child {{ margin-top: 0; }}
  .ref-panel h3::before {{ content: "▸ "; }}
  .ref-panel h3.open::before {{ content: "▾ "; }}
  .ref-panel .section-body {{ display: none; }}
  .ref-panel h3.open + .section-body {{ display: block; }}

  .cards {{ display: grid; grid-template-columns: 1fr 1fr; gap: 0.4rem; margin-bottom: 0.4rem; }}
  .card {{
    background: var(--bg); border: 1px solid var(--border);
    border-radius: 8px; padding: 0.5rem 0.7rem;
  }}
  .card .label {{ font-size: 0.68rem; color: var(--muted); text-transform: uppercase; }}
  .card .value {{ font-size: 0.9rem; font-weight: 600; }}

  .model-list {{ display: flex; flex-wrap: wrap; gap: 0.35rem; }}
  .model-tag {{
    background: var(--bg); border: 1px solid var(--border);
    border-radius: 5px; padding: 0.12rem 0.45rem; font-size: 0.78rem;
  }}

  table {{ width: 100%; border-collapse: collapse; font-size: 0.78rem; }}
  th, td {{ text-align: left; padding: 0.35rem 0.4rem; border-bottom: 1px solid var(--border); }}
  th {{ color: var(--muted); font-weight: 500; font-size: 0.7rem; text-transform: uppercase; }}
  .tool-name {{ font-weight: 600; white-space: nowrap; }}
  .tool-endpoint code {{ color: var(--accent); font-size: 0.75rem; }}

  .links {{ display: flex; gap: 0.6rem; margin-top: 0.4rem; }}
  .links a {{ color: var(--accent); text-decoration: none; font-size: 0.8rem; }}
  .links a:hover {{ text-decoration: underline; }}

  /* --- Welcome / Onboarding --- */
  .welcome {{ text-align: center; padding: 1.5rem 1rem; color: var(--muted); }}
  .welcome h2 {{ font-size: 1.2rem; color: var(--text); margin-bottom: 0.3rem; }}
  .welcome .subtitle {{
    font-size: 0.88rem; max-width: 520px; margin: 0 auto 0.8rem;
    line-height: 1.5;
  }}
  .welcome .caps {{
    display: flex; flex-wrap: wrap; gap: 0.5rem; justify-content: center;
    margin-bottom: 1.2rem;
  }}
  .cap-badge {{
    display: inline-flex; align-items: center; gap: 0.3rem;
    background: var(--card); border: 1px solid var(--border);
    border-radius: 20px; padding: 0.25rem 0.7rem; font-size: 0.78rem;
    color: var(--text);
  }}
  .cap-badge .cap-icon {{ font-size: 0.85rem; }}
  .example-cats {{ max-width: 580px; margin: 0 auto; }}
  .example-cat {{
    margin-bottom: 0.7rem; text-align: left;
  }}
  .example-cat-title {{
    font-size: 0.72rem; color: var(--muted); text-transform: uppercase;
    letter-spacing: 0.03em; margin-bottom: 0.3rem; padding-left: 0.1rem;
  }}
  .examples {{ display: flex; flex-wrap: wrap; gap: 0.4rem; }}
  .examples button {{
    background: var(--card); color: var(--text); border: 1px solid var(--border);
    border-radius: 8px; padding: 0.45rem 0.85rem; font-size: 0.82rem;
    cursor: pointer; transition: border-color 0.15s, background 0.15s;
    text-align: left;
  }}
  .examples button:hover {{ border-color: var(--accent); background: var(--hover-bg); }}

  /* --- Keyboard shortcut overlay --- */
  .shortcut-overlay {{
    display: none; position: fixed; inset: 0;
    background: rgba(0,0,0,0.6); z-index: 1000;
    justify-content: center; align-items: center;
  }}
  .shortcut-overlay.visible {{ display: flex; }}
  .shortcut-card {{
    background: var(--card); border: 1px solid var(--border);
    border-radius: 12px; padding: 1.5rem; max-width: 400px; width: 90%;
    max-height: 80vh; overflow-y: auto;
  }}
  .shortcut-card h3 {{ font-size: 1rem; margin-bottom: 0.8rem; color: var(--text); }}
  .shortcut-row {{
    display: flex; justify-content: space-between; align-items: center;
    padding: 0.3rem 0; font-size: 0.85rem;
  }}
  .shortcut-row kbd {{
    background: var(--bg); border: 1px solid var(--border); border-radius: 4px;
    padding: 0.15rem 0.45rem; font-size: 0.78rem; font-family: inherit;
    min-width: 28px; text-align: center;
  }}

  /* --- Responsive --- */
  @media (max-width: 600px) {{
    header {{ padding: 0.5rem 0.8rem; }}
    header h1 {{ font-size: 1rem; }}
    .chat-messages {{ padding: 0.6rem 0.8rem; }}
    .chat-input {{ padding: 0.5rem 0.8rem; }}
    .msg {{ font-size: 0.88rem; padding: 0.55rem 0.75rem; }}
    .msg-wrapper {{ max-width: 92%; }}
    .input-hint {{ display: none; }}
    .examples button {{ font-size: 0.8rem; padding: 0.4rem 0.7rem; }}
    .header-right {{ gap: 0.4rem; }}
  }}
</style>
</head>
<body>

<!-- Keyboard shortcut overlay -->
<div class="shortcut-overlay" id="shortcut-overlay" role="dialog" aria-label="键盘快捷键" aria-modal="true">
  <div class="shortcut-card">
    <h3>⌨️ 键盘快捷键</h3>
    <div class="shortcut-row"><span>聚焦输入框</span><kbd>Ctrl</kbd>+<kbd>K</kbd></div>
    <div class="shortcut-row"><span>发送消息</span><kbd>Enter</kbd></div>
    <div class="shortcut-row"><span>上一条消息</span><kbd>↑</kbd></div>
    <div class="shortcut-row"><span>下一条消息</span><kbd>↓</kbd></div>
    <div class="shortcut-row"><span>取消执行</span><kbd>Escape</kbd></div>
    <div class="shortcut-row"><span>清空对话</span><kbd>Ctrl</kbd>+<kbd>Shift</kbd>+<kbd>L</kbd></div>
    <div class="shortcut-row"><span>切换主题</span><kbd>Ctrl</kbd>+<kbd>Shift</kbd>+<kbd>T</kbd></div>
    <div class="shortcut-row"><span>关闭此面板</span><kbd>Escape</kbd> / <kbd>?</kbd></div>
    <div style="margin-top:0.8rem;text-align:center">
      <button class="header-btn" onclick="toggleShortcuts()" style="margin:0 auto">关闭</button>
    </div>
  </div>
</div>

<!-- Header -->
<header role="banner">
  <h1>🤖 <span>Ollama Local Agent</span></h1>
  <div class="header-right">
    <div class="status" id="status-bar" role="status" aria-live="polite">
      <span class="status-dot loading" aria-hidden="true"></span>
      <span>连接中…</span>
    </div>
    <select id="model-select" title="选择模型" aria-label="选择 AI 模型">
      <option value="">加载中…</option>
    </select>
    <button class="header-btn" id="theme-btn" onclick="toggleTheme()" title="切换明暗主题 (Ctrl+Shift+T)" aria-label="切换主题" aria-pressed="false">🌙</button>
    <button class="header-btn" onclick="toggleShortcuts()" title="键盘快捷键 (?)" aria-label="快捷键帮助">⌨️</button>
  </div>
</header>

<!-- Main layout -->
<div class="main" role="main">
  <!-- Chat area -->
  <div class="chat-area">
    <div class="chat-messages" id="chat-messages" role="log" aria-live="polite" aria-label="对话消息">
      <div class="welcome" id="welcome">
        <h2>👋 你好！我是本地 AI 助手</h2>
        <p class="subtitle">我可以帮你执行命令、读写文件、查询数据库、分析数据——所有操作都在本地安全执行，数据不会离开你的电脑。</p>
        <div class="caps" aria-label="核心能力">
          <span class="cap-badge"><span class="cap-icon">💻</span> 命令执行</span>
          <span class="cap-badge"><span class="cap-icon">📁</span> 文件读写</span>
          <span class="cap-badge"><span class="cap-icon">🗄️</span> 数据库查询</span>
          <span class="cap-badge"><span class="cap-icon">📊</span> 数据分析</span>
          <span class="cap-badge"><span class="cap-icon">🔒</span> 本地安全</span>
        </div>
        <div class="example-cats">
          <div class="example-cat">
            <div class="example-cat-title">🚀 快速开始</div>
            <div class="examples">
              <button onclick="sendExample(this)">我的系统是什么环境？</button>
              <button onclick="sendExample(this)">工作空间里有哪些文件？</button>
            </div>
          </div>
          <div class="example-cat">
            <div class="example-cat-title">🗄️ 数据库</div>
            <div class="examples">
              <button onclick="sendExample(this)">查看数据库结构</button>
              <button onclick="sendExample(this)">分析数据库字段含义</button>
            </div>
          </div>
          <div class="example-cat">
            <div class="example-cat-title">📊 分析 &amp; 可视化</div>
            <div class="examples">
              <button onclick="sendExample(this)">设计宽表并加载数据</button>
              <button onclick="sendExample(this)">生成 3D 可视化散点图</button>
            </div>
          </div>
          <div class="example-cat">
            <div class="example-cat-title">🤖 系统</div>
            <div class="examples">
              <button onclick="sendExample(this)">本地有哪些模型可用？</button>
              <button onclick="sendExample(this)">查看磁盘使用情况</button>
            </div>
          </div>
        </div>
      </div>
    </div>
    <div class="input-hint" aria-hidden="true">
      <span><kbd>Enter</kbd> 发送</span>
      <span><kbd>↑</kbd><kbd>↓</kbd> 历史</span>
      <span><kbd>Ctrl+K</kbd> 聚焦</span>
      <span><kbd>Esc</kbd> 取消</span>
    </div>
    <div class="chat-input" role="form" aria-label="发送消息">
      <input type="text" id="chat-input" placeholder="输入消息… (Enter 发送，↑↓ 历史)" autocomplete="off" aria-label="消息输入" />
      <button id="send-btn" onclick="sendMessage()" aria-label="发送">发送</button>
      <button id="stop-btn" class="btn-stop" onclick="cancelTask()" aria-label="停止执行">⏹ 停止</button>
      <button class="btn-secondary" onclick="resetChat()" title="清空对话 (Ctrl+Shift+L)" aria-label="清空对话">🗑</button>
    </div>
  </div>

  <!-- Reference sidebar -->
  <nav class="ref-panel" id="ref-panel" aria-label="参考信息">
    <h3 class="open" onclick="toggleSection(this)" role="button" aria-expanded="true" tabindex="0">服务状态</h3>
    <div class="section-body" role="region">
      <div class="cards">
        <div class="card"><div class="label">Server</div><div class="value"><span class="status-dot ok" aria-hidden="true"></span> 运行中</div></div>
        <div class="card"><div class="label">Ollama</div><div class="value" id="ollama-status"><span class="status-dot loading" aria-hidden="true"></span> …</div></div>
        <div class="card"><div class="label">系统</div><div class="value" id="sys-os">—</div></div>
        <div class="card"><div class="label">工作空间</div><div class="value" id="sys-workspace" style="font-size:0.72rem;word-break:break-all">—</div></div>
      </div>
    </div>

    <h3 onclick="toggleSection(this)" role="button" aria-expanded="false" tabindex="0">可用模型</h3>
    <div class="section-body" role="region">
      <div class="model-list" id="model-list">
        <span class="model-tag" style="color:var(--muted)">加载中…</span>
      </div>
    </div>

    <h3 onclick="toggleSection(this)" role="button" aria-expanded="false" tabindex="0">可用工具 ({len(TOOLS)})</h3>
    <div class="section-body" role="region">
      <table>
        <thead><tr><th>工具</th><th>说明</th></tr></thead>
        <tbody>
          {"".join(f'<tr><td class="tool-name">{n}</td><td>{d}</td></tr>' for n, _, d in TOOLS)}
        </tbody>
      </table>
    </div>

    <h3 onclick="toggleSection(this)" role="button" aria-expanded="false" tabindex="0">链接</h3>
    <div class="section-body" role="region">
      <div class="links">
        <a href="/docs">📖 API 文档</a>
        <a href="/openapi.json">📄 OpenAPI</a>
      </div>
    </div>
  </nav>
</div>

<script>
const chatBox = document.getElementById("chat-messages");
const chatInput = document.getElementById("chat-input");
const sendBtn = document.getElementById("send-btn");
const stopBtn = document.getElementById("stop-btn");
const modelSel = document.getElementById("model-select");
let sessionId = "web-" + Date.now();
let currentEventSource = null;

// --- Input history ---
const inputHistory = [];
let historyIdx = -1;

// --- Theme ---
function toggleTheme() {{
  const isLight = document.documentElement.classList.toggle("light");
  const btn = document.getElementById("theme-btn");
  btn.textContent = isLight ? "☀️" : "🌙";
  btn.setAttribute("aria-pressed", isLight ? "true" : "false");
  try {{ localStorage.setItem("theme", isLight ? "light" : "dark"); }} catch(_) {{}}
}}

function initTheme() {{
  try {{
    const saved = localStorage.getItem("theme");
    if (saved === "light") {{
      document.documentElement.classList.add("light");
      const btn = document.getElementById("theme-btn");
      btn.textContent = "☀️";
      btn.setAttribute("aria-pressed", "true");
    }}
  }} catch (_) {{}}
}}
initTheme();

// --- Shortcut overlay ---
function toggleShortcuts() {{
  const overlay = document.getElementById("shortcut-overlay");
  const visible = overlay.classList.toggle("visible");
  if (visible) {{
    overlay.querySelector("button").focus();
  }} else {{
    chatInput.focus();
  }}
}}

// Focus trap for shortcut overlay
document.getElementById("shortcut-overlay").addEventListener("keydown", (e) => {{
  if (e.key === "Tab") {{
    const btn = e.currentTarget.querySelector("button");
    if (btn) {{ e.preventDefault(); btn.focus(); }}
  }}
}});

// --- UI helpers ---
function addMsg(role, text, opts) {{
  opts = opts || {{}};
  const w = document.getElementById("welcome");
  if (w) w.style.display = "none";

  const wrapper = document.createElement("div");
  wrapper.className = "msg-wrapper " + role;

  const div = document.createElement("div");
  div.className = "msg " + role;
  div.setAttribute("role", role === "bot" ? "article" : "log");

  if (role === "bot") {{
    div.innerHTML = escapeHtml(text)
      .replace(/```([\\s\\S]*?)```/g, '<pre style="background:var(--bg);padding:0.5rem;border-radius:6px;overflow-x:auto;margin:0.3rem 0">$1</pre>')
      .replace(/`([^`]+)`/g, '<code style="background:var(--bg);padding:0.1rem 0.3rem;border-radius:3px">$1</code>')
      .replace(/\\n/g, '<br>');
  }} else if (role === "error") {{
    div.textContent = text;
  }} else if (role === "system") {{
    div.textContent = text;
    chatBox.appendChild(div);
    chatBox.scrollTop = chatBox.scrollHeight;
    return div;
  }} else {{
    div.textContent = text;
  }}

  wrapper.appendChild(div);

  // Meta row: timestamp + actions
  const meta = document.createElement("div");
  meta.className = "msg-meta";

  const ts = document.createElement("span");
  ts.textContent = new Date().toLocaleTimeString("zh-CN", {{hour:"2-digit", minute:"2-digit"}});
  meta.appendChild(ts);

  if (opts.elapsed) {{
    const dur = document.createElement("span");
    dur.textContent = opts.elapsed;
    dur.style.marginLeft = "0.3rem";
    meta.appendChild(dur);
  }}

  // Copy button
  const copyBtn = document.createElement("button");
  copyBtn.className = "msg-action";
  copyBtn.textContent = "📋";
  copyBtn.title = "复制";
  copyBtn.setAttribute("aria-label", "复制消息");
  copyBtn.onclick = () => {{
    navigator.clipboard.writeText(text).then(() => {{
      copyBtn.textContent = "✓";
      copyBtn.classList.add("copied");
      setTimeout(() => {{ copyBtn.textContent = "📋"; copyBtn.classList.remove("copied"); }}, 1500);
    }}).catch(() => {{}});
  }};
  meta.appendChild(copyBtn);

  // Retry button for bot errors
  if (role === "error" && opts.retryText) {{
    const retryBtn = document.createElement("button");
    retryBtn.className = "msg-action";
    retryBtn.textContent = "🔄 重试";
    retryBtn.setAttribute("aria-label", "重试");
    retryBtn.onclick = () => {{
      chatInput.value = opts.retryText;
      sendMessage();
    }};
    meta.appendChild(retryBtn);
  }}

  wrapper.appendChild(meta);
  chatBox.appendChild(wrapper);
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
  div.setAttribute("role", "status");
  div.innerHTML = '<span class="step-icon" aria-hidden="true">' + icon + '</span><span>' + escapeHtml(text) + '</span>';
  chatBox.appendChild(div);
  chatBox.scrollTop = chatBox.scrollHeight;
  return div;
}}

function updateProgressStep(div, icon, text, cls, preview) {{
  div.className = "progress-step " + (cls || "");
  let html = '<span class="step-icon" aria-hidden="true">' + icon + '</span><span>' + escapeHtml(text) + '</span>';
  if (preview) {{
    const id = "detail-" + Date.now();
    html += '<span class="step-toggle" onclick="toggleDetail(\'' + id + '\')" role="button" tabindex="0" aria-label="展开详情">详情</span>';
    html += '<div class="step-detail" id="' + id + '" role="region">' + escapeHtml(preview) + '</div>';
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
  if (running) {{
    chatInput.setAttribute("aria-busy", "true");
  }} else {{
    chatInput.removeAttribute("aria-busy");
  }}
}}

function toggleSection(h3) {{
  const isOpen = h3.classList.toggle("open");
  h3.setAttribute("aria-expanded", isOpen ? "true" : "false");
}}

// --- Cancel ---
async function cancelTask() {{
  try {{
    await fetch("/api/chat/cancel?session_id=" + encodeURIComponent(sessionId), {{method: "POST"}});
  }} catch (_) {{}}
}}

// --- Send with streaming ---
let lastSentText = "";

async function sendMessage() {{
  const text = chatInput.value.trim();
  if (!text) return;
  lastSentText = text;
  chatInput.value = "";

  // Add to history
  if (inputHistory[inputHistory.length - 1] !== text) {{
    inputHistory.push(text);
  }}
  historyIdx = inputHistory.length;

  addMsg("user", text);
  setRunning(true);
  const startTime = Date.now();

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
              handleSSE(eventType, data, stepElements, startTime);
            }} catch (_) {{}}
          }}
          eventType = null;
          dataStr = "";
        }}
      }}
    }}
  }} catch (e) {{
    addMsg("error", "⚠️ 连接失败: " + e.message, {{ retryText: text }});
  }} finally {{
    setRunning(false);
    chatInput.focus();
  }}
}}

function formatElapsed(startTime) {{
  const ms = Date.now() - startTime;
  if (ms < 1000) return ms + "ms";
  return (ms / 1000).toFixed(1) + "s";
}}

function handleSSE(event, data, stepElements, startTime) {{
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
      Object.values(stepElements).forEach(el => {{
        if (el.classList.contains("running")) el.remove();
      }});
      addMsg("bot", data.reply, {{ elapsed: formatElapsed(startTime) }});
      break;

    case "error":
      addMsg("error", "⚠️ " + (data.error || "未知错误"), {{ retryText: lastSentText }});
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
      <p class="subtitle">我可以帮你执行命令、读写文件、查询数据库、分析数据——所有操作都在本地安全执行，数据不会离开你的电脑。</p>
      <div class="caps" aria-label="核心能力">
        <span class="cap-badge"><span class="cap-icon">💻</span> 命令执行</span>
        <span class="cap-badge"><span class="cap-icon">📁</span> 文件读写</span>
        <span class="cap-badge"><span class="cap-icon">🗄️</span> 数据库查询</span>
        <span class="cap-badge"><span class="cap-icon">📊</span> 数据分析</span>
        <span class="cap-badge"><span class="cap-icon">🔒</span> 本地安全</span>
      </div>
      <div class="example-cats">
        <div class="example-cat">
          <div class="example-cat-title">🚀 快速开始</div>
          <div class="examples">
            <button onclick="sendExample(this)">我的系统是什么环境？</button>
            <button onclick="sendExample(this)">工作空间里有哪些文件？</button>
          </div>
        </div>
        <div class="example-cat">
          <div class="example-cat-title">🗄️ 数据库</div>
          <div class="examples">
            <button onclick="sendExample(this)">查看数据库结构</button>
            <button onclick="sendExample(this)">分析数据库字段含义</button>
          </div>
        </div>
        <div class="example-cat">
          <div class="example-cat-title">📊 分析 &amp; 可视化</div>
          <div class="examples">
            <button onclick="sendExample(this)">设计宽表并加载数据</button>
            <button onclick="sendExample(this)">生成 3D 可视化散点图</button>
          </div>
        </div>
        <div class="example-cat">
          <div class="example-cat-title">🤖 系统</div>
          <div class="examples">
            <button onclick="sendExample(this)">本地有哪些模型可用？</button>
            <button onclick="sendExample(this)">查看磁盘使用情况</button>
          </div>
        </div>
      </div>
    </div>`;
}}

// --- Keyboard shortcuts ---
chatInput.addEventListener("keydown", (e) => {{
  // Enter to send
  if (e.key === "Enter" && !e.shiftKey && !sendBtn.disabled) {{
    e.preventDefault();
    sendMessage();
    return;
  }}
  // Up arrow: previous history
  if (e.key === "ArrowUp" && chatInput.value === "") {{
    e.preventDefault();
    if (historyIdx > 0) {{
      historyIdx--;
      chatInput.value = inputHistory[historyIdx] || "";
    }}
    return;
  }}
  // Down arrow: next history
  if (e.key === "ArrowDown" && inputHistory.length > 0) {{
    e.preventDefault();
    if (historyIdx < inputHistory.length - 1) {{
      historyIdx++;
      chatInput.value = inputHistory[historyIdx] || "";
    }} else {{
      historyIdx = inputHistory.length;
      chatInput.value = "";
    }}
    return;
  }}
  // Escape: cancel or unfocus
  if (e.key === "Escape") {{
    if (stopBtn.classList.contains("visible")) {{
      cancelTask();
    }} else {{
      chatInput.blur();
    }}
    return;
  }}
}});

// Global shortcuts
document.addEventListener("keydown", (e) => {{
  // Ctrl+K: focus input
  if ((e.ctrlKey || e.metaKey) && e.key === "k") {{
    e.preventDefault();
    chatInput.focus();
    return;
  }}
  // Ctrl+Shift+L: clear chat
  if ((e.ctrlKey || e.metaKey) && e.shiftKey && e.key === "L") {{
    e.preventDefault();
    resetChat();
    return;
  }}
  // Ctrl+Shift+T: toggle theme
  if ((e.ctrlKey || e.metaKey) && e.shiftKey && e.key === "T") {{
    e.preventDefault();
    toggleTheme();
    return;
  }}
  // ? key (not in input): toggle shortcuts
  if (e.key === "?" && document.activeElement !== chatInput) {{
    e.preventDefault();
    toggleShortcuts();
    return;
  }}
  // Escape to close shortcut overlay
  if (e.key === "Escape") {{
    const overlay = document.getElementById("shortcut-overlay");
    if (overlay.classList.contains("visible")) {{
      toggleShortcuts();
    }}
  }}
}});

// Sidebar section keyboard support
document.querySelectorAll(".ref-panel h3").forEach(h3 => {{
  h3.addEventListener("keydown", (e) => {{
    if (e.key === "Enter" || e.key === " ") {{
      e.preventDefault();
      toggleSection(h3);
    }}
  }});
}});

// --- Boot: populate status & models ---
(async () => {{
  try {{
    const r = await fetch("/tool/get_system_info", {{method:"POST", headers:{{"Content-Type":"application/json"}}, body:"{{}}"}});
    if (!r.ok) throw new Error("HTTP " + r.status);
    const info = await r.json();
    if (info.ok) {{
      document.getElementById("sys-os").textContent = info.result.platform || info.result.system;
      document.getElementById("sys-workspace").textContent = info.result.workspace_root;
    }}
  }} catch (_) {{}}

  try {{
    const r = await fetch("/api/chat/models");
    if (!r.ok) throw new Error("HTTP " + r.status);
    const data = await r.json();
    const sel = document.getElementById("model-select");
    const modelListEl = document.getElementById("model-list");
    const statusEl = document.getElementById("ollama-status");
    const statusBar = document.getElementById("status-bar");

    if (data.models && data.models.length > 0) {{
      sel.innerHTML = data.models.map(m =>
        '<option value="' + m + '"' + (m === data.default ? ' selected' : '') + '>' + m + '</option>'
      ).join("");
      statusEl.innerHTML = '<span class="status-dot ok" aria-hidden="true"></span> ' + data.models.length + ' 模型';
      statusBar.innerHTML = '<span class="status-dot ok" aria-hidden="true"></span><span>已连接</span>';
      modelListEl.innerHTML = data.models.map(m => '<span class="model-tag">' + m + '</span>').join("");
    }} else {{
      sel.innerHTML = '<option value="">无可用模型</option>';
      statusEl.innerHTML = '<span class="status-dot err" aria-hidden="true"></span> 未连接';
      statusBar.innerHTML = '<span class="status-dot err" aria-hidden="true"></span><span>Ollama 未连接</span>';
      modelListEl.innerHTML = '<span class="model-tag" style="color:var(--muted)">无</span>';
    }}
  }} catch (_) {{
    document.getElementById("status-bar").innerHTML = '<span class="status-dot err" aria-hidden="true"></span><span>Ollama 未连接</span>';
  }}

  chatInput.focus();
}})();
</script>
</body>
</html>"""
