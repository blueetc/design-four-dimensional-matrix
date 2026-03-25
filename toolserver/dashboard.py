"""Self-contained HTML dashboard for the tool server.

The page is returned as a single string so we do not need any static-file
serving infrastructure.  JavaScript on the page calls the existing JSON
API endpoints to populate live data (system info, available models, etc.).
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
<title>Ollama Local Agent – Tool Server</title>
<style>
  :root {{
    --bg: #0f1117; --card: #1a1d27; --border: #2d3140;
    --text: #e0e0e0; --muted: #8b8fa3; --accent: #6c8cff;
    --green: #2ecc71; --red: #e74c3c; --yellow: #f1c40f;
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    background: var(--bg); color: var(--text); line-height: 1.6;
    padding: 0 1rem;
  }}
  .container {{ max-width: 960px; margin: 0 auto; padding: 2rem 0; }}
  h1 {{ font-size: 1.8rem; margin-bottom: 0.25rem; }}
  h1 span {{ color: var(--accent); }}
  .subtitle {{ color: var(--muted); margin-bottom: 2rem; font-size: 0.95rem; }}
  .cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 1rem; margin-bottom: 2rem; }}
  .card {{
    background: var(--card); border: 1px solid var(--border);
    border-radius: 10px; padding: 1.2rem;
  }}
  .card h3 {{ font-size: 0.85rem; color: var(--muted); text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 0.5rem; }}
  .card .value {{ font-size: 1.4rem; font-weight: 600; }}
  .status-dot {{
    display: inline-block; width: 10px; height: 10px;
    border-radius: 50%; margin-right: 6px; vertical-align: middle;
  }}
  .status-dot.ok {{ background: var(--green); }}
  .status-dot.err {{ background: var(--red); }}
  .status-dot.loading {{ background: var(--yellow); animation: pulse 1s infinite; }}
  @keyframes pulse {{ 0%,100%{{opacity:1}} 50%{{opacity:0.4}} }}

  section {{ margin-bottom: 2rem; }}
  section h2 {{ font-size: 1.2rem; margin-bottom: 0.75rem; border-bottom: 1px solid var(--border); padding-bottom: 0.4rem; }}

  table {{ width: 100%; border-collapse: collapse; font-size: 0.9rem; }}
  th, td {{ text-align: left; padding: 0.55rem 0.75rem; border-bottom: 1px solid var(--border); }}
  th {{ color: var(--muted); font-weight: 500; font-size: 0.8rem; text-transform: uppercase; }}
  .tool-name {{ font-weight: 600; white-space: nowrap; }}
  .tool-endpoint code {{ color: var(--accent); font-size: 0.85rem; }}

  /* Try-it panel */
  .try-panel {{
    background: var(--card); border: 1px solid var(--border);
    border-radius: 10px; padding: 1.2rem;
  }}
  .try-panel select, .try-panel textarea, .try-panel button {{
    font-family: inherit; font-size: 0.9rem;
    background: var(--bg); color: var(--text); border: 1px solid var(--border);
    border-radius: 6px; padding: 0.5rem 0.75rem; width: 100%;
  }}
  .try-panel select {{ margin-bottom: 0.75rem; cursor: pointer; }}
  .try-panel textarea {{ min-height: 80px; resize: vertical; margin-bottom: 0.75rem; }}
  .try-panel button {{
    background: var(--accent); color: #fff; border: none;
    cursor: pointer; font-weight: 600; padding: 0.6rem;
    transition: opacity 0.15s;
  }}
  .try-panel button:hover {{ opacity: 0.85; }}
  .try-panel button:disabled {{ opacity: 0.4; cursor: not-allowed; }}
  pre.result {{
    background: var(--bg); border: 1px solid var(--border);
    border-radius: 6px; padding: 0.75rem; margin-top: 0.75rem;
    max-height: 320px; overflow: auto; font-size: 0.82rem;
    white-space: pre-wrap; word-break: break-all;
  }}

  .model-list {{ display: flex; flex-wrap: wrap; gap: 0.5rem; }}
  .model-tag {{
    background: var(--card); border: 1px solid var(--border);
    border-radius: 6px; padding: 0.3rem 0.7rem; font-size: 0.85rem;
  }}

  .links {{ display: flex; gap: 1rem; margin-top: 0.5rem; }}
  .links a {{
    color: var(--accent); text-decoration: none; font-size: 0.9rem;
  }}
  .links a:hover {{ text-decoration: underline; }}

  footer {{ text-align: center; color: var(--muted); font-size: 0.8rem; padding: 1rem 0; border-top: 1px solid var(--border); }}
</style>
</head>
<body>
<div class="container">

  <h1>🤖 <span>Ollama Local Agent</span> – Tool Server</h1>
  <p class="subtitle">本地自动化代理的策略化工具服务 &nbsp;|&nbsp; 所有操作经安全策略审计</p>
  <div class="links">
    <a href="/docs">📖 Swagger UI (API 文档)</a>
    <a href="/openapi.json">📄 OpenAPI Schema</a>
  </div>

  <!-- Status cards -->
  <section style="margin-top:1.5rem">
    <h2>服务状态</h2>
    <div class="cards">
      <div class="card">
        <h3>Tool Server</h3>
        <div class="value"><span class="status-dot ok"></span>运行中</div>
      </div>
      <div class="card">
        <h3>Ollama</h3>
        <div class="value" id="ollama-status"><span class="status-dot loading"></span>检测中…</div>
      </div>
      <div class="card">
        <h3>操作系统</h3>
        <div class="value" id="sys-os">—</div>
      </div>
      <div class="card">
        <h3>工作空间</h3>
        <div class="value" id="sys-workspace" style="font-size:0.95rem;word-break:break-all">—</div>
      </div>
    </div>
  </section>

  <!-- Models -->
  <section>
    <h2>可用模型</h2>
    <div class="model-list" id="model-list">
      <span class="model-tag" style="color:var(--muted)">加载中…</span>
    </div>
  </section>

  <!-- Tool table -->
  <section>
    <h2>可用工具 ({len(TOOLS)})</h2>
    <div style="overflow-x:auto">
    <table>
      <thead><tr><th>工具名</th><th>端点</th><th>说明</th></tr></thead>
      <tbody>
        {tool_rows}
      </tbody>
    </table>
    </div>
  </section>

  <!-- Try-it -->
  <section>
    <h2>🔧 在线试用</h2>
    <div class="try-panel">
      <select id="tool-select">
        <option value="/tool/get_system_info">get_system_info</option>
        <option value="/tool/list_models">list_models</option>
        <option value="/tool/db_schema">db_schema</option>
        <option value="/tool/analyze_fields">analyze_fields</option>
        <option value="/tool/list_dir">list_dir</option>
        <option value="/tool/read_file">read_file</option>
        <option value="/tool/run_command">run_command</option>
      </select>
      <textarea id="tool-body" placeholder='请求体 JSON（如无参数留空 {{}}）'>{{}}</textarea>
      <button id="tool-run" onclick="runTool()">▶ 发送请求</button>
      <pre class="result" id="tool-result">结果将显示在此处</pre>
    </div>
  </section>

  <footer>Ollama Local Agent – Tool Server &copy; MIT License</footer>
</div>

<script>
async function post(url, body) {{
  const r = await fetch(url, {{
    method: "POST",
    headers: {{"Content-Type": "application/json"}},
    body: JSON.stringify(body),
  }});
  return r.json();
}}

// Populate status cards on load
(async () => {{
  try {{
    const info = await post("/tool/get_system_info", {{}});
    if (info.ok) {{
      document.getElementById("sys-os").textContent = info.result.platform || info.result.system;
      document.getElementById("sys-workspace").textContent = info.result.workspace_root;
    }}
  }} catch (_) {{}}

  try {{
    const mRes = await post("/tool/list_models", {{}});
    const el = document.getElementById("model-list");
    const oEl = document.getElementById("ollama-status");
    if (mRes.ok && mRes.result.models.length > 0) {{
      oEl.innerHTML = '<span class="status-dot ok"></span>' + mRes.result.models.length + ' 模型可用';
      el.innerHTML = mRes.result.models
        .map(m => '<span class="model-tag">' + m.name + '</span>')
        .join("");
    }} else {{
      oEl.innerHTML = '<span class="status-dot err"></span>未连接';
      el.innerHTML = '<span class="model-tag" style="color:var(--muted)">未检测到模型</span>';
    }}
  }} catch (_) {{
    document.getElementById("ollama-status").innerHTML = '<span class="status-dot err"></span>未连接';
    document.getElementById("model-list").innerHTML = '<span class="model-tag" style="color:var(--muted)">无法连接 Ollama</span>';
  }}
}})();

async function runTool() {{
  const btn = document.getElementById("tool-run");
  const pre = document.getElementById("tool-result");
  const url = document.getElementById("tool-select").value;
  let body;
  try {{
    body = JSON.parse(document.getElementById("tool-body").value || "{{}}");
  }} catch (e) {{
    pre.textContent = "⚠️  JSON 解析失败: " + e.message;
    return;
  }}
  btn.disabled = true;
  pre.textContent = "请求中…";
  try {{
    const res = await post(url, body);
    pre.textContent = JSON.stringify(res, null, 2);
  }} catch (e) {{
    pre.textContent = "⚠️  请求失败: " + e.message;
  }} finally {{
    btn.disabled = false;
  }}
}}
</script>
</body>
</html>"""
