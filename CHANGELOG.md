# Changelog

All notable changes to CloudWBot are documented here.

## [0.3.0] - 2026-03-19

### Added
- **模块化架构**：将 `server.js` 拆分为多个模块
  - `apps/lib/config.js` - 配置管理（支持 `.env` 文件）
  - `apps/lib/ollama.js` - Ollama 客户端
  - `apps/lib/utils.js` - 工具函数
  - `apps/lib/executor.js` - 任务执行引擎
  - `apps/lib/taskManager.js` - 任务管理
- **CI 严格质量门禁**：新增 Node 18/20 矩阵校验，执行 `npm ci -> npm run lint -> npm test`
- **压力测试脚本**：新增 `npm run test:stress`，覆盖任务上限、超时控制与 SSE 重连稳定性
- **SSE 自动重连**：前端 EventSource 断开连接后自动重连（指数退避策略）
- **任务超时控制**：
  - Shell 命令默认 60 秒超时
  - Java 编译和运行默认 30 秒超时
- **任务数量限制**：
  - 内存中最多保留 100 个任务
  - 自动清理 30 天前的任务文件
- **主题切换**：支持深色/浅色主题（`/theme` 命令）
- **代码语法高亮**：产物文件预览支持语法高亮
- **测试覆盖**：添加基础测试框架（Node.js 内置 test runner）
- **响应式布局**：适配移动端和窄屏设备

### Changed
- 任务管理功能现在限制历史任务数量，防止内存和磁盘耗尽
- 优化了文件预览的加载性能

### Removed
- `cloudwbot/executor.py` - Python 执行器（功能已合并到 Node.js 模块）
- `cloudwbot/__init__.py`

### Fixed
- 修复了长时间运行的命令可能阻塞系统的问题（添加超时）
- 修复了 SSE 连接断开后不会自动恢复的问题
- 修复了任务无限增长导致内存泄漏的问题

## [0.2.0] - 2026-03-18

### Changed
- **架构重构**：从 VS Code 扩展改为独立 Node.js Web 应用，在浏览器中运行，不再需要 VS Code 插件宿主
  - 新增 `apps/server.js`：Node.js HTTP 服务器，通过 SSE 向浏览器推送消息
  - 新增 `apps/webview/index.html`：独立 HTML 页面
  - 重写 `apps/webview/main.js` 通信层：`acquireVsCodeApi()` → `fetch` + `EventSource`
  - 按 F5 现在启动本地 Web 服务器，在浏览器打开 http://localhost:3000 即可使用
  - 配置方式改为环境变量（`OLLAMA_URL`、`OLLAMA_MODEL`、`PORT`）

### Removed
- `apps/extension.js`（VS Code 扩展入口，已被 `apps/server.js` 取代）
- `.vscodeignore`（VS Code 扩展打包配置）
- `package.json` 中的 VS Code 扩展字段（`engines.vscode`、`activationEvents`、`contributes`）

## [0.1.0] - 2026-03-18

### Added
- Initial release as a VS Code extension
- Intelligent chat panel with Ollama integration
- Task execution engine: parses AI-generated code and runs it locally
  - Supports `FILE: <path>` + fenced code block format
  - Supports standalone fenced code blocks (auto-infers filename)
  - Supports legacy `file_write(path, variable)` pseudocode
  - Automatic Java compile (`javac`) + run (`java`) pipeline
  - `RUN: <command>` shell execution support
- Real-time console output panel
- Execution trace panel
- `/chat`, `/task`, `/status`, `/models`, `/hints`, `/github search`, `/plan`, `/clear` commands

### Fixed
- Welcome message no longer shows `&lt;内容&gt;` — angle brackets render correctly
- Task execution now actually writes files and compiles/runs Java programs
- `/chat` errors (Ollama not running, model not found) now show a clear error message
