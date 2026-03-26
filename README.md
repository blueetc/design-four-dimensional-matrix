# CloudWBot — 本地 AI 驱动的任务执行与审查运行时

CloudWBot 是一个本地 **Web 应用**，集成 Ollama 本地大语言模型，在浏览器中运行，支持：

- 💬 **智能对话**：`/chat <内容>` 进入普通问答模式
- 🤖 **任务执行**：直接输入描述或 `/task <描述>` 创建任务，先本地生成稳妥方案，提供 5 秒确认/取消窗口（无操作默认执行）
- ☕ **Java 自动编译运行**：AI 生成 Java 代码后，服务端自动调用 `javac` 编译、`java` 运行
- 📋 **执行轨迹**：实时查看每步操作日志
- 🧾 **完成摘要**：任务结束后自动汇总产物、命令和关键结果
- 🗂 **历史任务恢复**：刷新页面或重启服务后，仍可恢复已完成任务；异常中断的任务会自动标记为“已中断”
- 👁️ **产物文件预览**：点击任务产物文件列表即可在右侧直接预览内容
- ♻️ **中断任务重跑**：选中“已中断”任务后，可直接一键重新执行；新任务会保留来源任务标记
- ⬇️ **产物本地操作**：任务产物支持在浏览器中打开完整内容、直接下载、复制绝对路径，以及在 Finder 中定位
- 🔁 **任务追问**：选中已完成任务后，可继续基于生成的项目和执行结果提问
- 🔍 **GitHub 搜索**：`/github search <关键词>`
- 💡 **经验提示**：`/hints [查询词]`

---

## 🚀 快速启动

### 第一步：克隆代码

```bash
git clone -b copilot/check-task-execution-issues \
  https://github.com/blueetc/ollama-python.git
cd ollama-python
```

> 不想用 git？可以在 [Actions 页面](../../actions/workflows/build.yml) 下载最新 CI 构建的 **cloudwbot-source** Artifact，解压即得完整源码。

### 第二步：安装依赖

```bash
npm install
```

### 第三步：启动服务

```bash
npm start
```

然后在浏览器中打开 **[http://localhost:3000](http://localhost:3000)**。

---

## 🔧 前置要求

| 依赖 | 作用 | 安装方式 |
| --- | --- | --- |
| [Node.js](https://nodejs.org) ≥ 18 | 运行本地 Web 服务 | 官网下载 |
| [Ollama](https://ollama.com) | 本地 AI 推理服务 | 官网下载后运行 `ollama serve` |
| 已安装的模型 | 实际回答问题的模型 | `ollama pull qwen2.5:7b`（任意模型均可） |
| JDK（可选） | Java 任务自动编译运行 | 系统包管理器安装 |

> **提示**：启动后页面右上角会显示 Ollama 连接状态。若显示红色 `● Ollama 未运行`，请先执行 `ollama serve`。

---

## ⚙️ 配置（环境变量）

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `OLLAMA_MODEL` | _(自动)_ | 指定使用的 Ollama 模型。留空时自动使用第一个已安装的模型 |
| `OLLAMA_URL` | `http://localhost:11434` | Ollama 服务地址 |
| `CLOUDWBOT_WORKSPACE` | `.cloudwbot_workspace` | 任务工作目录根路径。未设置时优先使用自动识别的项目根目录下的 `.cloudwbot_workspace`，若项目目录不可写则回退到 `~/cloudwbot_workspace`；设置后可使用绝对路径或相对项目根目录的路径 |
| `PORT` | `3000` | 本地 Web 服务端口 |

你也可以在项目根目录创建 `.env` 文件：

```bash
OLLAMA_MODEL=qwen2.5:7b
OLLAMA_URL=http://localhost:11434
PORT=3000
CLOUDWBOT_WORKSPACE=./workspace
```

示例：

```bash
OLLAMA_MODEL=qwen2.5:7b CLOUDWBOT_WORKSPACE=./tmp/cloudwbot PORT=8080 npm start
```

任务产物默认位于自动识别的项目根目录下的 `.cloudwbot_workspace/tasks/<task-id>/`；若项目目录不可写，则自动回退到 `~/cloudwbot_workspace/tasks/<task-id>/`。

---

## 🛠 开发（VS Code）

用 VS Code 打开项目目录，按 **F5** 即可启动服务器：

- VS Code 会在集成终端中运行 `node apps/server.js`
- 终端输出 `CloudWBot running → http://localhost:3000` 后，打开浏览器访问该地址
- 修改代码后重启服务（`Ctrl+C` 后重新按 F5），或使用 `npm run dev` 开启文件监视自动重启

---

## ✅ 质量与压力测试

```bash
# 代码质量与单元测试
npm run lint
npm test

# 压力测试（任务数量限制 / 超时控制 / SSE 重连稳定性）
npm run test:stress
```

`test:stress` 会在临时工作目录中执行，不会污染你当前任务目录，完成后自动清理。

发版说明与回滚步骤请参考：`RELEASE_NOTES_0.3.0.md`。

---

## 目录结构

```text
.github/
  workflows/
    build.yml    # CI：每次推送自动做语法检查并上传源码为 Artifact
apps/
  server.js      # 主入口：Node.js HTTP 服务器（API + 静态文件）
  stress/
    stress.js    # 压力测试脚本（任务上限 / 超时 / SSE）
  lib/           # 服务端模块
    config.js    # 配置管理
    ollama.js    # Ollama 客户端
    taskManager.js   # 任务管理
    executor.js  # 任务执行引擎
    utils.js     # 工具函数
  webview/       # 前端 UI（浏览器直接渲染）
    index.html   # 应用 HTML 骨架
    main.js      # 聊天/任务管理逻辑（fetch + SSE 与服务端通信）
    style.css    # 样式
cloudwbot/       # 运行时数据目录
  hints.json     # 经验提示数据
package.json     # 项目清单（scripts: start / dev / lint / test）
```

---

## 已修复问题

### 1. 欢迎消息 HTML 双重编码

**现象**：欢迎消息中 `/chat` 等命令显示为 `&lt;内容&gt;` 字面量而非 `<内容>`。

**根因**：模板字符串中使用了 HTML 实体（`&lt;` / `&gt;`），再经过 `innerHTML` 插入时被二次转义。

**修复**：`WELCOME_TEXT` 改用 Unicode 原始字符 `<` / `>`，通过统一的 `escHtml()` 函数转义后插入 DOM，确保只做一次转义，正确渲染为 `<内容>`。

### 2. 任务执行未真正运行代码

**现象**：任务状态显示 `completed`，但结果只展示 Python 伪代码（`file_write(...)`），文件从未写入，Java 程序从未编译或运行，执行轨迹为空。

**根因**：执行器将 AI 响应中的伪代码直接作为"结果"展示，缺少解析与执行步骤。

**修复**：`executeAiResponse()`（`apps/server.js`）和 `execute_ai_response()`（`cloudwbot/executor.py`）实现完整解析–执行流水线：

1. 解析 `FILE: <path>` + 代码块 → 实际写入文件
2. 解析无标记的独立代码块 → 按语言推断文件名后写入
3. 解析旧式 `file_write(path, content)` 伪代码 → 实际写入文件
4. `.java` 文件写入后自动调用 `javac` 编译，成功后调用 `java` 运行
5. 解析 `RUN: <command>` 行 → 在工作目录中执行 shell 命令

### 3. 项目架构：从 VS Code 扩展改为独立 Web 应用

**现象**：在 VS Code 中打开项目并按 F5，VS Code 总是打开第二个窗口（Extension Development Host），用户无法直接在浏览器中使用。

**根因**：项目最初被构建为 VS Code 扩展（`.vsix`），依赖 `vscode` API（`acquireVsCodeApi`、`WebviewPanel` 等），只能在 VS Code 扩展宿主中运行。

**修复**：将项目重构为标准 Node.js Web 应用：

- 新增 `apps/server.js`：Node.js HTTP 服务器，通过 **Server-Sent Events（SSE）** 推送消息到浏览器，通过 `POST /api/message` 接收前端命令
- 新增 `apps/webview/index.html`：独立 HTML 页面，直接由服务器提供
- 重写 `apps/webview/main.js` 的通信层：将 `acquireVsCodeApi()` 替换为 `fetch` + `EventSource` 的透明 shim，其余业务逻辑不变
- 更新 `package.json`：移除所有 VS Code 扩展字段（`engines.vscode`、`activationEvents`、`contributes`、`main`），添加 `start`/`dev` 脚本
- 更新 `.vscode/launch.json`：F5 改为启动 Node.js 服务器（`type: node`），不再打开第二个 VS Code 窗口
- 移除 `apps/extension.js` 和 `.vscodeignore`（扩展专有文件）

