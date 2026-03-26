# Release Notes - 0.3.0

发布日期：2026-03-19  
发布类型：稳定功能发布（本地运行时能力增强 + 工程质量增强）

## 1. 发布摘要

CloudWBot 0.3.0 聚焦三件事：

- 提升可维护性：服务端重构为模块化架构，职责更清晰。
- 提升稳定性：任务超时控制、任务数量限制、SSE 自动重连覆盖关键风险点。
- 提升工程可验证性：CI 严格质量门禁 + 压力测试脚本，确保改动可重复验证。

## 2. 主要变更

### 2.1 运行时与产品能力

- 模块化服务端结构：`config / ollama / executor / taskManager / utils`
- 任务执行超时保护：
  - Shell 命令默认 60 秒超时
  - Java 编译/运行默认 30 秒超时
- 任务数量限制：
  - 内存最多保留 100 个任务
  - 自动清理 30 天前历史任务
- SSE 自动重连：指数退避重试，提升断线恢复能力
- 前端体验优化：主题切换 + 产物语法高亮

### 2.2 工程质量与验证能力

- CI 工作流升级为严格质量门禁：
  - 触发方式：`push` / `pull_request` / `workflow_dispatch`
  - 运行环境：Node 18 / 20 矩阵
  - 执行链路：`npm ci -> npm run lint -> npm test`
  - 并发策略：同分支新提交自动取消旧任务
- 新增压力测试：`npm run test:stress`
  - 任务上限压力检查
  - 超时控制压力检查
  - SSE 重连与多客户端广播检查

## 3. 默认配置基线

### 3.1 环境变量默认值

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `PORT` | `3000` | 本地服务监听端口 |
| `OLLAMA_URL` | `http://localhost:11434` | Ollama 服务地址 |
| `OLLAMA_MODEL` | 空字符串（自动选择） | 未设置时自动选择本地已安装模型 |
| `CLOUDWBOT_WORKSPACE` | 未显式设置 | 优先项目根目录 `.cloudwbot_workspace`，不可写则回退到 `~/cloudwbot_workspace` |

### 3.2 运行保护默认值

| 项目 | 默认值 | 说明 |
| --- | --- | --- |
| `TASK_MAX_COUNT` | `100` | 内存中任务保留上限 |
| `TASK_MAX_AGE_DAYS` | `30` | 历史任务自动清理阈值 |
| `DEFAULT_SHELL_TIMEOUT` | `60000 ms` | Shell 命令默认超时 |
| `DEFAULT_JAVA_TIMEOUT` | `30000 ms` | Java 编译/运行默认超时 |
| `MAX_RECONNECT_ATTEMPTS` | `10` | SSE 最大重连次数 |
| `BASE_RECONNECT_DELAY` | `1000 ms` | SSE 基础重连间隔（指数退避，上限 30 秒） |

## 4. 验收结果

本地验收命令：

```bash
npm run lint && npm test && npm run test:stress
```

验收结论：

- Lint 通过
- 单元测试 21/21 通过
- 压力测试三项全部通过

## 5. 升级步骤

```bash
npm ci
npm run lint
npm test
npm run test:stress
npm start
```

> 说明：`test:stress` 建议在发版前或合并前执行；日常快速验证可先执行 lint + 单测。

## 6. 回滚步骤

### 6.1 整体版本回滚（推荐）

1. 定位上一个稳定版本标签或提交。
2. 切换并安装依赖。
3. 启动服务并验证关键路径。

```bash
git fetch --tags
git checkout <last-stable-tag-or-commit>
npm ci
npm start
```

### 6.2 仅回滚 CI 工作流

当问题仅发生在流水线策略时，可单独回滚 CI 文件：

```bash
git checkout <last-good-commit> -- .github/workflows/build.yml
```

### 6.3 仅回滚压力测试脚本

当问题仅发生在测试脚本时，可单独回滚以下文件：

```bash
git checkout <last-good-commit> -- apps/stress/stress.js package.json README.md
```

### 6.4 运行时数据清理与恢复

如需清理本地产物工作目录（非代码回滚）：

```bash
npm run clean
```

> 注意：该操作会删除本地任务产物目录，请先备份需要保留的任务结果。
