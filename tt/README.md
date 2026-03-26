# TT Deep Analysis Runner

该目录用于发起并全程跟踪 CloudWBot 的深度项目分析任务。

## 用法

1. 确保服务已启动（默认地址 `http://127.0.0.1:3000`）：

   - `npm start`

2. 运行跟踪脚本：

   - `npm run tt:analyze`

   或者：

   - `node tt/run_project_deep_analysis.js`
   - `node tt/run_project_deep_analysis.js /your/project/path`

## 可选环境变量

- `CLOUDWBOT_SERVER`：CloudWBot 服务地址（默认 `http://127.0.0.1:3000`）
- `CLOUDWBOT_TASK_TIMEOUT_MS`：任务等待超时（默认 1800000，即 30 分钟）

## 输出

- 跟踪日志：`tt/logs/task-<taskId>.log`
- 最近一次执行结果：`tt/last_deep_analysis_result.json`

脚本会自动：

1. 创建任务
2. 监听执行事件
3. 在方案确认阶段自动确认执行
4. 持续记录 trace/console/status
5. 任务结束后输出快照路径、命令和产物信息
