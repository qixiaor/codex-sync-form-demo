# Codex CLI Task Orchestrator

这个程序会把任务放在一个在线 HTTP URL 后面，然后自动拉起多个全新的 `codex exec` 进程去执行任务。每个任务都运行在新的 Codex CLI 会话里，因此不会继承上一个任务的上下文。

## 任务表

表格前三列固定语义如下：

| A列 | B列 | C列 |
| --- | --- | --- |
| 标题 | 任务详情 | 状态 |

状态只允许：

- `未开始`
- `执行中`
- `已完成`

服务启动后，可以通过：

- `GET /` 查看网页表格
- `GET /table.tsv` 查看 TSV 形式的 A-C 三列
- `GET /api/tasks` 查看完整 JSON

## 并发设计

并发问题按两层处理：

1. 抢任务冲突
   服务端用 SQLite `BEGIN IMMEDIATE` 事务原子地完成“挑一条 `未开始` 任务并更新成 `执行中`”。多个 worker 同时请求时，只会有一个 worker 抢到某一行。
2. 多个 Codex CLI 写同一目录的冲突
   每个任务都会先复制一个独立工作目录，然后在这个目录里执行新的 `codex exec`。这样不同任务不会在同一份代码上互相覆盖。

另外还有租约机制：

- worker 抢到任务后会带一个 `lease_seconds`
- 运行期间持续 heartbeat
- 如果 worker 崩溃，租约到期后任务会被自动回收到 `未开始`

## 快速开始

### 1. 启动服务

```powershell
python -m codex_orchestrator serve --host 127.0.0.1 --port 8000 --db .codex-runtime/tasks.db
```

### 2. 新增任务

```powershell
python -m codex_orchestrator add --server-url http://127.0.0.1:8000 --title "修复登录页" --detail "检查项目并修复登录按钮点击无响应的问题"
python -m codex_orchestrator add --server-url http://127.0.0.1:8000 --title "补测试" --detail "为订单金额计算补充边界测试"
```

### 3. 启动 worker 池

```powershell
python -m codex_orchestrator pool `
  --server-url http://127.0.0.1:8000 `
  --workers 3 `
  --template-dir . `
  --runtime-dir .codex-runtime `
  --codex-timeout-seconds 900
```

这会启动 3 个独立 worker。每个 worker 会循环：

1. 从在线 URL 抢一条 `未开始` 任务
2. 复制工作目录
3. 启动一个全新的 `codex exec`
4. 成功则把状态更新为 `已完成`
5. 失败则释放回 `未开始`

如果是 Windows，程序现在会自动把 `codex` 解析到 `codex.cmd` / `codex.exe`。如果你的环境变量比较特殊，也可以手动指定：

```powershell
python -m codex_orchestrator pool `
  --server-url http://127.0.0.1:8000 `
  --workers 3 `
  --template-dir . `
  --runtime-dir .codex-runtime `
  --codex-bin codex.cmd
```

## 目录说明

- `.codex-runtime/tasks.db`: 任务数据库
- `.codex-runtime/worker-*/task-*/workspace`: 每个任务的独立工作区
- `.codex-runtime/worker-*/task-*/logs`: `codex` 标准输出、错误输出和最终摘要

## 运行测试

```powershell
python -m unittest discover -s tests -v
```

## 说明

- 默认实现把“在线 URL”定义为这个程序自带的 HTTP 任务服务。
- 如果你后面要接入现有的在线表格系统，比如 Google Sheets、飞书多维表格或自建 API，可以保留 worker/pool，不动并发模型，只替换 `TaskClient` 和服务端适配层。
- 当前实现不会自动把多个并发任务的代码改动合并回同一份目录，因为这件事没有可靠的无冲突通用方案。程序会把每个任务结果保存在独立工作区里，供你后续审查或手工合并。
