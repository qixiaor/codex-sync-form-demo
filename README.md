# Codex CLI Task Orchestrator

这个程序做 3 件事：

1. 从在线表格同步任务到线上 MySQL
2. 用多个全新的 `codex exec` 会话并发执行任务
3. 把本地任务状态再回写到在线表格

任务表固定读取三列：

| A列  | B列   | C列  |
| --- | ---- | --- |
| 标题  | 任务详情 | 状态  |

状态统一使用：

- `未开始`
- `执行中`
- `已完成`

状态为空（未填写）会被直接忽略，不会导入本地任务。

当前只保留两个同步 provider：

- `google-sheets`
- `dingtalk-base`

## 运行结构

整套程序通常有 3 个长期进程：

1. `serve`
   本地任务服务，负责 MySQL 和本地 HTTP API
2. `sync loop`
   在线表格同步进程，负责导入任务和回写状态
3. `pool`
   worker 池，负责启动多个独立 worker 和新的智能体 CLI 会话

关系是：

`在线表格 <-> sync loop <-> MySQL / 本地任务服务 <-> worker pool <-> agent CLI`

并发控制只在 MySQL 里做。在线表格不负责抢任务。

## 一键启动

**推荐优先使用统一配置文件 + 单命令启动，而不是手工维护 3 条命令**

示例 MySQL URL：

`mysql://root:password@127.0.0.1:3306/agent_tasks?charset=utf8mb4`

使用 MySQL 前需要：

1. `python -m pip install pymysql`
2. 先创建数据库，例如 `agent_tasks`
3. 确认 `--db` 使用的是可连接的 MySQL URL

钉钉表格获取`dentry_uuid`方法：

1. 在钉钉中创建一个ai表格并复制分享链接，如：`https://alidocs.dingtalk.com/i/nodes/1DKw2zgV2PpdzzQ1SBzpZPGL8B5r9YAn?utm_scene=person_space&iframeQuery=viewId%3Dh6yiyob6r755cg7wfwexe%26sheetId%3D1h0j4aeec525eknxao7l7`

2. 其中`dentry_uuid`为`1DKw2zgV2PpdzzQ1SBzpZPGL8B5r9YAn`这一段，复制到dingtalk-base.sync.json中即可

统一启动配置示例：

- [`examples/stack.google-sheets.json`](/f:/work/codexSyncDemo/examples/stack.google-sheets.json)
- [`examples/stack.dingtalk-base.json`](/f:/work/codexSyncDemo/examples/stack.dingtalk-base.json)

最常用的启动方式：

```powershell
python -m codex_orchestrator stack run --config .\examples\stack.dingtalk-base.json
```

## 分步启动

如果你要单独调试某个环节，再使用下面这 3 个底层命令。

### 1. 启动本地任务服务

```powershell
python -m codex_orchestrator serve --host 127.0.0.1 --port 8000 --db "mysql://root:password@127.0.0.1:3306/agent_tasks?charset=utf8mb4"
```

这个进程启动后，可以访问：

- `http://127.0.0.1:8000/`
- `http://127.0.0.1:8000/table.tsv`
- `http://127.0.0.1:8000/api/tasks`

### 2. 启动同步进程

Google Sheets 示例：

```powershell
python -m codex_orchestrator sync loop `
  --db "mysql://root:password@127.0.0.1:3306/agent_tasks?charset=utf8mb4" `
  --config .\examples\google-sheets.sync.json `
  --interval-seconds 15 `
  --proxy-url http://127.0.0.1:7890
```

钉钉多维表格示例：

```powershell
python -m codex_orchestrator sync loop `
  --db "mysql://root:password@127.0.0.1:3306/agent_tasks?charset=utf8mb4" `
  --config .\examples\dingtalk-base.sync.json `
  --interval-seconds 15 `
  --proxy-url http://127.0.0.1:7890
```

如果你只想先单次调试，用 `sync once`：

```powershell
python -m codex_orchestrator sync once `
  --db "mysql://root:password@127.0.0.1:3306/codex_tasks?charset=utf8mb4" `
  --config .\examples\dingtalk-base.sync.json `
  --proxy-url http://127.0.0.1:7890
```

### 3. 启动 worker 池

Windows 建议显式指定 `codex.cmd`：

```powershell
python -m codex_orchestrator pool `
  --server-url http://127.0.0.1:8000 `
  --workers 3 `
  --template-dir . `
  --runtime-dir .codex-runtime `
  --codex-bin codex.cmd `
  --server-timeout-seconds 10 `
  --codex-timeout-seconds 900 `
  --proxy-url http://127.0.0.1:7890
```

### 3.1 worker 工作目录参数（重点）

- `--template-dir`：任务模板目录（源目录）。每个任务开始时，worker 会把这个目录复制到该任务的独立 `workspace`，然后在这个 `workspace` 里执行智能体命令。
- `--runtime-dir`：运行时目录（目标根目录）。每个任务的 `workspace`、`logs`、worker 临时目录都在这里。
- `--results-dir`：任务结果摘要输出目录（默认是 `<runtime-dir>/task-results`）。
- `--workspace-cleanup`：任务结束后的 workspace 清理策略。`after-sync-back`（默认，仅“成功回写主项目”后清理）、`on-success`（只清理成功任务）、`always`（成功/失败都清理）、`never`（不清理）。
- `--workspace-sync-back`：是否把任务 `workspace` 的变更文件增量回写到 `--template-dir`。`on-success`（默认，仅成功任务回写）、`never`（不回写）、`always`（无论成功失败都回写）。

这意味着你可以显式指定“在哪个项目里执行”：

- 想让任务在 `F:\work\my-project` 上下文中执行，就把 `--template-dir` 设为 `F:\work\my-project`。
- 程序不会在原项目目录原地执行，而是复制一份到任务 `workspace` 后再执行，避免多个 worker 并发互相污染。
- 为减少 Claude 误读父目录项目的概率，worker 会在 `workspace` 写入根目录标记，并尝试初始化独立 `.git` 根。
- 如果模板目录很大，建议至少加 `--workspace-cleanup always`，避免磁盘被历史 workspace 快速占满。
- **如果你希望把改动写回主项目**，再加 `--workspace-sync-back on-success`（只同步变更文件，不做整目录对拷）。
- 回写时会加文件锁，避免多个 worker 同时写主项目；若主项目文件在任务执行期间已被外部修改，该文件会标记冲突并跳过。

另外，当前没有对外暴露 `--cwd` 参数；worker 内部会自动把每个任务子进程的 `cwd` 设为该任务 `workspace`。

## `stack.*.json` 参数说明

统一启动配置文件分 4 段：

```json
{
  "database_url": "mysql://root:password@127.0.0.1:3306/codex_tasks?charset=utf8mb4",
  "serve": {
    "host": "127.0.0.1",
    "port": 8000
  },
  "sync": {
    "config": "./dingtalk-base.sync.json",
    "interval_seconds": 15,
    "proxy_url": "http://127.0.0.1:7890"
  },
  "pool": {
    "workers": 3,
    "template_dir": "..",
    "runtime_dir": "../.codex-runtime",
    "agent_type": "command-template",
    "agent_bin": "claude.cmd",
    "agent_timeout_seconds": 900,
    "server_timeout_seconds": 10,
    "workspace_cleanup": "after-sync-back",
    "workspace_sync_back": "on-success",
    "proxy_url": "http://127.0.0.1:7890"
  }
}
```

常用字段：

| 字段                           | 说明                                  |
| ---------------------------- | ----------------------------------- |
| `database_url`               | MySQL URL，`serve` 和 `sync` 共用       |
| `serve.host`                 | 本地 HTTP 服务监听地址                      |
| `serve.port`                 | 本地 HTTP 服务端口                        |
| `sync.config`                | 同步源配置文件路径，支持相对 `stack.json`         |
| `sync.interval_seconds`      | 同步轮询间隔                              |
| `sync.proxy_url`             | 同步进程代理                              |
| `pool.workers`               | worker 数量                           |
| `pool.template_dir`          | 任务模板目录，支持相对 `stack.json`            |
| `pool.runtime_dir`           | worker 运行目录，支持相对 `stack.json`       |
| `pool.agent_type`            | `codex` 或 `command-template`        |
| `pool.agent_bin`             | 智能体 CLI，例如 `codex.cmd`、`claude.cmd` |
| `pool.agent_timeout_seconds` | 单任务超时                               |
| `pool.workspace_cleanup`     | workspace 清理策略                      |
| `pool.workspace_sync_back`   | 成功后是否回写主项目                          |
| `pool.proxy_url`             | worker / agent 代理                   |

相对路径都会按 `stack.json` 所在目录解析，不再要求你在固定 cwd 下启动。

### 4. 验证状态流转

正常情况下：

1. 在线表格中的 `未开始` 任务被同步到本地
2. worker 抢任务后，本地状态变成 `执行中`
3. `codex exec` 执行任务
4. 成功后状态变成 `已完成`
5. `sync loop` 把状态回写到在线表格

## `google-sheets.sync.json` 参数说明

示例文件：[`examples/google-sheets.sync.json`](/f:/work/codexSyncDemo/examples/google-sheets.sync.json)

```json
{
  "provider": "google-sheets",
  "name": "google-sheet-demo",
  "spreadsheet_url": "https://docs.google.com/spreadsheets/d/YOUR_SPREADSHEET_ID/edit?gid=0#gid=0",
  "sheet_name": "Sheet1",
  "header_row": 1,
  "read_range": "'Sheet1'!A:C",
  "status_column": "C",
  "service_account_file": "C:/path/to/service-account.json",
  "status_aliases": {
    "未完成": "未开始"
  },
  "timeout_seconds": 30
}
```

参数含义：

| 参数                     | 是否必填 | 说明                                         |
| ---------------------- | ---- | ------------------------------------------ |
| `provider`             | 是    | 固定写 `google-sheets`                        |
| `name`                 | 是    | 这个同步源的名字，会写到本地数据库 `source_name`            |
| `spreadsheet_url`      | 是    | Google Sheet 共享链接，程序会自动提取 `spreadsheet_id` |
| `sheet_name`           | 是    | 底部标签页名字，例如 `Sheet1`                        |
| `header_row`           | 否    | 表头所在行，默认 `1`                               |
| `read_range`           | 否    | 读取范围，默认 `'<sheet>'!A:C`                    |
| `status_column`        | 否    | 状态列，默认 `C`                                 |
| `service_account_file` | 是    | Google service account JSON 文件路径           |
| `status_aliases`       | 否    | 状态文案映射，例如把 `未完成` 归一化成 `未开始`                |
| `timeout_seconds`      | 否    | 单次 HTTP 超时时间，默认 `30`                       |

使用前需要：

1. `python -m pip install google-auth`
2. 在 Google Cloud 创建 service account
3. 下载 JSON 密钥文件
4. 把 service account 邮箱加入表格共享成员

## `dingtalk-base.sync.json` 参数说明

示例文件：[`examples/dingtalk-base.sync.json`](/f:/work/codexSyncDemo/examples/dingtalk-base.sync.json)

```json
{
  "provider": "dingtalk-base",
  "name": "dingtalk-base-demo",
  "mcp_url": "https://mcp.api-inference.modelscope.net/4ffd90bb56e447/mcp",
  "dentry_uuid": "YOUR_DENTRY_UUID",
  "sheet_id_or_name": "Sheet1",
  "title_field": "标题",
  "detail_field": "任务详情",
  "status_field": "状态",
  "status_aliases": {
    "未完成": "未开始"
  },
  "write_enabled": true,
  "timeout_seconds": 30
}
```

参数含义：

| 参数                 | 是否必填 | 说明                              |
| ------------------ | ---- | ------------------------------- |
| `provider`         | 是    | 固定写 `dingtalk-base`             |
| `name`             | 是    | 这个同步源的名字，会写到本地数据库 `source_name` |
| `mcp_url`          | 是    | 钉钉 AI 表格 MCP 地址                 |
| `dentry_uuid`      | 是    | 多维表格文档 ID                       |
| `sheet_id_or_name` | 是    | 数据表 ID 或名字，通常是 `Sheet1`         |
| `title_field`      | 否    | 标题字段名，默认 `标题`                   |
| `detail_field`     | 否    | 任务详情字段名，默认 `任务详情`               |
| `status_field`     | 否    | 状态字段名，默认 `状态`                   |
| `status_aliases`   | 否    | 状态文案映射，例如把 `未完成` 归一化成 `未开始`     |
| `write_enabled`    | 否    | 是否回写在线状态；只想验证读取时可设为 `false`     |
| `timeout_seconds`  | 否    | 单次 MCP 调用超时时间，默认 `30`           |

当前 provider 默认使用这些 MCP 工具：

- 读取任务：`search_base_record`
- 回写状态：`update_records`

使用前需要：

1. `python -m pip install mcp`
2. 确认 MCP 地址可访问
3. 确认 `dentry_uuid` 和 `sheet_id_or_name` 正确

## 最常用命令

本地服务：

```powershell
python -m codex_orchestrator serve --host 127.0.0.1 --port 8000 --db "mysql://root:password@127.0.0.1:3306/codex_tasks?charset=utf8mb4"
```

Google Sheets 单次同步：

```powershell
python -m codex_orchestrator sync once `
  --db "mysql://root:password@127.0.0.1:3306/codex_tasks?charset=utf8mb4" `
  --config .\examples\google-sheets.sync.json `
  --proxy-url http://127.0.0.1:7890
```

Google Sheets 持续同步：

```powershell
python -m codex_orchestrator sync loop `
  --db "mysql://root:password@127.0.0.1:3306/codex_tasks?charset=utf8mb4" `
  --config .\examples\google-sheets.sync.json `
  --interval-seconds 15 `
  --proxy-url http://127.0.0.1:7890
```

钉钉多维表格单次同步：

```powershell
python -m codex_orchestrator sync once `
  --db "mysql://root:password@127.0.0.1:3306/codex_tasks?charset=utf8mb4" `
  --config .\examples\dingtalk-base.sync.json `
  --proxy-url http://127.0.0.1:7890
```

钉钉多维表格持续同步：

```powershell
python -m codex_orchestrator sync loop `
  --db "mysql://root:password@127.0.0.1:3306/codex_tasks?charset=utf8mb4" `
  --config .\examples\dingtalk-base.sync.json `
  --interval-seconds 15 `
  --proxy-url http://127.0.0.1:7890
```

完整 worker 池启动：

```powershell
python -m codex_orchestrator pool `
  --server-url http://127.0.0.1:8000 `
  --workers 3 `
  --template-dir . `
  --runtime-dir .codex-runtime `
  --agent-type codex `
  --agent-bin codex.cmd `
  --codex-bin codex.cmd `
  --server-timeout-seconds 10 `
  --codex-timeout-seconds 900 `
  --proxy-url http://127.0.0.1:7890
```

如果你不用 Codex CLI，而是别的智能体 CLI，可以改成 `command-template` 模式。

例如 Claude Code 风格命令模板：

```powershell
python -m codex_orchestrator pool `
  --server-url http://127.0.0.1:8000 `
  --workers 3 `
  --template-dir . `
  --runtime-dir .claude-runtime `
  --agent-type command-template `
  --agent-bin claude.cmd `
  --agent-timeout-seconds 900 `
  --server-timeout-seconds 10 `
  --proxy-url http://127.0.0.1:7890
```

如果你不传 `--agent-command-template`，程序也会走默认兜底：

- `claude` / `claude.exe` / `claude.cmd`：自动使用内置 Claude 模板，默认走 stdin 传 prompt（避免 Windows 下多行参数被截断），并附带 `--permission-mode bypassPermissions`、`--setting-sources user`、`--add-dir {workspace_dir}` 提高隔离稳定性
- 其他未知 CLI：默认按 `可执行文件 + prompt 参数` 方式调用
- 如果你显式传了 `--agent-use-stdin`，未知 CLI 会退化成只启动可执行文件本身，由 stdin 提供 prompt

## 同步输出说明

`sync completed` 会打印三个关键数字：

- `imported`: 本轮从在线表格导入到本地的任务数
- `updated`: 本轮成功回写到在线表格的任务数
- `writeback_errors`: 本轮回写失败的任务数

如果 `writeback_errors > 0`，说明读取成功，但有部分状态没写回在线表格。

## 目录说明

- `--db`: MySQL URL，例如 `mysql://root:password@127.0.0.1:3306/codex_tasks?charset=utf8mb4`
- `.codex-runtime/worker-*/task-*/workspace`: 每个任务的独立工作区
- `.codex-runtime/worker-*/task-*/logs`: `codex` 执行日志
- `.codex-runtime/task-results/task-*.json|txt`: 按任务编号输出的结果摘要

## 故障排查

`sync loop` 没导入任务：

- 检查 `sync.json` 里的 `provider` 是否正确
- 检查 Google 的 `sheet_name` 或钉钉的 `sheet_id_or_name` 是否正确
- 检查状态列内容是否能映射到 `未开始 / 执行中 / 已完成`
- 先用 `sync once` 看一次性结果

Google 同步网络问题：

- 如果看到 `oauth2.googleapis.com/token`
- 或 `SSLEOFError`
- 或 TLS 超时

优先给 `sync once` / `sync loop` 加：

```powershell
--proxy-url http://127.0.0.1:7890
```

worker 不执行任务：

- 检查 `serve` 是否还在运行
- 检查 `sync loop` 是否已经把任务导入本地
- 检查 `http://127.0.0.1:8000/api/tasks` 里是否存在 `未开始` 任务

Codex CLI 连接慢：

- 启动 `pool` 时加 `--proxy-url http://127.0.0.1:7890`

## 执行器参数

worker / pool 现在支持两套参数：

- 通用参数：`--agent-*`
- 兼容旧参数：`--codex-*`

推荐优先使用通用参数。

常用通用参数：

| 参数                         | 说明                                                                                                                        |
| -------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| `--agent-type`             | 执行模式，当前支持 `codex` 和 `command-template`                                                                                    |
| `--agent-bin`              | 智能体 CLI 可执行文件                                                                                                             |
| `--agent-model`            | 传给智能体 CLI 的模型名；只对支持模型参数的模板有意义                                                                                             |
| `--agent-timeout-seconds`  | 单个任务最大执行时间                                                                                                                |
| `--agent-command-template` | 自定义命令模板，支持 `{workspace_dir}`、`{prompt_path}`、`{final_message_path}`、`{prompt}`、`{model}`、`{title}`、`{detail}`、`{task_id}` |
| `--agent-use-stdin`        | 把任务提示词通过 stdin 传给 CLI                                                                                                     |
| `--agent-no-stdin`         | 不走 stdin，由命令模板自己消费 `prompt_path` 或 `{prompt}`                                                                             |
| `--agent-arg`              | 给智能体 CLI 追加额外参数，可重复传入                                                                                                     |
| `--workspace-cleanup`      | 任务结束后的 workspace 清理策略：`after-sync-back` / `on-success` / `always` / `never`，默认 `after-sync-back`                          |
| `--workspace-sync-back`    | 把 workspace 变更增量回写到 `--template-dir`：`on-success` / `never` / `always`，默认 `on-success`                                    |

## 测试

```powershell
python -m unittest discover -s tests -v
```

## MCP 接入思路

MCP 在这个项目里不是交给每个 worker 去“自主探索”的。

当前设计是：

- 由代码里的 provider 主动调用 MCP
- worker 只执行 MySQL 里已经领取到的任务
- 不让每个智能体自己决定该调用 MCP 里的哪个 tool

可以把它理解成“代码调用一个外部能力服务”，而不是“把 MCP 当作开放工具箱直接交给 agent”。

流程图：

```text
在线表格
   ^
   |  MCP tool call
   v
DingTalkBaseProvider / GoogleSheetsProvider
   ^
   |  provider.list_tasks() / provider.update_status()
   v
sync loop
   ^
   |  upsert / writeback
   v
MySQL / 本地任务服务
   ^
   |  claim / complete / release
   v
worker pool
   ^
   |  execute task
   v
agent CLI
```

这样设计的原因：

- 工具名、参数结构、状态映射都写死在 provider 里，行为更稳定
- 多个 worker 不会同时直接操作在线表格，避免并发冲突
- 不依赖不同模型自己判断该调哪个 MCP 工具，减少不确定性

对钉钉多维表格来说，当前 provider 内部默认就是：

- 读取任务：`search_base_record`
- 回写状态：`update_records`

worker 不需要知道这些工具细节。


