# Codex CLI Task Orchestrator

这个程序的目标是：

1. 从在线任务源读取任务
2. 把任务同步到本地 SQLite 队列
3. 用多个全新的 `codex exec` 会话并发执行任务
4. 把执行状态写回本地任务服务
5. 再由同步程序把状态回写到在线表格

每个任务都运行在新的 Codex CLI 会话里，因此不会继承上一个任务的上下文。

## 任务表格式

在线表格固定按前三列读取：

| A列 | B列 | C列 |
| --- | --- | --- |
| 标题 | 任务详情 | 状态 |

状态只允许：

- `未开始`
- `执行中`
- `已完成`

## 组件说明

这个程序运行时通常有 3 个长期进程：

1. `serve`
   本地任务服务。worker 不直接读在线表格，而是只连这个本地服务。
2. `sync loop`
   在线表格同步进程。负责把在线表格导入本地数据库，并把本地状态回写到在线表格。
3. `pool`
   worker 池。负责启动多个独立 worker，每个 worker 再启动新的 `codex exec` 执行任务。

关系可以理解成：

`在线表格 <-> sync loop <-> SQLite / 本地任务服务 <-> worker pool <-> codex exec`

本地任务服务启动后，可以通过：

- `GET /` 查看网页表格
- `GET /table.tsv` 查看 TSV 形式的 A-C 三列
- `GET /api/tasks` 查看完整 JSON

## 并发设计

并发问题按两层处理：

1. 抢任务冲突
   服务端用 SQLite `BEGIN IMMEDIATE` 事务原子地完成“挑一条 `未开始` 任务并更新成 `执行中`”。多个 worker 同时请求时，只会有一个 worker 抢到某一行。
2. 多个 Codex CLI 写同一目录的冲突
   每个任务都会先复制一个独立工作目录，然后在这个目录里执行新的 `codex exec`。这样不同任务不会在同一份代码上互相覆盖。

之所以默认复制 workspace，是因为多个 Codex CLI 同时在同一目录改文件时，结果很容易互相覆盖，最终既无法知道是谁改坏的，也无法可靠回滚。独立 workspace 是当前并发模式下最稳妥的隔离方式。

另外还有租约机制：

- worker 抢到任务后会带一个 `lease_seconds`
- 运行期间持续 heartbeat
- 如果 worker 崩溃，租约到期后任务会被自动回收到 `未开始`

## 在线表格适配

现在的结构分成两层：

1. 在线表格适配层
   负责从在线表格读取 `标题 / 任务详情 / 状态`，并把本地执行状态回写回去。
2. 本地任务队列层
   负责并发 claim、租约、完成、失败释放。这一层仍然用 SQLite 做原子控制。

这样做的原因是，大多数在线表格 API 都不提供可靠的“原子抢任务”语义。如果让多个 worker 直接抢在线表格，很容易出现同一行被多个 worker 同时领取。现在改成“先同步到本地，再由本地原子分发”，并发行为会稳定很多。

当前内置了两个在线表格 provider：

- `google-sheets`
- `generic-json`

其中 `generic-json` 是通用 REST 表格适配器，适合给飞书、钉钉、自建表格 API 做包装。

## 启动流程

推荐按下面顺序启动。最清晰的方式是开 3 个终端窗口。

### 第 0 步：准备 Google Sheets 凭证

如果你用的是 Google Sheets：

1. 在 Google Cloud 创建一个 service account
2. 下载 JSON 密钥文件
3. 把 service account 邮箱加到你的 Google Sheet 共享成员里
4. 修改 [`examples/google-sheets.sync.json`](f:/work/codexSyncDemo/examples/google-sheets.sync.json)

至少改这两项：

- `spreadsheet_url`
- `service_account_file`

并安装依赖：

```powershell
python -m pip install google-auth
```

### 第 1 步：启动本地任务服务

```powershell
python -m codex_orchestrator serve --host 127.0.0.1 --port 8000 --db .codex-runtime/tasks.db
```

这个进程的作用：

- 提供本地 API 给 worker 抢任务
- 维护 SQLite 里的任务状态
- 对外提供 `/api/tasks`、`/table.tsv`、`/`

建议这个终端一直保持运行。

### 第 2 步：启动在线表格同步

单次调试先用：

```powershell
python -m codex_orchestrator sync once `
  --db .codex-runtime/tasks.db `
  --config .\examples\google-sheets.sync.json
```

确认没问题后，再启动持续同步：

```powershell
python -m codex_orchestrator sync loop `
  --db .codex-runtime/tasks.db `
  --config .\examples\google-sheets.sync.json `
  --interval-seconds 15
```

如果同步进程也需要走本地代理，直接加：

```powershell
python -m codex_orchestrator sync loop `
  --db .codex-runtime/tasks.db `
  --config .\examples\google-sheets.sync.json `
  --interval-seconds 15 `
  --proxy-url http://127.0.0.1:7890
```

这个进程的作用：

- 从 Google Sheet 读取 A/B/C 三列
- 导入本地 SQLite
- 把本地任务状态回写到 Google Sheet

建议这个终端也一直保持运行。

### 第 3 步：启动 worker 池

如果是 Windows，建议显式指定 `codex.cmd`。

```powershell
python -m codex_orchestrator pool `
  --server-url http://127.0.0.1:8000 `
  --workers 3 `
  --template-dir . `
  --runtime-dir .codex-runtime `
  --codex-bin codex.cmd `
  --server-timeout-seconds 10 `
  --codex-timeout-seconds 900
```

如果本机代理开在 `7890`，可以再加：

```powershell
python -m codex_orchestrator pool `
  --server-url http://127.0.0.1:8000 `
  --workers 3 `
  --template-dir . `
  --runtime-dir .codex-runtime `
  --codex-bin codex.cmd `
  --proxy-url http://127.0.0.1:7890
```

这个进程的作用：

- 从本地任务服务领取 `未开始` 任务
- 给每个任务复制独立 workspace
- 启动新的 `codex exec`
- 完成后更新状态

### 第 4 步：确认系统在正常工作

可以打开：

- `http://127.0.0.1:8000/`
- `http://127.0.0.1:8000/api/tasks`

正常情况下你会看到状态流转：

1. 从在线 URL 抢一条 `未开始` 任务
2. 变成 `执行中`
3. 启动一个全新的 `codex exec`
4. 成功则把状态更新为 `已完成`
5. 失败则回退为 `未开始`

## 常用命令

### 本地服务

```powershell
python -m codex_orchestrator serve --host 127.0.0.1 --port 8000 --db .codex-runtime/tasks.db
```

### 单次同步

```powershell
python -m codex_orchestrator sync once --db .codex-runtime/tasks.db --config .\examples\google-sheets.sync.json
```

### 持续同步

```powershell
python -m codex_orchestrator sync loop --db .codex-runtime/tasks.db --config .\examples\google-sheets.sync.json --interval-seconds 15
```

```powershell
python -m codex_orchestrator sync loop --db .codex-runtime/tasks.db --config .\examples\google-sheets.sync.json --interval-seconds 15 --proxy-url http://127.0.0.1:7890
```

### 启动 worker 池

```powershell
python -m codex_orchestrator pool --server-url http://127.0.0.1:8000 --workers 3 --template-dir . --runtime-dir .codex-runtime --codex-bin codex.cmd
```

## Provider 配置

### Google Sheets

示例文件：[`examples/google-sheets.sync.json`](f:/work/codexSyncDemo/examples/google-sheets.sync.json)

推荐使用 `service_account_file`，因为它可以同时完成读取和状态回写。

需要提供：

- `spreadsheet_url` 或 `spreadsheet_id`
- `sheet_name`
- `service_account_file`

安装依赖：

```powershell
python -m pip install google-auth
```

1. 在 Google Cloud 创建一个 service account，并下载 JSON 密钥文件。
2. 把这个 service account 的邮箱加入你的 Google Sheet 共享成员，至少给编辑权限。
3. 把共享链接填到 `spreadsheet_url`。
4. 把 JSON 文件路径填到 `service_account_file`。
5. 运行 `sync once` 或 `sync loop`。

如果你的共享链接是：

```text
https://docs.google.com/spreadsheets/d/1AbCdEfGhIjKlMnOpQrStUvWxYz/edit?gid=0#gid=0
```

程序会自动提取：

```text
1AbCdEfGhIjKlMnOpQrStUvWxYz
```

所以通常不需要手工找 `spreadsheet_id`。

Google Sheets provider 约定：

- A列：标题
- B列：任务详情
- C列：状态
- 第 1 行默认是表头
- 程序会回写对应行的 C 列状态

### Generic JSON

示例文件：[`examples/generic-json.sync.json`](f:/work/codexSyncDemo/examples/generic-json.sync.json)

这个 provider 用于适配“在线表格已经有 HTTP API”的场景。你只需要告诉程序：

- 从哪个 URL 读列表
- 列表数组在 JSON 里的哪一层
- 哪个字段是 `id/title/detail/status`
- 更新单条状态时的 URL 模板和 HTTP 方法

`headers` 支持 `${ENV_NAME}` 形式的环境变量替换，方便放 token。

## 目录说明

- `.codex-runtime/tasks.db`: 任务数据库
- `.codex-runtime/worker-*/task-*/workspace`: 每个任务的独立工作区
- `.codex-runtime/worker-*/task-*/logs`: `codex` 标准输出、错误输出和最终摘要
- `.codex-runtime/task-results/task-0001-*.json|txt`: 按 task 编号输出的结果摘要，里面会直接写明标题、详情、状态、worker、workspace、logs 路径

## 故障排查

### `sync once` / `sync loop` 报错

优先检查：

- `spreadsheet_url` 是否正确
- `sheet_name` 是否和 Google Sheet 底部标签页名字一致
- `service_account_file` 路径是否正确
- service account 邮箱是否已加入表格共享成员
- 如果是 `oauth2.googleapis.com/token`、`SSLEOFError` 或 TLS 连接中断，优先给 `sync once` / `sync loop` 加 `--proxy-url http://127.0.0.1:7890`

`sync loop` 现在会在进程内复用 service account 凭证，不会每一轮都重新请求一次 token；如果你修改了同步配置，需要重启 `sync` 进程让新配置生效。

### worker 没有执行任务

优先检查：

- `serve` 是否还在运行
- `sync loop` 是否已经把任务导入到本地
- `http://127.0.0.1:8000/api/tasks` 里是否存在 `未开始` 任务

### Codex 连接慢或超时

如果本机代理监听在 `7890`，启动 `pool` 时加：

```powershell
--proxy-url http://127.0.0.1:7890
```

## 运行测试

```powershell
python -m unittest discover -s tests -v
```

## 说明

- 默认本地 worker 仍然连程序自带的 HTTP 任务服务；在线表格通过 `sync` 命令导入/回写，不直接给 worker 抢。
- `sync` 当前会把在线表格里的标题和详情持续同步到本地；状态以本地队列为准，再反向回写到在线表格。
- 如果你后面要接入现有的在线表格系统，比如 Google Sheets、飞书多维表格、钉钉表格或自建 API，优先新增一个 provider，不要改 worker 的并发控制逻辑。
- 当前实现不会自动把多个并发任务的代码改动合并回同一份目录，因为这件事没有可靠的无冲突通用方案。程序会把每个任务结果保存在独立工作区里，供你后续审查或手工合并。
