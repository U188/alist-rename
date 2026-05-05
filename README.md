# alist-rename / EmbyRename

面向 **AList / OpenList 网盘媒体库** 的 Emby / Jellyfin 整理工具。它会扫描电视剧、动漫等媒体目录，结合 TMDB 与可选 AI 兜底，把命名混乱的目录整理成更适合刮削、归档和长期维护的结构。

> 安全原则：第一次运行请只处理小范围目录；始终先 `plan` 预演，确认无误后再 `apply` 执行真实改名/移动。

## 目录

- [项目特点](#项目特点)
- [重要说明：只使用 config.json](#重要说明只使用-configjson)
- [运行环境](#运行环境)
- [快速开始](#快速开始)
- [配置说明](#配置说明)
- [常用命令](#常用命令)
- [WebUI 与日志](#webui-与日志)
- [目录结构](#目录结构)
- [典型整理结果](#典型整理结果)
- [安全建议与回滚](#安全建议与回滚)
- [常见问题](#常见问题)
- [开发验证](#开发验证)
- [License](#license)

## 项目特点

| 能力 | 说明 |
| --- | --- |
| AList / OpenList 扫描 | 按 `roots` 或自动发现的媒体根目录扫描剧集、动漫等目录 |
| TMDB 识别 | 查询 TMDB 元数据，生成标准化剧名、年份、季集信息 |
| AI 兜底 | 可用于脏目录名识别、候选选择、分类/地区判断、缺失季判断等场景 |
| 预演 / 执行 | `plan` 只输出计划不改动；`apply` 才执行真实 rename/move |
| 断点续跑 | 可通过 `resume` 控制是否加载已完成状态记录 |
| 归档整理 | 支持整理到 `目标根/分类/地区/剧名 (年份)/Sxx/文件` |
| WebUI | 提供配置、启动/停止任务、实时日志、缓存清理等操作 |
| Undo 回滚 | 执行移动/改名时记录 undo 日志，可按日志回滚 |
| 重构结构 | 核心逻辑已拆分到 `alist_rename/` 包，前端 HTML 单独维护 |

## 重要说明：只使用 config.json

本项目已废弃 `.env`。AList、TMDB、AI、扫描根目录、日志端口等所有业务配置都保存在：

```text
config.json
```

默认位置为项目目录。也可以通过系统环境变量指定独立配置目录：

```bash
export EMBYRENAME_CONFIG_DIR=/opt/embyrename
```

设置后：

```text
/opt/embyrename/config.json
/opt/embyrename/logs/
/opt/embyrename/cache/
```

都会集中存放，便于升级代码时保留配置与运行数据。

> 注意：`config.json` 可能包含 AList token、TMDB key、AI key、WebUI 管理密码哈希等敏感信息，不要提交到公开仓库。

## 运行环境

- Python 3.10+（当前代码在 Python 3.11 环境验证）
- 可访问的 AList / OpenList 服务
- TMDB API Key（建议配置）
- 可选：兼容 OpenAI API 的 AI 服务
- Python 依赖：

```bash
pip install -r requirements.txt
```

当前 `requirements.txt` 至少包含：

```text
requests>=2.31.0
```

## 快速开始

```bash
git clone https://github.com/KingStoning/alist-rename.git
cd alist-rename
chmod +x embyrename
pip install -r requirements.txt
./embyrename setup
```

`setup` 会生成或更新 `config.json`，并尝试辅助初始化配置。

启动 WebUI：

```bash
./embyrename daemon
```

默认访问：

```text
http://127.0.0.1:55255/
```

查看后台状态或停止服务：

```bash
./embyrename status
./embyrename stop
```

## 配置说明

最小 `config.json` 示例：

```json
{
  "alist_url": "http://127.0.0.1:5244",
  "alist_token": "your_alist_token",
  "tmdb_key": "your_tmdb_key",
  "tmdb_lang": "zh-CN",
  "roots": ["/电视剧", "/动漫"],
  "auto_roots": false,
  "dry_run": true,
  "resume": false,
  "ai_base_url": "https://api.openai.com/v1",
  "ai_api_key": "",
  "ai_model": "gpt-4o-mini",
  "log_host": "127.0.0.1",
  "log_port": 55255
}
```

常用字段说明：

| 字段 | 作用 |
| --- | --- |
| `alist_url` | AList / OpenList 地址 |
| `alist_token` | AList 访问 token |
| `tmdb_key` | TMDB API Key |
| `tmdb_lang` | TMDB 语言，默认 `zh-CN` |
| `roots` | 扫描根目录列表 |
| `auto_roots` | 是否自动发现根目录 |
| `dry_run` | 是否只预演；真实执行前建议保持 `true` 验证 |
| `resume` | 是否断点续跑 |
| `state_file` | 断点状态文件路径；为空时使用默认运行目录 |
| `undo_log` | undo 日志路径；为空时自动生成 |
| `ai_base_url` / `ai_api_key` / `ai_model` | AI 兜底配置 |
| `no_ai` | 禁用 AI |
| `ai_infer_season` | 是否允许 AI 辅助推断季 |
| `organize_enabled` | 是否启用分类/地区归档整理 |
| `target_root` | 归档目标根目录 |
| `category_buckets` | 分类桶，如电影、剧集、动漫 |
| `region_buckets` | 地区桶，如大陆、港台、欧美、日韩、其他 |
| `category_region_map` | 分类到地区桶的映射 |
| `scan_exclude_target` | 扫描时排除目标整理根，避免重复扫描已整理内容 |
| `move_individual` | 是否移动单个视频文件到季目录 |
| `on_conflict` | 目标冲突策略，默认 `suffix` |
| `alist_refresh` | 操作后是否刷新 AList |
| `log_port` | WebUI 端口，默认 `55255` |

更完整字段以 `alist_rename/config.py` 中的 `DEFAULTS` 为准，也可以在 WebUI 设置页保存一次后查看生成的 `config.json`。

## 常用命令

```bash
./embyrename setup                         # 一次性向导：生成/更新 config.json
./embyrename search "关键词"               # 只搜索，不改动任何东西
./embyrename fix   "关键词"                # 只修复一个剧：plan → 确认 → apply

./embyrename plan  [--only "关键词"] [--ui]        # 预演，不改动
./embyrename apply [--only "关键词"] [--yes] [--ui]# 真执行，默认二次确认
./embyrename go    [--only "关键词"] [--ui]        # 一键：plan → 确认 → apply

./embyrename batch [--ui]                  # 批量：按 config.json 的 roots/auto_roots 执行
./embyrename daemon [--ui]                 # 后台启动 WebUI/任务中心
./embyrename status                        # 查看后台 WebUI 状态
./embyrename stop                          # 停止后台 WebUI
./embyrename undo  <undo.jsonl> [--yes]    # 回滚上一次 apply 产生的 rename/move
```

说明：

- 启动器不会读取 `.env`。
- CLI 会从 `config.json` 合成运行参数，再交给 `renamer.py` / `alist_rename.cli`。
- `--ui` 用于让任务日志同步到 WebUI。
- `--yes` 用于跳过执行前二次确认，自动化场景才建议使用。

## WebUI 与日志

启动 WebUI：

```bash
./embyrename daemon
```

WebUI 主要能力：

- 查看和保存 `config.json`
- 启动 / 停止整理任务
- 实时查看任务日志
- 查看运行状态
- 清理 TMDB / AI / 状态等缓存
- 管理 WebUI 访问相关配置

### 日志文件

WebUI 启动任务后，会生成两类持久日志：

```text
logs/latest-webui.log
logs/webui-run-YYYYMMDD-HHMMSS.log
```

说明：

- `logs/latest-webui.log`：固定“最新一次 WebUI 任务日志”，方便直接查看最新问题。
- `logs/webui-run-YYYYMMDD-HHMMSS.log`：每次运行独立保存，便于追溯历史。
- CLI 普通运行日志仍会按运行逻辑写入 `logs/embyrename-*.log`。
- WebUI 后台服务自身输出通常在 `logs/webui-service.log`。

查看最新 WebUI 任务日志：

```bash
tail -n 200 logs/latest-webui.log
```

## 目录结构

```text
embyrename                         # Bash 启动器：读取 config.json 调用 CLI / WebUI
renamer.py                         # 兼容入口：转到 alist_rename.cli
logui.py                           # 兼容入口：导出 alist_rename.web
runtime_config.py                  # 兼容配置模块：转到 alist_rename.config
config.example.json                # 示例配置，不包含真实密钥
requirements.txt                   # Python 依赖
README.md                          # 项目说明
LICENSE                            # 许可证
.gitignore                         # 忽略 config.json、日志、缓存、pid/out 等运行文件

alist_rename/
  __main__.py                      # python -m alist_rename 入口
  cli.py                           # CLI 参数、任务编排、WebUI 任务调度
  config.py                        # config.json 读写、默认值、脱敏、密码哈希
  clients/
    alist.py                       # AList / OpenList 客户端
    tmdb.py                        # TMDB 客户端
    ai.py                          # AI 客户端
  common/
    paths.py                       # 路径处理
    rate_limit.py                  # 限速
    text.py                        # 文本工具
  media/
    models.py                      # 媒体数据结构
    parse.py                       # 文件名/目录名解析
    naming.py                      # 标准命名
    resolver.py                    # 媒体识别编排
    tmdb_resolver.py               # TMDB 解析逻辑
  scanner/
    discover.py                    # 根目录发现、扫描候选
    processor.py                   # 单剧处理流程
  ops/
    filesystem.py                  # 改名/移动
    undo.py                        # undo 日志和回滚
    state.py                       # 断点状态
    cleanup.py                     # 缓存/状态清理
  web/
    hub.py                         # 日志 Hub，实时日志和持久日志双写
    handler.py                     # HTTP API handler
    server.py                      # Web 服务启动
    live_log.py                    # 实时日志接口
    templates.py                   # 模板加载
    templates/index.html           # 前端 HTML/CSS/JS，后续 UI 优化改这里
  legacy/
    renamer_core.py                # 兼容旧入口
    logui_core.py                  # 兼容旧入口
```

## 典型整理结果

启用 `organize_enabled` 并配置 `target_root` 后，整理目标应落在目标整理根目录下，而不是继续留在扫描源目录中。

示例：

```text
源目录：
/天翼/我的视频/80动漫 (1983)/博人传 火影忍者新时代 (2017)/S01/xxx.mp4

目标整理根：
/天翼/影视一

期望结果：
/天翼/影视一/动漫/日韩/博人传 火影忍者新时代 (2017)/S01/xxx.mp4
```

其中：

- `动漫` 来自分类判断。
- `日韩` 来自地区判断。
- `博人传 火影忍者新时代 (2017)` 来自 TMDB / AI 识别后的标准剧名。
- `S01` 来自季信息。

## 安全建议与回滚

1. 第一次只处理小范围目录。
2. 优先使用 `plan`，确认目标路径、剧名、年份、季集信息。
3. 再执行 `apply`。
4. 大批量前保留 `logs/undo-*.jsonl`。
5. 不要把 `config.json`、日志、缓存、pid/out 文件提交到 Git 仓库。
6. 开启整理归档时，确认 `target_root` 不在普通扫描范围内；或保持 `scan_exclude_target=true`。
7. 如果真实执行后发现路径不对，优先使用 undo 日志回滚：

```bash
./embyrename undo logs/undo-xxxx.jsonl --yes
```

## 常见问题

### 为什么仓库里没有 `.env`？

`.env` 已废弃。所有业务配置只读写 `config.json`。如果需要把配置和代码分离，请设置 `EMBYRENAME_CONFIG_DIR`。

### 为什么日志里出现“断点续跑”？

只有 `resume=true` 时才应该加载已完成状态记录。若不希望断点续跑，请在 WebUI 或 `config.json` 中关闭 `resume`，必要时清理状态文件。

### WebUI 上看不到最新扫描过程怎么办？

优先查看固定最新日志：

```bash
tail -n 200 logs/latest-webui.log
```

如果还没有该文件，说明 WebUI 重启后还没有点击过“启动”产生任务日志。

### 清理 TMDB 缓存后为什么还像是命中缓存？

先确认清理的是当前 `EMBYRENAME_CONFIG_DIR` 下的缓存，并重新启动任务。WebUI 清理按钮会按当前配置目录处理缓存。

### 如何避免重复扫描整理目标目录？

启用整理归档时建议保持：

```json
{
  "scan_exclude_target": true
}
```

并将 `target_root` 设置为明确的目标整理根目录。

## 开发验证

```bash
python3 -m py_compile renamer.py logui.py runtime_config.py
python3 -m py_compile alist_rename/web/hub.py alist_rename/cli.py
./embyrename --help
```

如果改动 WebUI 前端，主要查看：

```text
alist_rename/web/templates/index.html
```

## License

MIT License. See [LICENSE](LICENSE).