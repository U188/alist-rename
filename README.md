# alistrename / EmbyRename

面向 **AList / OpenList 网盘媒体库** 的 Emby / Jellyfin 整理工具。它会扫描剧集、动漫等目录，结合 TMDB 与可选 AI 兜底，把命名混乱的媒体目录整理成更适合刮削、归档和长期维护的结构。

> 建议始终先 `plan` 预演，确认结果无误后再 `apply` 执行真实改名/移动。

## 主要能力

- 扫描 AList / OpenList 中的电视剧、动漫等媒体根目录
- 识别剧名、季、集、清晰度与常见中文质量标记
- 查询 TMDB 元数据并生成标准化目录/文件命名
- 可选 AI 兜底：用于脏目录名、候选选择、分类/地区判断等场景
- 支持归档到类似 `剧集/大陆`、`动漫/日韩` 的分类目录
- 支持 Web UI：配置、启动/停止任务、查看实时日志、清理缓存
- 支持 dry-run/plan、apply、daemon 后台任务和 undo 回滚
- 内置运行日志、断点续跑状态和缓存文件；这些运行时文件默认不应提交到仓库

## 仓库文件

建议上传到新仓库的核心文件：

```text
README.md
LICENSE
requirements.txt
.env.example
.gitignore
embyrename
renamer.py
logui.py
VERSION
```

不要上传真实运行配置和运行时产物：

```text
.env
.venv/
__pycache__/
logs/
*.pid
*.out
*.log
config.json
runtime_config.py
ui_config.json
tmdb_cache.json
roots_cache*.json
*.bak-*
```

## 环境要求

- Linux / macOS / WSL
- Python 3.10+
- 可访问的 AList / OpenList 服务
- TMDB API Key
- 可选：OpenAI 兼容的 AI 接口

## 快速开始

### 1. 克隆并进入项目

```bash
git clone <your-repo-url> alistrename
cd alistrename
```

### 2. 准备配置

```bash
cp .env.example .env
```

编辑 `.env`，至少填写：

```env
ALIST_URL="http://127.0.0.1:5244"
ALIST_TOKEN="your_alist_token"
TV_ROOTS="/电视剧,/动漫"
TMDB_KEY="your_tmdb_key"
```

如需 AI 兜底，再填写：

```env
AI_BASE_URL="https://api.openai.com/v1"
AI_API_KEY="your_ai_api_key"
AI_MODEL="gpt-4o-mini"
```

### 3. 首次运行

包装脚本会自动创建 `.venv` 并安装 `requirements.txt` 中的依赖：

```bash
chmod +x ./embyrename
./embyrename --help
```

也可以手动安装：

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 常用命令

```bash
./embyrename setup                         # 一次性向导：生成 .env 并尝试自动发现 TV_ROOTS
./embyrename search "关键词"               # 只搜索，不改动
./embyrename fix "关键词"                  # 修复一个剧：plan -> 确认 -> apply
./embyrename plan [--only "关键词"] [--ui]  # 预演，不改动真实文件
./embyrename apply [--only "关键词"] [--yes] [--ui]  # 真实执行
./embyrename go [--only "关键词"] [--ui]    # 一键 plan -> 确认 -> apply
./embyrename batch [--ui]                  # 批量处理 TV_ROOTS 下项目
./embyrename daemon [--ui]                 # nohup 后台批量任务
./embyrename status                        # 查看后台任务状态
./embyrename stop                          # 停止后台任务
./embyrename undo <undo.jsonl> [--yes]      # 回滚上一次 apply 记录
```

## Web UI

在命令后加 `--ui` 可启动 Web UI，用于查看日志、调整配置、清理缓存和控制任务：

```bash
./embyrename plan --ui
./embyrename apply --ui
./embyrename daemon --ui
```

相关 `.env` 配置：

```env
LOG_HOST="0.0.0.0"
LOG_PORT="55255"
LOGUI_TOKEN="change_me"
LOG_FILE=""
STATE_FILE=""
UNDO_FILE=""
```

访问示例：

```text
http://127.0.0.1:55255/?token=change_me
```

如果部署到公网，请务必修改 `LOGUI_TOKEN`，并自行处理反代、TLS、防火墙和访问控制。

## 关键配置说明

### AList / OpenList

```env
ALIST_URL="http://127.0.0.1:5244"
ALIST_TOKEN="your_alist_token"
TV_ROOTS="/电视剧,/动漫"
```

- 推荐使用 `ALIST_TOKEN`
- `TV_ROOTS` 多个目录用英文逗号分隔
- 不建议把电影总库和剧集/动漫混在一起一把梭处理

### TMDB

```env
TMDB_KEY="your_tmdb_key"
TMDB_LANG="zh-CN"
TMDB_API_BASE="https://api.themoviedb.org"
```

`TMDB_API_BASE` 可填写官方地址，也可填写兼容代理地址；程序会按实现规则补齐常见路径。

### AI 兜底

```env
AI_BASE_URL="https://api.openai.com/v1"
AI_API_KEY="your_ai_api_key"
AI_MODEL="gpt-4o-mini"
AI_SLEEP="1.2"
```

只要未填写有效 `AI_API_KEY`，程序不会调用 AI。启用后主要用于：

- 目录名过脏，普通规则难以提取标题
- TMDB 候选过多，需要辅助选择
- 分类/地区无法稳定判断
- 归档路径需要兜底判断

AI 只是辅助，不保证 100% 正确；首次使用请先小范围 `plan`。

### 归档目录

```env
TARGET_ROOT=""
CATEGORY_REGION_MAP="剧集:大陆,港台,欧美,日韩,其他;动漫:大陆,港台,日韩,欧美,其他;电影:大陆,港台,欧美,日韩,其他;纪录片:大陆,港台,欧美,日韩,其他;综艺:大陆,港台,日韩,欧美,其他"
```

- `TARGET_ROOT` 留空：尽量回到来源顶层根下归档
- `TARGET_ROOT` 非空：统一归档到指定根目录
- 归档判断优先级大致为 TMDB 元数据、AI 结果、路径提示、默认兜底

## 安全建议

1. 第一次只处理小范围目录
2. 始终先 `plan`，确认目标路径和命名结果
3. 再执行 `apply`
4. 大批量前保留 `logs/undo-*.jsonl`
5. 不要把 `.env`、日志、缓存、pid/out 文件提交到 Git 仓库

## 开发验证

```bash
python3 -m py_compile renamer.py logui.py
./embyrename --help
```

## License

MIT License. See [LICENSE](LICENSE).
