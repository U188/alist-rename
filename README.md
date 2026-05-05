# alist-rename / EmbyRename

面向 **AList / OpenList 网盘媒体库** 的 Emby / Jellyfin 整理工具。扫描电视剧、动漫等媒体目录，结合 TMDB 与可选 AI 兜底，把命名混乱的目录整理为更适合刮削、归档和长期维护的结构。

> 建议始终先 `plan` 预演，确认结果无误后再 `apply` 执行真实改名/移动。

## 重要说明：配置只使用 config.json

本项目已废弃 `.env`。AList、TMDB、AI、扫描根目录、日志端口等所有业务配置都保存在：

```text
config.json
```

默认位置为项目目录；也可以通过系统环境变量指定独立配置目录：

```bash
export EMBYRENAME_CONFIG_DIR=/opt/embyrename
```

设置后配置文件位于 `/opt/embyrename/config.json`，日志/缓存/undo 等运行文件也会放在该目录下，便于升级代码时保留配置。

## 主要能力

- 扫描 AList / OpenList 中的电视剧、动漫等媒体根目录
- 识别剧名、季、集、清晰度与常见中文质量标记
- 查询 TMDB 元数据并生成标准化目录/文件命名
- 可选 AI 兜底：用于脏目录名、候选选择、分类/地区判断等场景
- 支持预演、真实执行、撤销 undo、后台 WebUI、日志查看
- 支持整理到分类/地区归档树，并可按冲突策略处理同名目标

## 文件说明

```text
embyrename          # Bash 启动器：读取 config.json 调用 renamer.py / WebUI
renamer.py          # 核心扫描、识别、TMDB/AI、改名移动逻辑
logui.py            # WebUI 与实时日志/任务控制
runtime_config.py   # config.json 读写、字段默认值、敏感字段脱敏
requirements.txt    # Python 依赖
.gitignore          # 忽略 config.json、日志、缓存、pid/out 等运行文件
```

`config.json` 可能包含 token/API key，不要提交到公开仓库。

## 快速开始

```bash
git clone https://github.com/KingStoning/alist-rename.git
cd alist-rename
chmod +x embyrename
./embyrename setup
```

`setup` 会生成/更新 `config.json`，并尝试自动发现媒体根目录。也可以直接启动 WebUI 后在设置页填写：

```bash
./embyrename daemon
# 默认本机访问：http://127.0.0.1:55255/
```

## 最小 config.json 示例

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

更完整字段可在 WebUI 设置页保存一次后查看 `config.json`，或参考 `runtime_config.py` 中的 `DEFAULTS`。

## 常用命令

```bash
./embyrename setup                         # 生成/更新 config.json
./embyrename daemon                        # 后台启动 WebUI/任务中心
./embyrename status                        # 查看后台状态
./embyrename stop                          # 停止后台 WebUI

./embyrename search "关键词"               # 只搜索，不改动
./embyrename plan --only "关键词"          # 预演单个关键词
./embyrename apply --only "关键词"         # 执行单个关键词（会二次确认）
./embyrename apply --only "关键词" --yes   # 非交互执行
./embyrename go --only "关键词"            # plan 后确认再 apply
./embyrename batch                         # 按 config.json 批量执行
./embyrename undo logs/undo-xxxx.jsonl --yes
```

启动器不会读取 `.env`；命令参数由 `runtime_config.py` 从 `config.json` 转换后传给 `renamer.py`。

## 安全建议

1. 第一次只处理小范围目录
2. 始终先 `plan`，确认目标路径和命名结果
3. 再执行 `apply`
4. 大批量前保留 `logs/undo-*.jsonl`
5. 不要把 `config.json`、日志、缓存、pid/out 文件提交到 Git 仓库

## 开发验证

```bash
python3 -m py_compile renamer.py logui.py runtime_config.py
./embyrename --help
```

## License

MIT License. See [LICENSE](LICENSE).
