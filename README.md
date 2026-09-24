# ShadowBot Auto Retry Controller

面向影刀社区版 6.3.x 的 Windows 本地失败任务自动重试服务。

它读取影刀本地 `tasks.db3`，识别定时触发和邮件触发的失败任务，并通过已经登录的影刀桌面客户端重新发起任务。项目只使用 Windows UI Automation 操作客户端，不调用影刀 CLI、企业版 API 或 `shadowbot:Run` 协议，因此不会消耗影刀社区版的 CLI 体验额度。

> 本项目依赖影刀桌面 UI 的当前结构，仅在影刀社区版 6.3.x 环境验证过。请先在测试环境确认行为，再用于生产流程。

## 功能

- 监控 `sourceKind=0`（定时）和 `sourceKind=8`（邮件）触发的失败记录。
- 用持久化状态机管理队列；控制器重启后可恢复未完成任务。
- 默认按 30、120、300 秒退避；最多计入 3 次**已验证实际执行**的失败重试。
- 仅在出现新的 `.tasklog` 或 `tasks.db3` 记录后确认本次启动；单纯点击“运行”绝不算作成功或一次重试。
- 检测正在运行的流程、普通定时任务静默窗口、应用设计器打开、客户端窗口隐藏等情形，安全延后而非盲目点击。
- 精确匹配完整应用名；列表虚拟化时使用唯一搜索框定位，且不会使用固定屏幕坐标。
- 可选企业微信机器人通知，以及可选的 Codex 失败原因分析。
- 单实例互斥锁、运行心跳、最近 3 天滚动日志和单元测试覆盖。

## 工作流程

```text
影刀 tasks.db3
      │ 扫描失败任务
      ▼
持久化重试队列 ──► 安全检查 ──► 影刀桌面 UI 发起运行
      ▲                                  │
      └──── 未确认/需等待 ◄── tasklog 或数据库记录验证
                                         │
                         成功 / 重试失败 / 用户取消 / 达到上限
```

主要状态：

```text
queued -> deferred / paused_editing -> launch_requested -> retry_running -> succeeded / exhausted
                                  \-> cancelled
```

## 环境要求

- Windows 10/11
- Python 3.11（其他版本未验证）
- 影刀社区版 6.3.x
- 已登录的影刀客户端，且与控制器运行在同一个 Windows 用户会话
- 会话必须保持可交互、未锁屏；远程桌面断开后若交互桌面被隐藏，任务会保持等待

## 安装与启动

1. 克隆仓库并安装依赖：

   ```powershell
   python -m pip install -r requirements.txt
   ```

2. 复制示例配置：

   ```powershell
   Copy-Item retry_config.example.json retry_config.json
   ```

3. 编辑 `retry_config.json` 中至少两个字段：

   - `shadowbot_exe`：影刀启动程序路径，通常为 `C:\Program Files\ShadowBot\ShadowBot.exe`。
   - `user_folder`：当前影刀账号对应的本地用户目录，其中必须存在 `tasks.db3` 和 `task_logs`。

   `enable_codex_analysis` 默认为 `false`。若保持关闭，`codex_exe` 与 `codex_workdir` 可填写任意非空占位路径；若开启，请改为实际 Codex 可执行文件和工作目录。

4. 启动影刀并确认处于已登录状态，然后双击 `start_controller.cmd`，或运行：

   ```powershell
   python retry_controller_service.py
   ```

5. 使用 `view_logs.cmd` 查看实时日志。服务日志写入 `logs/retry_controller.log`。

## 可选通知

运行以下命令并输入企业微信群机器人 Webhook。凭据使用当前 Windows 用户的 DPAPI 加密保存为本机文件，不会写入 Git。

```powershell
python configure_webhook.py
```

## 诊断与测试

```powershell
# 输出已保存的队列状态
python retry_controller.py status

# 执行单元测试
python -m unittest discover -s tests -v
```

`health` 诊断命令会检查影刀和 Codex 环境；仅在配置了实际 Codex 路径时使用：

```powershell
python retry_controller.py health
```

## 配置重点

| 字段 | 默认值 | 说明 |
| --- | ---: | --- |
| `retry_delays_seconds` | `[30, 120, 300]` | 每次已验证失败后的等待时间。 |
| `scheduled_task_quiet_seconds` | `60` | 普通定时活动结束后的静默窗口。 |
| `community_initial_quiet_seconds` | `60` | 新发现失败的初始观察时间。 |
| `editing_clear_stable_seconds` | `30` | 关闭设计器后需持续稳定的秒数。 |
| `task_start_timeout_seconds` | `60` | 等待启动凭据出现的最大时间。 |
| `task_run_timeout_seconds` | `1800` | 等待已确认任务执行结果的最大时间。 |

## 安全与数据边界

`.gitignore` 已排除 `retry_config.json`、运行状态、日志和企业微信凭据。它们可能含有本机路径、应用名、任务 ID、错误信息或通知密钥，均不应提交。

控制器只操作当前用户桌面中已登录的影刀客户端；不会上传 `tasks.db3`，也不会调用影刀的企业接口。请在有权限的机器和账号上使用。

## 开源许可

本项目采用 [MIT License](LICENSE)。
