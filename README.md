# muChat

微信个人号直连 Muse 的桥：你在个人微信的“微信 ClawBot”对话里发消息，Muse 执行后把结果发回同一个微信对话。**不经过 OpenClaw 中继**，一台小服务器上的守护进程直接对接腾讯官方 iLink API。

## 它是怎么工作的

```
微信个人号 ── iLink API ── 守护进程 (bridge/daemon.py) ── 队列 ── Hook 唤醒 worker ── 回微信
                              │ 唯一读 bot_token、唯一调 iLink 的进程
                              └─ 入队 inbox → worker 移到 running → 写 outbox → 守护进程发送
```

- **轮询**：守护进程长轮询 `getupdates`，游标按官方顺序（成功响应后立即保存，再处理消息）。
- **队列**：`pairs/<pair_id>/queue/{inbox,running,outbox,done,failed}`，每对隔离。
- **回执**：守护进程自己先发一条“收到，正在处理”，不占用 worker 的冷启动时间；文件批次立刻发并列出文件名，纯文字批次等 2 秒看有没有迟到的文件。
- **原文直通**：守护进程只搬运字节、不解读内容；worker 打开原文亲自读、多文件并行读、合并成一份回复。已删除的旧设计（守护进程抽文本、按哈希复用旧总结）不在本仓库里。
- **打字提示**：官方 `getconfig` / `sendtyping`，长任务期间持续显示“正在输入”。
- **主动消息**：`bridge/proactive_write.py` 只在 24 小时窗口内、且每 token 预算未用完时写 outbox，否则什么都不写并退出；被拒绝的主动消息必须在 Muse 里告诉用户“发条微信重新打开窗口”，绝不静默丢弃。
- **多对**：代码按 pair 隔离（状态、队列、日志、端口各一）；第二对需要第二个微信号和第二个 Muse 账号，见 `MULTI_PAIR_CHECKLIST.md`；第二对 hook 脚本见 `examples/second-pair/`（可直接 curl 安装）。

## 快速开始

```bash
python3 -m venv .venv && .venv/bin/pip install requests pillow qrcode
cp config.example.json config.json   # 按你的环境填，config.json 永不提交
.venv/bin/python bridge/login.py --get-qr   # 生成二维码，2 分钟内扫码
.venv/bin/python bridge/login.py --wait     # 确认后原子写 pairs/<pair>/state/credentials.json (0600)
.venv/bin/python bridge/daemon.py --mode queue --pair primary
```

发图驱动用腾讯官方代码原样编译（见 `official-send/NOTICE`）：本地编译，只把 `dist` 拷到部署机，**不要在小内存机器上跑完整 npm install**。Node 需要 >= 22.13。

## 安全红线

见 `SECURITY.md`：凭据只在 `pairs/<pair>/state/credentials.json`（0600）；只有守护进程读 token；微信不是审批通道；停用 = 停服务 + 关 hook + 微信插件侧解绑。

## 已知局限（实测结论，不夸大）

- **突发丢文件（未关闭，正在查）**：一次发送多个文件时，偶尔会少一个，丢的是哪个文件每轮随机；单发都能到。已排除桥侧过滤和游标顺序，丢点在微信服务器转 iLink 之前、桥之外。
- **语音发送不支持**：通道会静默丢弃机器人语音（API 返回成功、客户端什么都不显示），等腾讯开放后再说；不做音频当文件/视频的变通。
- **速度瓶颈在 worker 段**：管道（回执、唤醒、接手、发送）只有几秒；worker 冷启动 + 干活占 95% 以上，且同形状任务耗时逐轮波动（纯文字实测 57 秒到 2 分 07 秒不等）。Hook 模型下没有常驻热 worker，这是结构性的。
- **视频只能看缩略图/抽帧**，完整视频不能直接观看；无服务器转录的语音无法本地转录；微信语音转文字输入通道侧不投递，桥收不到。
- **只实测过单对**：多对接口已留好，第二对 onboarding 和跨对干扰测试尚未完成。

## 目录

- `bridge/`：`daemon.py`（轮询/队列/回执）、`ilink_client.py`（唯一协议层）、`chunking.py`、`health.py`、`proactive_write.py`、`login.py`、`shadow_poll.py`（影子轮询，仅诊断用）。
- `official-send/`：官方发图驱动的出处说明（`NOTICE`），编译产物本身不进仓库。
- `examples/`：systemd 单元模板（占位符路径）。
- `docs/`：见根目录的 `P2_FINAL_PLAN.md`、`P3_MEDIA_RESEARCH.md`、`TENCENT_IMAGE_RESEARCH.md`、`WHOLE_SOLUTION_PLAN.md`、`MULTI_PAIR_CHECKLIST.md`、`TESTING.md`。

## 许可证

MIT，见 `LICENSE`。`official-send/` 的腾讯部分保留腾讯版权声明，见 `official-send/NOTICE`。
