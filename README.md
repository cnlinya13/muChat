# muChat

微信个人号直连 Muse 的桥：你在个人微信的“微信 ClawBot”对话里发消息，Muse 执行后把结果发回同一个微信对话。**不经过 OpenClaw 中继**，一台小服务器上的守护进程直接对接腾讯官方 iLink API。

## 优点

- 不用自己改配置、不用看代码，给 Muse 一句话加一个网关地址，剩下的它自己装。
- 扫完绑定自己生效，不用重启服务，不用再跑回来确认。
- 一个端口带所有对，再加微信不用新开端口、不用新装服务，各对走各的队列和令牌，互不串门。
- 服务器上很轻，不用在小内存机器上跑完整 npm install，官方代码在本地编好再拷过去，网关本身只是 Python。
- 直接对接腾讯官方 iLink，不经过第三方中继，也不走非官方客户端登录。
- 凭证加密存，hook 令牌只存哈希，聊天记录和日志里都不会出现明文。

## 它是怎么工作的

```
微信个人号 ── iLink API ── 网关，单端口 3820 ── 队列 ── Hook 唤醒 worker ── 回微信
                              │ 注册表 + 轮询池 + 统一发送器
                              └─ 每对 pair-xxxxxxxx 隔离，hook 令牌逐请求校验
```

- **轮询**：守护进程长轮询 `getupdates`，游标按官方顺序，成功响应后立即保存，再处理消息。
- **队列**：`pairs/<pair_id>/queue/{inbox,running,outbox,done,failed}`，每对隔离。
- **回执**：守护进程自己先发一条“收到，正在处理”，不占用 worker 的冷启动时间；文件批次立刻发并列出文件名，纯文字批次等 2 秒看有没有迟到的文件。
- **原文直通**：守护进程只搬运字节、不解读内容；worker 打开原文亲自读、多文件并行读、合并成一份回复。已删除的旧设计是守护进程抽文本、按哈希复用旧总结，不在本仓库里。
- **打字提示**：官方 `getconfig` / `sendtyping`，长任务期间持续显示“正在输入”。
- **主动消息**：`bridge/proactive_write.py` 只在 24 小时窗口内、且每 token 预算未用完时写 outbox，否则什么都不写并退出；被拒绝的主动消息必须在 Muse 里告诉用户“发条微信重新打开窗口”，绝不静默丢弃。
- **多对**：代码按 pair 隔离，状态、队列、日志都是各对分开的，所有对共用一个网关端口；第二对需要第二个微信号和第二个 Muse 账号，网关 hook 模板见 `examples/gateway/`，可以直接 curl 安装；`examples/second-pair/` 是旧架构的，已由网关取代。

一个网关可以接通多个微信和多个 Muse，微信和 Muse 是一对一配的，配成一对就各走各的队列和令牌，互不干扰。想再加一对，服务器不用动，重复快速开始的第 2、3 步就行。

## 快速开始

### 1. 安装服务端，并拿到网关地址

把 `gateway/` 拷到服务器，装好 systemd 单元并启动，确认健康检查有输出，记下网关地址，默认端口是 3820。
Python 依赖与 `bridge/` 相同，就是 requests 和 qrcode，可直接复用桥的 venv。
发图驱动仍用官方代码本地编译，见 `official-send/NOTICE`，不要在小内存机器上跑完整 npm install。Node 需要 >= 22.13。

```bash
cp -r gateway /home/ubuntu/wechat-bridge-gateway/gateway
# 装单元模板 examples/gateway/wechat-bridge-gateway.service.template
# 地址和路径换成你的，然后：
sudo systemctl enable --now wechat-bridge-gateway.service
curl http://YOUR_GATEWAY_TAILNET_IP:3820/health
# 应返回 {"ok": true, "pairs": [], ...}，记下这个地址和端口
```

### 2. 给 Muse 一句话

把下面这句发给你的 Muse，地址换成你自己的：

```
请你读取 https://raw.githubusercontent.com/cnlinya13/muChat/main/examples/gateway/README.md 中的内容并配置你本机环境，配置好以后给我二维码，我要开始绑定。
网关地址是 YOUR_GATEWAY_TAILNET_IP:3820。
```

### 3. 扫码绑定，即可使用

Muse 出码后用要绑定的微信扫码。码约 2 分钟过期，过期了重出。扫完绑定自动生效，不用重启、不用再确认，然后直接在微信里发消息使用。

## 安全红线

见 `SECURITY.md`：凭据只以加密形式存在，只由网关读取；微信不是审批通道；停用 = 停服务 + 关 hook + 微信插件侧解绑。

## 已知局限

- **突发丢文件**：一次发送多个文件时，偶尔会少一个，丢的是哪个文件每轮随机；单发都能到。已排除桥侧过滤和游标顺序，丢点在微信服务器转 iLink 之前、桥之外。这事目前暂缓。
- **语音发送不支持**：通道会静默丢弃机器人语音，API 返回成功，但客户端什么都不显示，等腾讯开放后再说；不做音频当文件/视频的变通。
- **速度瓶颈在 worker 段**：管道只有几秒，就是回执、唤醒、接手、发送；worker 冷启动 + 干活占 95% 以上，且同形状任务耗时逐轮波动，纯文字实测 57 秒到 2 分 07 秒不等。Hook 模型下没有常驻热 worker，这是结构性的。
- **视频只能看缩略图/抽帧**，完整视频不能直接观看；无服务器转录的语音无法本地转录；微信语音转文字输入通道侧不投递，桥收不到。
- **两对**：两对已绑，交叉测试正向已过，反向待测；文件、图片、语音、视频、引用、突发在网关上还没测过。

## 目录

- `bridge/`：`daemon.py` 做轮询、队列、回执，`ilink_client.py` 是唯一协议层，`chunking.py`、`health.py`、`proactive_write.py`、`login.py`，还有只做诊断的 `shadow_poll.py`。
- `gateway/`：网关服务端，单端口、多对、hook 令牌，见 `examples/gateway/README.md`。
- `official-send/`：官方发图驱动的出处说明在 `NOTICE`，编译产物本身不进仓库。
- `examples/`：systemd 单元模板，路径都是占位符。
- `docs/`：见根目录的 `P2_FINAL_PLAN.md`、`P3_MEDIA_RESEARCH.md`、`TENCENT_IMAGE_RESEARCH.md`、`WHOLE_SOLUTION_PLAN.md`、`MULTI_PAIR_CHECKLIST.md`、`TESTING.md`。

## 许可证

MIT，见 `LICENSE`。`official-send/` 的腾讯部分保留腾讯版权声明，见 `official-send/NOTICE`。
