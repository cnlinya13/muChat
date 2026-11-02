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

### 环境要求

| 项目 | 要求 |
| --- | --- |
| 系统 | Linux，带 systemd |
| Python | Python 3，带 venv |
| Node | 22.13 以上 |
| 组网 | 服务器和 Muse 在同一个 Tailscale 内网里 |

### 1. 安装服务端，并拿到网关地址

在服务器上跑一键安装脚本，地址换成你自己的，端口默认 3820，部署目录默认是 ~/wechat-bridge-gateway：

```bash
curl -sSL https://raw.githubusercontent.com/cnlinya13/muChat/main/scripts/install-gateway.sh | bash -s -- --tailnet-ip YOUR_GATEWAY_TAILNET_IP
```

不想直接管道跑的话，先把仓库拉下来看过再跑：

```bash
git clone https://github.com/cnlinya13/muChat.git
bash muChat/scripts/install-gateway.sh --tailnet-ip YOUR_GATEWAY_TAILNET_IP
```

一条命令装完，文字和图片都能发，装好了脚本会自己做健康检查，通了就打印网关地址，记下它。装了 tailscale 的机器可以不填地址，脚本会自己取，取不到就停下等你填。已经有数据在跑会停下不覆盖。

一键安装只给带 systemd 的 Linux 服务器用，macOS、Windows 机器跑不了一键安装。发图驱动本身只要有 Node 就能跑，缺的只是一键安装这一步。

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
- **语音**：用户发纯语音能收到，语音转文字直接发送不支持。
- **速度瓶颈在 worker 段**：管道只有几秒，就是回执、唤醒、接手、发送；worker 冷启动 + 干活占 95% 以上，且同形状任务耗时逐轮波动，纯文字实测 57 秒到 2 分 07 秒不等。Hook 模型下没有常驻热 worker，这是结构性的。
- **视频只能看缩略图/抽帧**，完整视频不能直接观看。

## 目录

- `bridge/`：`daemon.py` 做轮询、队列、回执，`ilink_client.py` 是唯一协议层，`chunking.py`、`health.py`、`proactive_write.py`、`login.py`，还有只做诊断的 `shadow_poll.py`。
- `gateway/`：网关服务端，单端口、多对、hook 令牌，见 `examples/gateway/README.md`。
- `official-send/`：官方发图驱动的出处说明在 `NOTICE`，编译产物本身不进仓库。
- `examples/`：systemd 单元模板，路径都是占位符。
- `docs/`：见根目录的 `P2_FINAL_PLAN.md`、`P3_MEDIA_RESEARCH.md`、`TENCENT_IMAGE_RESEARCH.md`、`WHOLE_SOLUTION_PLAN.md`、`MULTI_PAIR_CHECKLIST.md`、`TESTING.md`。

## 许可证

MIT，见 `LICENSE`。`official-send/` 的腾讯部分保留腾讯版权声明，见 `official-send/NOTICE`。
