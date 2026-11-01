# P2 最终方案(draft, not implemented until approved)

## 范围
- P2 先跑通一对：当前已绑定的微信（东京登录已确认）对当前这个 Muse。
- 但架构从一开始就按多对设计，不做单对写死、多对再重构。
  多对一对一是本方案的一部分，不是 P2 之后才想的事：
  每一对独立实例、独立凭据、独立队列、独立端口、独立服务、
  独立 hook，消息绝不串对。P2 先以当前这一对验证架构，
  第二对作为多对验证，不改架构只加实例。
- P3 媒体、P4 主动消息不在 P2。

## 东京守护进程（队列模式，多线程，单进程单写者）
运行在东京 ~/wechat-bridge/，systemd 托管，服务名 wechat-bridge，
RestartSec=30（崩溃 30 秒重起）。
- 收消息线程：POST getupdates 长轮询（35 秒挂起），收到就原子写
  queue/inbox/<message_id>.json（0600，含 from_user_id、context_token、
  text、received_at），更新游标和已见 id，并通知 HTTP 线程。
  遇 -14 不退出：取消 typing、写 session_expired 标记、内部暂停 1 小时
  后恢复，符合官方暂停策略。
- HTTP 线程：只绑 tailnet 地址 <BIND_HOST>，端口 3810，不对外网。
  GET /inbox/wait?since=<last_seen_id>&timeout=30，有新文件马上回，
  超时回空。只回消息 id，绝不回正文、context_token、bot_token、用户 id。
- 发消息线程：每 2 秒扫 queue/outbox/，分段（900 字一段），按顺序发，
  段与段之间停 1 秒，每个 context_token 预算 9 条，超了不发半截，
  进 failed 给 worker 看。-2 记成未分类发送失败，不当成配额。
  发完移到 done/，失败移到 failed/。
- typing 线程：消息一进收件箱就开始显示“正在输入”，getconfig 拿票
  （每用户缓存 24 小时，存 state/typing_tickets.json 0600，票值不进日志），
  每 5 秒发一次 status=1 刷新，回复发出去（或失败）后发 status=2 取消。
  typing 已在东京实测通过：服务器全回 0，手机上看到出现和消失。
  长任务不另发“收到，正在处理”文字，因为 typing 本身就是处理中的提示，
  文字会多占 1 条配额。

## Muse 这边：hook 脚本，不是定时 agent
- 脚本：~/hooks/scripts/wechat-bridge-inbox.sh，bash，来源运行环境，
  结尾必为 silent 或 wake 之一。
- 每次运行：用隧道代理向东京发 HTTP 长轮询
  http://<BIND_HOST>:3810/inbox/wait?since=...&timeout=30，
  最多等 35 秒。脚本里不放任何秘密：不放 token、票、代理密码，
  代理来自环境。只看消息 id，不读正文。
- hook 间隔 35 秒，长轮询 30 秒，中间空 5 秒。唤醒平均 2-3 秒，
  最多 5-10 秒加 hook 启动时间。
- 状态：~/hooks/state/wechat-bridge-inbox.json，记已见 id。
  试运行（dry_run）不消耗检测；取数失败（curl 失败、超时、坏 JSON）
  就记日志并 silent，下次再试，本版不悄悄回退到 SSH ls。
- 空查 0 token：查的是脚本，不是 agent。只有真有消息，hook 叫醒
  worker 那一次才耗 token，那次是干活必须花的。

## Worker（hook 叫醒后才跑）
- 只认绑定账号的私聊，不群聊、不多租户、不给第三人发消息。
- 一次只处理一条，按顺序来。先把收件箱文件移到 running/，
  防止重复拿；读完执行任务；写 queue/outbox/<id>.json（全文，
  分段由东京发消息线程做）；再把收件箱文件移到 done/ 或 failed/。
  一次来了多条，在同一次运行里按顺序处理完，不用每条重叫醒。
- 失败处理：worker 崩溃后文件留在 running/，下次 worker 先把
  超过 30 分钟无发件箱的 running/ 文件移回收件箱重试，最多 3 次，
  3 次还不行进 failed/，并经发件箱回微信一句“这个任务失败了”，
  不悄悄丢、不卡在处理中。
- 高后果动作（付款、对外发消息、删除等）worker 不自动批，
  经发件箱回“已在 Muse 里发起确认，请去那边批准”，并移到 done/，
  同时由 worker 摘要通知 Muse 这边，等用户在 Muse 里批。
  微信不是审批通道。

## 延迟预算（简单任务）
- 微信到东京收件箱：几秒。
- 收件箱到 hook 唤醒：平均 2-3 秒，最多 5-10 秒。
- worker 干活：任务本身的时间。
- 发件箱到微信：约 2 秒加发送时间。
- 简单任务全程约 30-60 秒。

## 多对一对一（本方案内，P2 架构即按多对设计）
- 一对 = 一个微信号 + 一个 Muse 账号，配一个 pair_id，例如 primary、second。
  代码是同一套，状态全部分开，绝不共用：
  - 东京目录：~/wechat-bridge/pairs/<pair_id>/state/、queue/、logs/
    （凭据 0600、目录 0700，pair 之间不可读对方目录）
  - 守护进程：每对一个进程，启动带 --pair <pair_id>，读自己的
    pair 配置（凭据路径、队列路径、HTTP 端口），不读别对的配置。
  - HTTP 端口：每对一个，primary 3810，之后每对 +1（3811、3812……），
    只绑 tailnet 地址，只回本对的消息 id，响应里带 pair_id，
    hook 端核对 pair_id 不符就 silent。
  - systemd：每对一个服务 wechat-bridge-<pair_id>.service，
    RestartSec=30，-14 内部暂停 1 小时，互不影响。
  - typing 票、游标、已见 id、每 token 计数：全部按 pair 分开存。
- Muse 这边：每对在它自己的 Muse 账号里单独建 hook 和 worker。
  这一点不能跨账号代劳：当前这个 Muse 聊天只能建当前账号的 hook，
  第二对的 hook 必须在第二个 Muse 账号的聊天里单独建、单独批，
  否则就是把 A 账号的 worker 错绑到 B 对上。
  hook id 也带 pair_id，例如 wechat-bridge-inbox-primary、
  wechat-bridge-inbox-second，查的也是自己那对的 HTTP 端口。
- 加一对的步骤（不改架构，只加实例）：
  1. 新微信号在东京上单独扫码，凭据写进新 pair 的 state/，不覆盖旧对。
  2. 写新 pair 配置，起新 pair 的 systemd 服务，核对 active 和日志。
  3. 在新对对应的 Muse 账号里建 hook（先禁用、dry_run、再启用）。
  4. 串对测试：A 对微信发 ping，只 A 对 Muse 回，B 对必须静默；
     反过来也一样。串对测试不过，第二对不上线。
- 资源：每对一个轻量 Python 进程，2-5 对在东京这台机上没问题；
  对数再多要先复核 CPU/内存，不在本方案里默认无限加。
- 安全边界重申：每对只认自己绑定的那个微信私聊，不群聊，
  不同对之间不共享消息、不共享 token、不共享队列。

## 停用
停东京 systemd 服务 + 关掉 hook + 微信插件侧解绑，三步齐全才算彻底关。

## 实施步骤（定稿后才做）
1. 把 daemon.py 队列模式改成上面四个线程版。
2. 拷到东京，停 echo 守护进程，用 systemd 起队列守护进程，
   核对服务 active、日志、游标文件。
3. 写 hook 脚本，hooks.add（先禁用），hooks.dry_run 直到干净，
   再 hooks.enable。
4. 实测：微信发 ping，核对收件箱时间、唤醒时间、回复时间、
   typing 出现/消失、分段、重启后不重不丢。
5. 按简报四段式汇报 P2：做了什么、验收逐条、遗留风险、
   下一期需要用户做的唯一动作。P2 在实测通过前不算完成。
