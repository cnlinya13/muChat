# 第二对接入（pair=second）

从 GitHub 直接安装（在第二个 Muse 自己的聊天里执行）：

```bash
export MUCHAT_NOTIFY_HOST=<本对的服务器地址>
curl -fsSL https://raw.githubusercontent.com/cnlinya13/muChat/main/examples/second-pair/hook-script-second.sh -o ~/hooks/scripts/wechat-bridge-inbox-second.sh
curl -fsSL https://raw.githubusercontent.com/cnlinya13/muChat/main/examples/second-pair/hook-definition-second.json -o /tmp/hook-definition-second.json
```

然后把定义里的 `delivery.to` 的 `FILL_IN_SECOND_CHAT_ID` 换成你自己聊天的 id，再建 hook（先 disabled，dry_run 干净后 enable）。

（本目录（examples/second-pair/））

- `hook-script-second.sh` → 存为 `~/hooks/scripts/wechat-bridge-inbox-second.sh`
- `hook-definition-second.json` → 建 hook 用的定义；其中 `delivery.to` 的 `FILL_IN_SECOND_CHAT_ID` 必须换成你自己聊天的 id，不许照抄第一对的。

## 执行步骤

1. 放好上面两个文件。
2. 建 hook（先 disabled），dry_run 跑到干净，再 enable。
3. 验证：第二个微信发 ping，只你回复；第一个微信发 ping，你必须全程静默；反向亦然。任一串扰即停并查原因。

## 禁止事项

- 不碰第一对的 hook、队列、凭证、服务。
- 不复用第一对的 token / context_token。
- 本对规则与第一对相同：分开发送、落定等待、清单只列自己打开的文件、原名粘贴不凭记忆。
