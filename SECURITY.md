# Security

- **Credentials live in exactly one place:** `pairs/<pair_id>/state/credentials.json`,
  mode 0600, on the daemon host. It holds `bot_token`, `baseurl`, `ilink_bot_id`,
  `ilink_user_id`. It is never printed, never logged, never committed, and never
  sent to any chat, including WeChat.
- **Single-writer rule:** only the daemon calls iLink and reads `bot_token`.
  Worker agents never touch the token; they read inbox files and write outbox
  replies. The daemon is the only process that sends.
- **WeChat is not an approval channel.** Payments, external actions, and
  deletions are approved in Muse, never in WeChat. WeChat only ever receives
  "confirmation started in Muse, please approve there".
- **Private by default:** the notify HTTP server binds a private-network
  address only, returns message IDs (plus verbatim text for text-only messages
  on that same private channel), and never tokens or media.
- **One private chat only.** The bridge accepts the bound user's private chat.
  No multi-tenancy, no group chats, no pushing without a token.
- **Disable:** stop the service, disable the hook, unbind the WeChat plugin.
- **Reporting:** if you find a credential leak path, do not post the credential
  anywhere -- open an issue describing the path only.
