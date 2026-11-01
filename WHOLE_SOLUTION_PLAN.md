# Whole solution plan (P1-P4 + multi-pair, single final acceptance)
Scope per the user 2026-11-01: do everything, not limited to P4, not phased acceptance.
Single final acceptance only after all self-tests pass on the primary pair, then second pair.

## Already passed (primary pair, live)
- P0 plugin enabled, P1 QR login (clean-egress host), ping->pong, restart no-duplicate.
- P2 queue + hook wake + worker reply + typing + systemd, ping 12:12 and 12:18, restart 12:18.
- Multi-chunk self-test 12:21: 2108 chars -> 3 chunks, SENT chunks=3, done, no failed.

## P3 media (to implement, from P3_MEDIA_RESEARCH.md)
- Inbound: getupdates item types 2 IMAGE, 3 VOICE, 4 FILE, 5 VIDEO; CDNMedia AES-128-ECB, full_url if present else encrypt_query_param; key from image_item.aeskey or media.aes_key (precedence/encoding unverified in official doc, live-test against a real inbound image before claiming).
- Outbound: getUploadUrl first (media_type 1 IMAGE, 2 VIDEO, 3 FILE, 4 VOICE — different from MessageItem.type), no_need_thumb:true, aeskey 32 hex chars, rawsize/rawfilemd5/filesize ciphertext; POST ciphertext application/octet-stream to upload_full_url (or upload_param URL); x-encrypted-param -> CDNMedia.encrypt_query_param; sendmessage with that reference. Exact outbound item JSON is NOT in the mirror, so the first outbound media self-test must be verified by daemon log + WeChat receipt, not by doc alone.
- Limits: none stated in mirror; any limit used is an implementation choice, labelled as such.
- Self-test order: (1) outbound image self-test using last_context (no user message needed), (2) inbound image live test at final acceptance (requires a real image sent in WeChat, cannot be self-tested without one).

## P4 proactive (to implement, part of same whole)
- Daemon already writes pairs/<pair>/state/last_context.json on each inbound (P4 readiness, P2 never sends using it).
- Proactive writer: a scheduled Muse task writes pairs/<pair>/queue/outbox/<id>.json with pair_id, from_user_id, context_token from last_context, reply; daemon sender checks: context_token age < ~24h (unverified constant, from brief/mirror, labelled as such) and sent_count for that token + chunks <= 9; if expired or over budget, move to failed with error and do NOT send; the scheduled task must then report back in Muse that the user needs to send a message in WeChat to reopen the window.
- Proactive is opt-in only: a scheduled task must explicitly name WeChat as its delivery target; P2/P3 inbound replies are never proactive.

## Multi-pair (after primary whole solution passes)
- Pair-agnostic daemon (--pair), per-pair state/queue/logs/ports/systemd/hook, pair_id in HTTP response and in hook payload, per-pair typing/cursor/sent-count.
- login.py multi-pair draft is local only (not copied to Tokyo): --pair, pair_dir pairs/<pair>/, CREDENTIALS_EXIST guard unless --force, QR image in pair_dir. Review before copying to Tokyo.
- Second pair onboarding (MULTI_PAIR_CHECKLIST.md): needs second WeChat + second Muse account from the user; second Muse hook must be created in that account's own chat; cross-talk test (ping A must not wake B, ping B must not wake A) is mandatory before second pair goes live.

## Final acceptance checklist (single, primary pair first)
- All P1-P4 self-tests above pass with daemon/journal/queue evidence, no failed queue items left unexplained.
- Then, and only then, bring the whole result back to the user for one final acceptance, followed by second-pair onboarding when she provides the second accounts.
