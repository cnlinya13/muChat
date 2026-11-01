# Tencent image send — source-first research (2026-11-01, second subagent)
Confirmed from official mirror https://openclaw-weixin.newfuture.cc/en/backend-api.html:
- POST /ilink/bot/sendmessage (text/image/video/file), POST /ilink/bot/getuploadurl.
- sendMessage example is text-only; response {ret:0, errmsg:""}.
- MessageItem.type: 1 TEXT, 2 IMAGE, 3 VOICE, 4 FILE, 5 VIDEO. getUploadUrl.media_type: 1 IMAGE, 2 VIDEO, 3 FILE, 4 VOICE (different numbering).
- ImageItem all optional in mirror: media?: CDNMedia, thumb_media?, aeskey?: string, url?: string, mid_size?, thumb_size?, thumb_height?, thumb_width?, hd_size?. Mirror does NOT say which are required to render.
- CDNMedia: AES-128-ECB, encrypt_query_param?, aes_key? (base64 AES-128 key), encrypt_type? ("Encryption metadata mode"), full_url?.
- getUploadUrl request: filekey, media_type 1, to_user_id, rawsize (plaintext), rawfilemd5 (plaintext MD5), filesize (ciphertext size after AES-128-ECB), no_need_thumb:true, aeskey (32 hex chars). Response: upload_param?, upload_full_url? (precedence), thumb_upload_param?.
- CDN upload flow: compute plaintext size/MD5 + ciphertext size, getUploadUrl no_need_thumb:true, prefer upload_full_url, encrypt AES-128-ECB, POST ciphertext as application/octet-stream, read x-encrypted-param as CDNMedia.encrypt_query_param, include reference in MessageItem and send. Plugin always no_need_thumb:true, uploads only original media.
Not in official doc (do not guess): exact sendmessage image item JSON, which ImageItem subfields required, ImageItem.aeskey format, encrypt_type values, mid_size/thumb_size meaning (plaintext vs ciphertext), CDN URL construction from upload_param, plaintext upload / url-only outbound variant, size/format/MIME limits, caption handling.
Secondary only (DeepWiki summary of src/messaging/send.ts, not verbatim code): sendImageMessageWeixin sends image using UploadedFileInfo (CDN params + AES keys), caption as separate preceding message, new clientId per item, context_token every request. Conflict: summary says message_state FINISH=1, mirror says FINISH=2 — mirror wins until plugin code itself is opened.
Next concrete investigation (not done yet): download the official npm package @tencent-weixin/openclaw-weixin-cli tarball and read its send.ts source directly, instead of relying on the mirror alone.
