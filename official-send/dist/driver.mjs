import fs from "node:fs";
import { uploadFileToWeixin } from "./src/cdn/upload.js";
import { sendImageMessageWeixin } from "./src/messaging/send.js";
// Request file mode (daemon sender): a JSON file with baseUrl, token, to,
// contextToken, filePath, caption. Token never printed. File is deleted by
// the caller after this process exits; this script only reads it.
const reqPath = process.argv[2];
const req = JSON.parse(fs.readFileSync(reqPath, "utf-8"));
const opts = { baseUrl: req.baseUrl, token: req.token, contextToken: req.contextToken };
const uploaded = await uploadFileToWeixin({ filePath: req.filePath, toUserId: req.to, opts, cdnBaseUrl: "https://novac2c.cdn.weixin.qq.com/c2c" });
const res = await sendImageMessageWeixin({ to: req.to, text: req.caption || "", uploaded, opts, filePath: req.filePath });
console.log(JSON.stringify({ ok: true, messageId_present: Boolean(res.messageId), serverMessageId_present: Boolean(res.serverMessageId), fileSize: uploaded.fileSize, cipherSize: uploaded.fileSizeCiphertext }));
