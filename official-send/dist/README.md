# Compiled driver artifact

driver.mjs plus src/ is the compiled output of Tencent's official
openclaw-weixin 2.4.10, used verbatim. See ../NOTICE for attribution
and license. Do not edit these files by hand; rebuild from the
official source instead.

The driver runs on Node builtins only, no package install needed.
It takes a request JSON file and sends one image through the
official iLink channel.
