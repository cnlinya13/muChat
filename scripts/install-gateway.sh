#!/usr/bin/env bash
# muChat 网关一键安装。
# 从 GitHub 拉代码，建 venv，拷到部署目录，装 systemd 单元并启动。
# 不动已有数据，不建对，不发令牌，那些是后面 Muse 的事。
set -euo pipefail

REPO_URL="https://github.com/cnlinya13/muChat.git"
SERVICE="wechat-bridge-gateway.service"
ROOT="$HOME/wechat-bridge-gateway"
PORT="3820"
IP=""
NO_SYSTEMD=0

usage() {
  cat <<EOF
用法：install-gateway.sh --tailnet-ip <内网地址> [选项]
选项：
  --tailnet-ip 地址   网关要绑的内网地址，必填，没法猜；装了 tailscale 可不填，自动取
  --root 目录         部署目录，默认 ~/wechat-bridge-gateway
  --port 端口         网关端口，默认 3820
  --no-systemd        跳过 sudo 和 systemd 步骤，只装文件，适合先验一下
  --help              看这个用法
EOF
}

die() {
  echo "错误：$*" >&2
  exit 1
}

while [ $# -gt 0 ]; do
  case "$1" in
    --tailnet-ip)
      IP="${2:-}"
      [ -n "$IP" ] || die "--tailnet-ip 后面要给一个地址"
      shift 2
      ;;
    --tailnet-ip=*)
      IP="${1#*=}"
      shift
      ;;
    --root)
      ROOT="${2:-}"
      [ -n "$ROOT" ] || die "--root 后面要给一个目录"
      shift 2
      ;;
    --root=*)
      ROOT="${1#*=}"
      shift
      ;;
    --port)
      PORT="${2:-}"
      [ -n "$PORT" ] || die "--port 后面要给一个端口"
      shift 2
      ;;
    --port=*)
      PORT="${1#*=}"
      shift
      ;;
    --no-systemd)
      NO_SYSTEMD=1
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      die "未知参数：$1，用 --help 看用法"
      ;;
  esac
done

# 先检查，缺东西就停下，不硬装。
for cmd in python3 git curl; do
  command -v "$cmd" >/dev/null 2>&1 || die "缺 $cmd，先装好再跑"
done
python3 -c "import venv, ensurepip" 2>/dev/null || die "缺 python3-venv，先装好再跑"
if [ "$NO_SYSTEMD" -eq 0 ]; then
  command -v systemctl >/dev/null 2>&1 || die "缺 systemctl，这台机器没有 systemd"
  command -v sudo >/dev/null 2>&1 || die "缺 sudo，装单元需要 sudo"
  pid1=$(ps -p 1 -o comm= 2>/dev/null | tr -d '[:space:]')
  [ "$pid1" = "systemd" ] || die "PID 1 不是 systemd，这台机器跑不了 systemd 单元"
  [ ! -e "/etc/systemd/system/$SERVICE" ] || die "单元已存在，先处理旧单元再跑：/etc/systemd/system/$SERVICE"
fi

# 网关地址没法猜，没给就试 tailscale，取不到就停下。
if [ -z "$IP" ]; then
  if command -v tailscale >/dev/null 2>&1; then
    IP=$(tailscale ip -n1 2>/dev/null | head -n 1 || true)
  fi
fi
[ -n "$IP" ] || die "网关地址必填，没法猜：用 --tailnet-ip 给一个内网地址"
if ! printf '%s' "$IP" | grep -Eq '^[0-9]{1,3}(\.[0-9]{1,3}){3}$'; then
  die "网关地址不对：$IP，给一个 IPv4 内网地址"
fi
case "$PORT" in
  ''|*[!0-9]*) die "端口不对：$PORT" ;;
esac
if [ "$PORT" -lt 1 ] || [ "$PORT" -gt 65535 ]; then
  die "端口不对：$PORT"
fi

# 已有数据绝不覆盖，没有 --force。
[ ! -e "$ROOT" ] || [ -d "$ROOT" ] || die "部署目录已有文件挡着：$ROOT"
[ ! -e "$ROOT/registry/registry.json" ] || die "已有注册表，停下不覆盖：$ROOT/registry/registry.json"
if [ -d "$ROOT/pairs" ] && [ -n "$(ls -A "$ROOT/pairs" 2>/dev/null)" ]; then
  die "已有配对数据，停下不覆盖：$ROOT/pairs"
fi

echo "拉代码..."
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
git clone --depth 1 "$REPO_URL" "$TMP/repo"
TEMPLATE="$TMP/repo/examples/gateway/wechat-bridge-gateway.service.template"
[ -d "$TMP/repo/gateway" ] || die "仓库里没有 gateway 代码"
[ -d "$TMP/repo/bridge" ] || die "仓库里没有 bridge 代码"
[ -f "$TEMPLATE" ] || die "仓库里没有单元模板"

echo "拷代码到 $ROOT..."
mkdir -p "$ROOT/gateway" "$ROOT/bridge" "$ROOT/registry" "$ROOT/pairs" "$ROOT/secrets"
cp -r "$TMP/repo/gateway/." "$ROOT/gateway/"
cp -r "$TMP/repo/bridge/." "$ROOT/bridge/"
chmod 700 "$ROOT" "$ROOT/registry" "$ROOT/pairs" "$ROOT/secrets"

echo "建 venv..."
python3 -m venv "$ROOT/.venv"
"$ROOT/.venv/bin/pip" install -q requests qrcode pillow

# 按模板生成单元，地址、路径、端口、用户都换成这次的。
# 先把部署目录换成一个临时记号，再换桥目录，最后把记号换回来。
# 不这样分三步的话，部署目录本身含桥目录做前缀时会被换两遍。
escape_sed() {
  printf '%s' "$1" | sed 's/[&|\\]/\\&/g'
}
ROOT_ESC=$(escape_sed "$ROOT")
IP_ESC=$(escape_sed "$IP")
USER_NOW=$(escape_sed "$(id -un)")
sed -e "s|/home/ubuntu/wechat-bridge-gateway|__DEPLOY_ROOT__|g" \
    -e "s|/home/ubuntu/wechat-bridge|$ROOT_ESC|g" \
    -e "s|__DEPLOY_ROOT__|$ROOT_ESC|g" \
    -e "s|YOUR_GATEWAY_TAILNET_IP|$IP_ESC|g" \
    -e "s|--port 3820|--port $PORT|g" \
    -e "s|^User=.*|User=$USER_NOW|" \
    "$TEMPLATE" > "$ROOT/$SERVICE"
grep -q 'YOUR_GATEWAY_TAILNET_IP' "$ROOT/$SERVICE" && die "单元模板里的占位地址没换掉，停下"
grep -Fq -- "--port $PORT" "$ROOT/$SERVICE" || die "单元里的端口没换掉，停下"
grep -Fq "$IP" "$ROOT/$SERVICE" || die "单元里的地址没换掉，停下"

if [ "$NO_SYSTEMD" -eq 0 ]; then
  echo "装单元并启动..."
  sudo cp "$ROOT/$SERVICE" "/etc/systemd/system/$SERVICE"
  sudo chmod 644 "/etc/systemd/system/$SERVICE"
  sudo systemctl daemon-reload
  sudo systemctl enable --now "$SERVICE"

  echo "等服务起来，做健康检查..."
  ok=0
  i=0
  while [ "$i" -lt 10 ]; do
    if curl -s --max-time 5 "http://$IP:$PORT/health" | grep -q '"ok": true'; then
      ok=1
      break
    fi
    sleep 2
    i=$((i + 1))
  done
  if [ "$ok" -ne 1 ]; then
    die "服务没起来，看 systemctl status $SERVICE 找原因"
  fi
fi

echo
echo "装好了。"
echo "网关地址：$IP:$PORT"
if [ "$NO_SYSTEMD" -eq 1 ]; then
  echo "systemd 步骤跳过了，自己装单元并启动："
  echo "  sudo cp $ROOT/$SERVICE /etc/systemd/system/$SERVICE"
  echo "  sudo systemctl daemon-reload && sudo systemctl enable --now $SERVICE"
  echo "起来后用 curl http://$IP:$PORT/health 检查，应返回 ok true。"
fi
echo "下一步：把下面这句发给你的 Muse，地址换成你自己的："
echo "请你读取 https://raw.githubusercontent.com/cnlinya13/muChat/main/examples/gateway/README.md 中的内容并配置你本机环境，配置好以后给我二维码，我要开始绑定。"
echo "网关地址是 $IP:$PORT。"
echo "注意：发图要走官方驱动的话，得另外按 official-send/NOTICE 本地编好 dist 再拷过来，这个脚本不管它。"
