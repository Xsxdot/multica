#!/usr/bin/env bash
# Build a single upload folder: Linux backend + Next standalone + macOS Desktop (zip, with CLI),
# root .env, nginx + systemd + install.sh for Ubuntu.
#
# Run on macOS from repo root (or anywhere; paths are derived from this script):
#   bash scripts/deploy/build-fullstack-bundle.sh
#
# Environment (optional):
#   MULTICA_DEPLOY_DOMAIN   default: multica.to6.cn
#   MULTICA_DEPLOY_TLS_CRT  default: /opt/cert/to6_cn.crt
#   MULTICA_DEPLOY_TLS_KEY  default: /opt/cert/to6_cn.key
#   MULTICA_WEB_PORT        default: 3000  (Next listen, nginx proxies here)
#   MULTICA_NODE_BIN       default: /usr/bin/node  (path written into multica-web.service)
#   LINUX_GOARCH            default: amd64  (set arm64 for ARM VPS)
#   SKIP_DESKTOP=1          skip Electron zip (e.g. Linux CI)
#   ELECTRON_MIRROR         default: https://npmmirror.com/mirrors/electron/
#
set -euo pipefail

MULTICA_DEPLOY_DOMAIN="${MULTICA_DEPLOY_DOMAIN:-multica.to6.cn}"
MULTICA_DEPLOY_TLS_CRT="${MULTICA_DEPLOY_TLS_CRT:-/opt/cert/to6_cn.crt}"
MULTICA_DEPLOY_TLS_KEY="${MULTICA_DEPLOY_TLS_KEY:-/opt/cert/to6_cn.key}"
MULTICA_WEB_PORT="${MULTICA_WEB_PORT:-3000}"
MULTICA_NODE_BIN="${MULTICA_NODE_BIN:-/usr/bin/node}"
LINUX_GOARCH="${LINUX_GOARCH:-amd64}"
SKIP_DESKTOP="${SKIP_DESKTOP:-0}"
ELECTRON_MIRROR="${ELECTRON_MIRROR:-https://npmmirror.com/mirrors/electron/}"
ELECTRON_CUSTOM_DIR="${ELECTRON_CUSTOM_DIR:-}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
FULLSTACK_DIR="$SCRIPT_DIR/fullstack"
OUT="$ROOT/scripts/deploy/out/fullstack-bundle-$(date +%Y%m%d-%H%M%S)"
LINUX_DIR="$OUT/linux"
SERVER_DIR="$ROOT/server"

substitute() {
  local in="$1" out="$2"
  sed \
    -e "s#@@DOMAIN@@#${MULTICA_DEPLOY_DOMAIN}#g" \
    -e "s#@@TLS_CRT@@#${MULTICA_DEPLOY_TLS_CRT}#g" \
    -e "s#@@TLS_KEY@@#${MULTICA_DEPLOY_TLS_KEY}#g" \
    -e "s#@@NEXT_PORT@@#${MULTICA_WEB_PORT}#g" \
    -e "s#@@NODE_BIN@@#${MULTICA_NODE_BIN}#g" \
    -e "s#@@WEB_ROOT@@#/opt/multica-web#g" \
    "$in" > "$out"
}

if [[ ! -d "$SERVER_DIR/cmd/server" ]]; then
  echo "Expected repo at $ROOT (missing server/cmd/server)" >&2
  exit 1
fi

if ! command -v go &>/dev/null; then
  echo "go not found in PATH" >&2
  exit 1
fi
if ! command -v pnpm &>/dev/null; then
  echo "pnpm not found in PATH" >&2
  exit 1
fi

mkdir -p "$LINUX_DIR/bin" "$LINUX_DIR/migrations" "$OUT/desktop"

echo "==> Output: $OUT"

echo "==> Rendering install templates..."
substitute "$FULLSTACK_DIR/nginx-fullstack.conf.in" "$OUT/nginx-fullstack.conf"
substitute "$FULLSTACK_DIR/multica-web.service.in" "$OUT/multica-web.service"
substitute "$FULLSTACK_DIR/install.sh.in" "$OUT/install.sh"
chmod +x "$OUT/install.sh"

if [[ ! -f "$FULLSTACK_DIR/multica.service" ]]; then
  echo "Missing $FULLSTACK_DIR/multica.service" >&2
  exit 1
fi
cp "$FULLSTACK_DIR/multica.service" "$OUT/multica.service"

echo "==> Building Linux backend (linux/$LINUX_GOARCH)..."
VERSION="$(cd "$ROOT" && git describe --tags --always --dirty 2>/dev/null || echo dev)"
COMMIT="$(cd "$ROOT" && git rev-parse --short HEAD 2>/dev/null || echo unknown)"
DATE="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
(
  cd "$SERVER_DIR"
  GOOS=linux GOARCH="$LINUX_GOARCH" CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION} -X main.commit=${COMMIT}" -o "$LINUX_DIR/bin/server" ./cmd/server
  GOOS=linux GOARCH="$LINUX_GOARCH" CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION} -X main.commit=${COMMIT} -X main.date=${DATE}" -o "$LINUX_DIR/bin/migrate" ./cmd/migrate
)
cp -a "$SERVER_DIR/migrations/." "$LINUX_DIR/migrations/"

echo "==> Building Next.js standalone (REMOTE_API_URL=http://127.0.0.1:8080)..."
cd "$ROOT"
STANDALONE=true REMOTE_API_URL=http://127.0.0.1:8080 pnpm --filter @multica/web build

STANDALONE_ROOT="$ROOT/apps/web/.next/standalone"
if [[ ! -f "$STANDALONE_ROOT/apps/web/server.js" ]]; then
  echo "Missing $STANDALONE_ROOT/apps/web/server.js after build" >&2
  exit 1
fi

echo "==> Collecting web standalone tree..."
mkdir -p "$LINUX_DIR/web-standalone"
cp -a "$STANDALONE_ROOT/." "$LINUX_DIR/web-standalone/"
mkdir -p "$LINUX_DIR/web-standalone/apps/web/.next"
cp -a "$ROOT/apps/web/.next/static" "$LINUX_DIR/web-standalone/apps/web/.next/static"
cp -a "$ROOT/apps/web/public" "$LINUX_DIR/web-standalone/apps/web/public"

if [[ -f "$ROOT/.env" ]]; then
  cp "$ROOT/.env" "$OUT/.env"
  echo "==> Copied repo .env (keep tarball private)"
else
  echo "WARN: no $ROOT/.env — server install will fail without it" >&2
fi

if [[ "$(uname -s)" == "Darwin" && "$SKIP_DESKTOP" != "1" ]]; then
  echo "==> Packaging Desktop (macOS zip + bundled CLI)..."
  export ELECTRON_MIRROR ELECTRON_CUSTOM_DIR
  export CSC_IDENTITY_AUTO_DISCOVERY="${CSC_IDENTITY_AUTO_DISCOVERY:-false}"
  if ! pnpm --filter @multica/desktop package -- --mac zip; then
    echo "WARN: desktop package failed — set ELECTRON_MIRROR or SKIP_DESKTOP=1" >&2
  else
    zip="$(ls -1t "$ROOT/apps/desktop/dist"/multica-desktop-*-mac-*.zip 2>/dev/null | head -1 || true)"
    if [[ -n "$zip" ]]; then
      cp "$zip" "$OUT/desktop/"
      echo "==> Desktop: $OUT/desktop/$(basename "$zip")"
    fi
  fi
else
  echo "==> Skipping Desktop (not macOS or SKIP_DESKTOP=1)"
fi

{
  echo "BUILT=$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  echo "GIT=$VERSION"
  echo "LINUX_GOARCH=$LINUX_GOARCH"
  echo "DOMAIN=$MULTICA_DEPLOY_DOMAIN"
} > "$OUT/BUNDLE_BUILD.txt"

cat > "$OUT/README.txt" << EOF
Multica full-stack bundle
==========================
Domain (nginx): $MULTICA_DEPLOY_DOMAIN
Linux backend:  linux/bin/{server,migrate} + linux/migrations/
Web:            linux/web-standalone/  (Next standalone — run: node apps/web/server.js)
Desktop (Mac): desktop/*.zip

Server requirements (Ubuntu):
  - Node.js 22+ at $MULTICA_NODE_BIN (or edit multica-web.service before install)
  - nginx
  - TLS files: $MULTICA_DEPLOY_TLS_CRT and $MULTICA_DEPLOY_TLS_KEY

Install on server:
  scp -r this folder to the host, then:
  sudo bash install.sh

  sudo bash install.sh --replace-env   # overwrite /opt/multica/.env

Then:
  sudo systemctl start multica.service multica-web.service
  sudo systemctl reload nginx

If you previously installed nginx only for the API, remove the old site file
that used the same server_name before reloading nginx (avoid duplicate server blocks).

Rebuild this bundle with another domain:
  MULTICA_DEPLOY_DOMAIN=app.example.com MULTICA_DEPLOY_TLS_CRT=/path/fullchain.pem \\
  MULTICA_DEPLOY_TLS_KEY=/path/privkey.pem bash scripts/deploy/build-fullstack-bundle.sh
EOF

echo ""
echo "==> Done: $OUT"
echo "    Upload the whole folder; on Ubuntu: sudo bash install.sh"
