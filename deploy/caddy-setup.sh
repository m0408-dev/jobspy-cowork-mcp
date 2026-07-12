#!/usr/bin/env bash
# Run this ON THE VM (via SSH or Cloud Shell's "SSH to instance") after it has booted
# and you know its public IP. Gives you free automatic HTTPS with no domain purchase,
# using sslip.io (a public DNS service that resolves "<ip>.sslip.io" -> <ip>).
#
# Usage: ./caddy-setup.sh 123.45.67.89

set -euo pipefail
PUBLIC_IP="${1:?Usage: ./caddy-setup.sh <public-ip>}"
HOSTNAME="${PUBLIC_IP//./-}.sslip.io"

sudo tee /etc/caddy/Caddyfile > /dev/null <<EOF
${HOSTNAME} {
    reverse_proxy 127.0.0.1:8000
}
EOF

sudo systemctl reload caddy

echo ""
echo "Done. Your connector URL (once DNS + Let's Encrypt settle, ~30s):"
echo "  https://${HOSTNAME}/mcp-CHANGE-ME-9f3a2b/"
echo ""
echo "Health check:"
echo "  curl https://${HOSTNAME}/health"
