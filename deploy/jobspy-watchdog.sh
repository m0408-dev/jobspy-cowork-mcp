#!/bin/sh
# Docker's startup grace period covers database migrations. A raw HTTP probe
# cannot distinguish startup from a hung service and creates a restart loop.
state=$(docker inspect --format '{{.State.Status}} {{if .State.Health}}{{.State.Health.Status}}{{end}}' jobspy 2>/dev/null) || exit 1
case "$state" in
    'running starting'|'running healthy'|'restarting '*|'paused '*) exit 0 ;;
    'running unhealthy'|'exited '*|'dead '*) docker restart jobspy >/dev/null ;;
    *) exit 1 ;;
esac
