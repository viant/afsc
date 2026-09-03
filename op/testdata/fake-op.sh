#!/bin/sh
# Test double for the 1Password CLI. Used by unit tests so `op read` never
# touches a real 1Password session or prompts for access.
if [ "$1" != "read" ]; then
  echo "fake-op: unsupported command: $1" >&2
  exit 1
fi
case "$2" in
  *nonexistent*)
    echo "[ERROR] 2026/01/01 00:00:00 could not read secret '$2': item not found" >&2
    exit 1
    ;;
esac
printf '\n%s\n' '{"type":"service_account","project_id":"test"}'
