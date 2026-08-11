#!/usr/bin/env bash
# afterFileEdit: remind to run algo/Redis Lua parity when relevant (fail-open).
set -eu

input=$(cat || true)
file=""
if command -v node >/dev/null 2>&1; then
  file=$(printf '%s' "$input" | node -e '
    let raw = "";
    process.stdin.on("data", (c) => (raw += c));
    process.stdin.on("end", () => {
      try {
        const p = JSON.parse(raw).file_path;
        if (typeof p === "string" && p) process.stdout.write(p);
      } catch {}
    });
  ' || true)
elif command -v python3 >/dev/null 2>&1; then
  file=$(printf '%s' "$input" | python3 -c '
import json, sys
try:
    p = json.load(sys.stdin).get("file_path") or ""
    if isinstance(p, str):
        sys.stdout.write(p)
except Exception:
    pass
' || true)
fi

[ -z "${file:-}" ] && exit 0

case "$file" in
  */algo/*.go|*/algo/*/*.go|*/redisstore/scripts.go)
    ;;
  *)
    # Also match when cwd-relative paths appear without a leading segment.
    case "$file" in
      algo/*.go|algo/*/*.go|redisstore/scripts.go) ;;
      *) exit 0 ;;
    esac
    ;;
esac

# Inject a reminder; ignore if the host ignores unknown fields.
printf '%s\n' '{"additional_context":"Algo/Lua edit detected. Run .agents/commands/parity-redis.md (go test -C redisstore -run TestAlgoRedisParity -race -count=1) and keep algo + redisstore/scripts.go aligned."}'
exit 0
