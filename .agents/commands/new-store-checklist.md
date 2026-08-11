# new-store-checklist

Use when adding or substantially changing a `ratelimit.Store` implementation.

## Checklist

1. **Interface:** `var _ ratelimit.Store = (*Store)(nil)`.
2. **Config:** validate `Tokens` in `1..MaxTokens`, `Interval` (redis `>= 1ms`; others `> 0`), and `Algorithm.Valid()`.
3. **Take:** concurrent-safe; missing key creates state (not an error); backend failures return errors.
4. **Close:** idempotent; set stopped **before** waiting on owned workers; later Take → `ErrStopped`; do not close caller Redis/`*sql.DB`.
5. **Algorithms:** all five supported unless the package docs say otherwise; prefer shared `algo` when in the same module graph; Redis keeps Lua in sync with `algo`.
6. **Tests:** allow/deny, Close/ErrStopped, config reject cases, MaxTokens smoke where float state applies.
7. **Verify:** run `.agents/commands/verify-all.md` for the touched modules (always include the module that owns the store).

See skill `ratelimit-store` and rule `.agents/rules/store-contract.mdc`.
