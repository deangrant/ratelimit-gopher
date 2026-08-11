# Agent and contributor guidance

Structured conventions for AI agents and humans working in this repository. For
fuller context, see [README.md](README.md).

## Docs

- [`.agents/docs/ARCHITECTURE.md`](.agents/docs/ARCHITECTURE.md) — high-level system architecture and diagrams
- [DeepWiki](https://deepwiki.com/deangrant/ratelimit-gopher) — indexed project wiki (architecture, API, pipeline)

## Rules

- [`.agents/rules/`](.agents/rules/) (symlinked from [`.cursor/rules`](.cursor/rules))
- [`.agents/rules/project-core.mdc`](.agents/rules/project-core.mdc) — always-on package/module/style/attribution policy
- [`.agents/rules/store-contract.mdc`](.agents/rules/store-contract.mdc) — Store Take/Close/ErrStopped/MaxTokens contracts
- [`.agents/rules/algo-lua-parity.mdc`](.agents/rules/algo-lua-parity.mdc) — keep `algo` and Redis Lua aligned
- [`.agents/rules/httplimit.mdc`](.agents/rules/httplimit.mdc) — middleware Taker, headers, fail-open policy

## Skills

- [`.agents/skills/`](.agents/skills/)
- [`.agents/skills/ratelimit-store/`](.agents/skills/ratelimit-store/) — Store contracts, backend asymmetries, algo/Lua
- [`.agents/skills/google-go-style-guide/`](.agents/skills/google-go-style-guide/) — Google Go style (naming, errors, tests, APIs)
- [`.agents/skills/solid-go-design/`](.agents/skills/solid-go-design/) — SOLID design in idiomatic Go

## Commands

- [`.agents/commands/`](.agents/commands/) (symlinked from [`.cursor/commands`](.cursor/commands))
- `/verify-all` — multi-module build, race tests, and golangci-lint fmt/run
- `/parity-redis` — `TestAlgoRedisParity` after algo or Lua changes
- `/new-store-checklist` — checklist for a new or changed `Store` implementation

## Hooks

- Canonical: [`.agents/hooks/hooks.json`](.agents/hooks/hooks.json)
- Cursor adapter: [`.cursor/hooks.json`](.cursor/hooks.json)
- `afterFileEdit` → [`.agents/hooks/gofmt.sh`](.agents/hooks/gofmt.sh) formats edited `*.go` files
- `afterFileEdit` → [`.agents/hooks/algo-parity-reminder.sh`](.agents/hooks/algo-parity-reminder.sh) reminds on `algo/` or `redisstore/scripts.go` edits
