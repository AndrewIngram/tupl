# AGENTS.md

## Verification

For any non-trivial code change, always run:

- `pnpm lint`
- `pnpm typecheck`
- `pnpm test`
- `pnpm fmt`

Do this before finalizing the work unless the user explicitly asks not to.

## Code coverage

When code coverage is needed:

- run `pnpm test:coverage` locally
- expect terminal coverage output plus `coverage/index.html` and `coverage/lcov.info`
- in GitHub Actions, coverage is produced by the `Coverage Report` job in `.github/workflows/ci.yml`
- the CI job merges coverage from the existing fast and slow Vitest jobs and uploads the `coverage-report` artifact

## Reporting

If any verification step fails:

- fix the failure when it is in scope
- otherwise report the failure clearly in the final response

If a verification step cannot be run, state that explicitly and explain why.

## Hard cut and refactor policy

- This application currently has no external installed user base; optimize for one canonical current-state implementation, not compatibility with historical local states.
- Do not preserve or introduce compatibility bridges, migration shims, fallback paths, compact adapters, or dual behavior for old local states unless the user explicitly asks for that support.
- Prefer:
- one canonical current-state codepath
- fail-fast diagnostics
- explicit recovery steps over:
- automatic migration
- compatibility glue
- silent fallbacks
- "temporary" second paths
- If temporary migration or compatibility code is introduced for debugging or a narrowly scoped transition, it must be called out in the same diff with:
- why it exists
- why the canonical path is insufficient
- exact deletion criteria
- the ADR/task that tracks its removal
- Default stance across the app: delete old-state compatibility code rather than carrying it forward.

## Skills

### Available skills

- `better-result`: Use when migrating library code from thrown or rejected error flows to typed Result-based flows with better-result. (file: `/Users/andrewingram/Code/tupl/.agents/skills/better-result/SKILL.md`)
- `security-audit`: Findings-first security auditing for TypeScript or Node libraries and runtimes exposed to untrusted or public interfaces. (file: `/Users/andrewingram/Code/tupl/.agents/skills/security-audit/SKILL.md`)
- `typescript`: Use when changing TypeScript types, refactoring signatures, or reviewing typing style. Prefer inference for internal functions and locals; keep explicit return types only when they carry API intent or are required for correctness. (file: `/Users/andrewingram/Code/tupl/.agents/skills/typescript/SKILL.md`)
