# AGENTS.md

## Verification

For any non-trivial code change, always run:

- `pnpm lint`
- `pnpm typecheck`
- `pnpm test`

Do this before finalizing the work unless the user explicitly asks not to.

## Reporting

If any verification step fails:

- fix the failure when it is in scope
- otherwise report the failure clearly in the final response

If a verification step cannot be run, state that explicitly and explain why.

## Skills

### Available skills

- `better-result`: Use when migrating library code from thrown or rejected error flows to typed Result-based flows with better-result. (file: `/Users/andrewingram/Code/sqlql/.agents/skills/better-result/SKILL.md`)
- `security-audit`: Findings-first security auditing for TypeScript or Node libraries and runtimes exposed to untrusted or public interfaces. (file: `/Users/andrewingram/Code/sqlql/.agents/skills/security-audit/SKILL.md`)
- `typescript`: Use when changing TypeScript types, refactoring signatures, or reviewing typing style. Prefer inference for internal functions and locals; keep explicit return types only when they carry API intent or are required for correctness. (file: `/Users/andrewingram/Code/sqlql/.agents/skills/typescript/SKILL.md`)
