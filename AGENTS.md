# Agent guidance

Standing guidance for coding agents and contributors working on easyraft.

## Commit messages

Use Conventional Commits: `type(scope): short imperative description`.

Examples of acceptable subjects:

- `feat(storage): persist vote and membership crash-safely`
- `fix(election): close stale prevote and durability holes`
- `test(storage): cover file log durability and recovery`
- `docs(storage): show how to open FileLogStorage`
- `chore: merge pull request #1 phase 1 raft core`

`type` is lowercase. Common types: `feat`, `fix`, `test`, `docs`, `build`, `refactor`, `chore`. Use a real module scope when one applies (for example `storage`, `raft`, `election`, `cmake`). Omit the scope only when the change is repo-wide.

The subject is English, imperative, and concise. Do not end the subject with a period. Add a body only when needed to explain why, not to repeat the diff.

Keep each commit small. Commit as soon as one small, complete unit of work is done. Do not bundle a whole feature into a single large commit. Each commit should compile and make sense on its own.

Do not rewrite `main` unless the owner explicitly asks to rewrite history.
