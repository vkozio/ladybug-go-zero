# Agent System Instructions

Persistent context for AI agents. Update when adding patterns/rules/context. Keep token-efficient; remove obsolete entries.

---

## Code Style and Architecture

**Principles:** KISS, SOLID, DRY+WET, YAGNI. Lean, pragmatic.

**Style:** Concise, technical. No business language/formality. Show relevant changes with minimal context. No testing/alternatives unless requested. Markdown formatting, mermaid+elk for diagrams.

---

## Project Context (ladybug-go-zero)

This repo is **Go bindings** for **Ladybug** (graph database). Unofficial driver: zero-copy Arrow, C API only at the boundary.

**Source of truth for binding design:** `.cursor/specs/go_binding_research.md` — C API summary, repo layout, CGO directives, delivery options, implementation sketch.

**Typical layout:**
- `include/lbug.h` — C header (from Ladybug releases or download script).
- `lib/dynamic/<platform>/` — shared libs (e.g. liblbug.so, liblbug.dylib).
- Go package(s) with `#cgo` in one place and wrappers for Database, Connection, PreparedStatement, QueryResult; row and Arrow result iteration.

**Upstream:** https://github.com/LadybugDB/ladybug (releases provide prebuilt libs and header). No need to clone it once you have `lbug.h` and the library.

---

## Documentation Rules

### New Features

When implementing a new feature, update in the same change (or immediately after):

- **README.md** — usage, compatibility (e.g. Ladybug version), how to get/update libs.
- **docs/** — if present: user-facing docs (e.g. configuration, best practices). Create `docs/` and files when needed.
- **Examples** — if you add `examples/` or sample code, document how to run it.

### Non-Obvious Bug Fixes

When fixing a non-obvious bug (CGO, lock/file, race, platform-specific):

1. Add an entry to **docs/developer/troubleshooting-and-gotchas.md** (create the file and `docs/developer/` if they do not exist).
2. Use template: **Symptom**, **Context**, **Root cause**, **Solution**, **How to recognize**.
3. Do this in the same change as the fix (or immediately after), so the experience is preserved and future agents or developers can find it by error message or component.

Do not skip this step: the doc exists so we don't lose this knowledge and can resolve similar issues faster later.

### Avoiding Duplication

When adding packages or Go code: check existing code first. Reuse existing helpers; do not copy-paste patterns that already exist in the repo. If the repo grows an `internal/` or shared layout later, keep a single place for shared logic (e.g. error handling, resource cleanup) and reference it instead of duplicating.

---

## Markdown Conventions

**Tables:** Use plain text only in cells. No backticks, no inline code. Keeps tables narrow and readable.

**Checkboxes:** Use GFM format `- [ ]` / `- [x]` in plans and checklists.

**Mermaid diagrams:** Use ELK layout when drawing flowcharts: start with `%%{init: {'flowchart': {'layout': 'elk'}}}%%`.

**Text encoding:** Use ASCII only for quotes and dashes: `"` not `"` or `"`, `'` not `'` or `'`, `-` not `–` or `—`. Replace Unicode smart quotes if found.

---

## Tool Usage

### Go (gopls MCP)

When **reading or editing** Go code, use the gopls MCP server if available so symbols and call sites are found reliably and edits are validated.

**Read workflow:** go_workspace (session start) → go_search for types/funcs → go_file_context after reading a file → go_package_api for public API of a package.

**Edit workflow:** Read first → **go_symbol_references** before changing any symbol definition → make edits → **go_diagnostics** after every edit (fix errors before tests) → go_vulncheck if go.mod changed → run tests for changed packages.

**Before first use:** Read the tool schema from the MCP config for required arguments (e.g. go_symbol_references needs `file` and `symbol`).

### Other

- **grep** for exact text or regex.
- **codebase_search** for semantic "how/where is X done" when the answer is not a single symbol name.
- **read_file** when the path is known.

---

## Quick Reference

**Before implementing:** Read `.cursor/specs/go_binding_research.md` for binding design. For Go: use gopls workflow; go_symbol_references before changing a symbol, go_diagnostics after edits.

**When fixing bugs:** If non-obvious, add an entry to docs/developer/troubleshooting-and-gotchas.md (create file if needed).

**When writing docs:** User docs in English. Tables: plain text in cells. ASCII quotes only.

**Commits:** One commit per logical unit (feature, fix, refactor). Include docs in the same commit as code when applicable.
