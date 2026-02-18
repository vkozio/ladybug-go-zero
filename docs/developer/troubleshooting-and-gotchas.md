# Troubleshooting and gotchas

## Windows: tests exit with 0xc0000135 (DLL not found)

**Symptom:** `go test ./...` on Windows exits with status 0xc0000135; build succeeds.

**Context:** CGO links the test binary against lbug_shared.dll. The DLL is placed in lib/dynamic/windows-amd64 by the download script.

**Root cause:** On Windows, the loader looks for DLLs in the executable's directory, then in directories on PATH. The test binary runs from a temporary directory under the build cache, so it does not see lib/dynamic/windows-amd64 unless that directory is on PATH.

**Solution:** Before running tests on Windows, add the library directory to PATH, e.g. in CI:

```yaml
- name: Add Windows DLL dir to PATH
  if: matrix.platform == 'windows-amd64'
  run: echo "PATH=${{ github.workspace }}\lib\dynamic\windows-amd64;$env:PATH" >> $env:GITHUB_ENV
  shell: pwsh
```

Locally: add the repo's lib/dynamic/windows-amd64 to your PATH, or run tests from the repo root after adding that path.

**How to recognize:** Exit code 0xc0000135 (STATUS_DLL_NOT_FOUND); Windows only; build succeeds, test step fails.
