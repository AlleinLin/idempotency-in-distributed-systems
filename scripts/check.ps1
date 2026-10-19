$ErrorActionPreference = 'Stop'
Push-Location (Join-Path $PSScriptRoot '..')
try {
    & python -m ruff format --check examples
    if ($LASTEXITCODE -ne 0) { throw 'Python formatting check failed.' }
    & python -m ruff check examples
    if ($LASTEXITCODE -ne 0) { throw 'Python lint check failed.' }
    Push-Location 'examples/multipartition/authority'
    try {
        $unformatted = & gofmt -l .
        if ($LASTEXITCODE -ne 0 -or $unformatted) { throw "Run gofmt on: $unformatted" }
        & go test ./...
        if ($LASTEXITCODE -ne 0) { throw 'Go tests failed.' }
    } finally { Pop-Location }
} finally { Pop-Location }
