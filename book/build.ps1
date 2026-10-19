$ErrorActionPreference = 'Stop'
if (-not (Get-Command xelatex -ErrorAction SilentlyContinue)) {
    throw 'Install TeX Live or MiKTeX with Chinese support and add xelatex to PATH.'
}
Push-Location $PSScriptRoot
try {
    New-Item -ItemType Directory -Force -Path 'build' | Out-Null
    for ($pass = 1; $pass -le 4; $pass++) {
        & xelatex '-interaction=nonstopmode' '-halt-on-error' '-file-line-error' '-synctex=1' '-output-directory=build' 'main.tex'
        if ($LASTEXITCODE -ne 0) { throw "XeLaTeX failed on pass $pass. See book/build/main.log." }
        $log = Get-Content -LiteralPath 'build/main.log' -Raw
        $needsRerun = $log -match 'Rerun to get|Please rerun|Rerun LaTeX|Label\(s\) may have changed'
        if ($pass -ge 2 -and -not $needsRerun) { break }
    }
    if ($needsRerun) { throw 'References did not stabilize after four passes. See book/build/main.log.' }
    Write-Output (Join-Path $PSScriptRoot 'build/main.pdf')
} finally {
    Pop-Location
}
