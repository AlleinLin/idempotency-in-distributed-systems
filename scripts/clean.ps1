[CmdletBinding(SupportsShouldProcess)]
param()
$ErrorActionPreference = 'Stop'
$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$targets = @('book/build', '.ruff_cache', '.pytest_cache',
    'examples/chapter-01/__pycache__', 'examples/transactional/__pycache__',
    'examples/multipartition/__pycache__', 'examples/multipartition/authority.exe',
    'examples/multipartition/authority-server')
foreach ($relative in $targets) {
    $target = [IO.Path]::GetFullPath((Join-Path $repoRoot $relative))
    if (-not $target.StartsWith($repoRoot + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) {
        throw "Cleanup target is outside repository: $target"
    }
    if (Test-Path -LiteralPath $target) {
        $item = Get-Item -LiteralPath $target -Force
        if ($item.Attributes -band [IO.FileAttributes]::ReparsePoint) { throw "Refusing linked cleanup target: $target" }
        if ($PSCmdlet.ShouldProcess($target, 'Remove generated output')) {
            Remove-Item -LiteralPath $target -Recurse -Force
        }
    }
}
