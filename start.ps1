#requires -Version 7.0
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

function Invoke-Docker {
    param(
        [Parameter(Mandatory)]
        [string[]] $Arguments,
        [int] $TimeoutSeconds = 600,
        [switch] $Quiet
    )

    $startInfo = [System.Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = 'docker'
    $startInfo.WorkingDirectory = $root
    $startInfo.UseShellExecute = $false
    $startInfo.CreateNoWindow = $true
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    foreach ($argument in $Arguments) {
        [void] $startInfo.ArgumentList.Add($argument)
    }

    $process = [System.Diagnostics.Process]::new()
    $process.StartInfo = $startInfo
    if (-not $process.Start()) {
        throw '无法启动 Docker CLI'
    }

    $stdoutTask = $process.StandardOutput.ReadToEndAsync()
    $stderrTask = $process.StandardError.ReadToEndAsync()
    if (-not $process.WaitForExit($TimeoutSeconds * 1000)) {
        $process.Kill($true)
        $process.WaitForExit()
        throw "Docker 命令执行超过 ${TimeoutSeconds} 秒，已终止"
    }

    $stdout = $stdoutTask.GetAwaiter().GetResult().TrimEnd()
    $stderr = $stderrTask.GetAwaiter().GetResult().TrimEnd()
    if (-not $Quiet) {
        if ($stdout) { Write-Host $stdout }
        if ($stderr) { Write-Host $stderr -ForegroundColor DarkGray }
    }
    if ($process.ExitCode -ne 0) {
        throw "Docker 命令失败，退出码 $($process.ExitCode)"
    }
}

if (-not (Test-Path -LiteralPath '.env')) {
    Copy-Item -LiteralPath '.env.example' -Destination '.env'
    Write-Host '已生成 .env，请填写 AirOps key 后重跑。' -ForegroundColor Yellow
    Write-Host 'DONE'
    exit 0
}

$envText = [System.IO.File]::ReadAllText((Join-Path $root '.env'), [System.Text.Encoding]::UTF8)
if ($envText -notmatch '(?m)^\s*AIROPS_API_KEY(?:S)?\s*=\s*\S+') {
    throw 'AIROPS_API_KEY(S) 未设置，请编辑 .env'
}
$dockerPort = '8081'
$portMatch = [System.Text.RegularExpressions.Regex]::Match(
    $envText,
    '(?m)^\s*DOCKER_PORT\s*=\s*(\d+)\s*$'
)
if ($portMatch.Success) {
    $dockerPort = $portMatch.Groups[1].Value
}

Write-Host '[1/2] 检查 Docker Engine…' -ForegroundColor Cyan
try {
    Invoke-Docker -Arguments @('info') -TimeoutSeconds 20 -Quiet
} catch {
    Write-Host 'Docker Engine 尚未运行，正在启动 Docker Desktop…' -ForegroundColor Yellow
    Invoke-Docker -Arguments @('desktop', 'start', '--timeout', '120') -TimeoutSeconds 150
    Invoke-Docker -Arguments @('info') -TimeoutSeconds 30 -Quiet
}
Write-Host 'Docker Engine 已就绪' -ForegroundColor Green

Write-Host '[2/2] 构建并启动容器…' -ForegroundColor Cyan
Invoke-Docker -Arguments @('compose', 'up', '--detach', '--build') -TimeoutSeconds 900

Write-Host '服务已启动（可在 .env 中用 DOCKER_PORT 修改端口）' -ForegroundColor Green
Write-Host "管理面板 : http://127.0.0.1:${dockerPort}" -ForegroundColor Cyan
Write-Host "OpenAI API: http://127.0.0.1:${dockerPort}/v1" -ForegroundColor Cyan
Write-Host '停止服务 : docker compose down' -ForegroundColor DarkGray
Write-Host 'DONE'
exit 0
