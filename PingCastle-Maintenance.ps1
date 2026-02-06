<#
PingCastle Database Maintenance
AUTHOR = Karim AZZOUZI
VENDOR = Netwrix Corporation
#>

[CmdletBinding()]
param(
  [string]$Python = "py",
  [string]$ScriptName = "pingcastle_maintenance.py",
  [string]$ExeName = "PingCastleMaintenance.exe"
)

$ErrorActionPreference = "Stop"

function Write-Title {
  Write-Host ""
  Write-Host "=========================================" -ForegroundColor Cyan
  Write-Host " PingCastle Database Maintenance" -ForegroundColor Cyan
  Write-Host " AUTHOR = Karim AZZOUZI | VENDOR = Netwrix Corporation" -ForegroundColor DarkGray
  Write-Host "=========================================" -ForegroundColor Cyan
  Write-Host ""
}

function Try-Run-ExeFallback {
  param(
    [Parameter(Mandatory=$true)][string]$Here,
    [Parameter(Mandatory=$true)][string]$ExeName
  )

  $exePath = Join-Path $Here $ExeName
  if (Test-Path $exePath) {
    Write-Host "Python not found. Running EXE fallback: $exePath" -ForegroundColor Yellow
    & $exePath
    if ($LASTEXITCODE -ne 0) { throw "EXE returned exit code: $LASTEXITCODE" }
    return $true
  }
  return $false
}

function Ensure-Python {
  param([string]$PythonCmd)

  try { & $PythonCmd --version | Out-Null }
  catch { throw "Python not found. Install Python 3.x or change -Python parameter." }
}

function Ensure-Venv {
  param(
    [Parameter(Mandatory=$true)][string]$PythonCmd,
    [Parameter(Mandatory=$true)][string]$VenvPath
  )

  if (-not (Test-Path $VenvPath)) {
    Write-Host "Creating venv: $VenvPath" -ForegroundColor Cyan

    # py launcher supports "-3", python.exe does NOT.
    if ($PythonCmd -match '^(py|py\.exe)$') {
      & $PythonCmd -3 -m venv $VenvPath
    } else {
      & $PythonCmd -m venv $VenvPath
    }
  }
}

function Get-VenvPython {
  param([Parameter(Mandatory=$true)][string]$VenvPath)

  $py = Join-Path $VenvPath "Scripts\python.exe"
  if (-not (Test-Path $py)) { throw "Venv python not found: $py" }
  return $py
}

function Show-OdbcDrivers {
  Write-Host ""
  Write-Host "ODBC drivers (SQL Server) detected:" -ForegroundColor Cyan
  try {
    $drivers = (Get-OdbcDriver -ErrorAction Stop | Where-Object { $_.Name -match "SQL Server|ODBC Driver|Native Client" }).Name
    if ($drivers) {
      $drivers | ForEach-Object { Write-Host " - $_" -ForegroundColor DarkGray }
    } else {
      Write-Host " - (none found)" -ForegroundColor Yellow
    }
  } catch {
    Write-Host " - Unable to list ODBC drivers (Get-OdbcDriver not available?)." -ForegroundColor Yellow
  }
  Write-Host ""
}

function Install-Requirements {
  param([Parameter(Mandatory=$true)][string]$VenvPython)

  Write-Host "Upgrading pip..." -ForegroundColor Cyan
  & $VenvPython -m pip install --upgrade pip | Out-Null

  Write-Host "Installing dependencies (rich, pyodbc)..." -ForegroundColor Cyan
  & $VenvPython -m pip install --upgrade rich pyodbc | Out-Null
}

function Run-Maintenance {
  param(
    [Parameter(Mandatory=$true)][string]$VenvPython,
    [Parameter(Mandatory=$true)][string]$ScriptPath
  )

  if (-not (Test-Path $ScriptPath)) { throw "Python script not found: $ScriptPath" }

  Write-Host ""
  Write-Host "Launching maintenance wizard..." -ForegroundColor Green
  Write-Host ""

  & $VenvPython $ScriptPath
  if ($LASTEXITCODE -ne 0) {
    throw "Maintenance returned exit code: $LASTEXITCODE"
  }
}

Write-Title

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$scriptPath = Join-Path $here $ScriptName
$venvPath = Join-Path $here ".venv"

# If python missing => try EXE fallback
try {
  Ensure-Python -PythonCmd $Python
} catch {
  if (Try-Run-ExeFallback -Here $here -ExeName $ExeName) { exit 0 }
  throw
}

Show-OdbcDrivers

Ensure-Venv -PythonCmd $Python -VenvPath $venvPath
$venvPy = Get-VenvPython -VenvPath $venvPath

Install-Requirements -VenvPython $venvPy
Run-Maintenance -VenvPython $venvPy -ScriptPath $scriptPath
