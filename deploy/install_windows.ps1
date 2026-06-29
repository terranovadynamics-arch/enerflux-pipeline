# ============================================================================
#  Deal Hunter - installation sur Windows (PowerShell)
#  Idempotent : peut etre relance sans danger.
#
#  Usage (PowerShell) :
#     powershell -ExecutionPolicy Bypass -File deploy\install_windows.ps1
#  ou, une fois le repo cloné, depuis le dossier du repo :
#     .\deploy\install_windows.ps1
# ============================================================================
$ErrorActionPreference = "Stop"

$AppDir = if ($env:APP_DIR) { $env:APP_DIR } else { "$env:USERPROFILE\dealhunter" }
$Repo   = "https://github.com/terranovadynamics-arch/enerflux-pipeline.git"
$Branch = "claude/deal-hunter-watch-agent-x3kt13"

Write-Host "==> Deal Hunter - installation dans $AppDir (branche $Branch)" -ForegroundColor Cyan

# --- Verifications outils ----------------------------------------------------
function Need($name, $hint) {
  if (-not (Get-Command $name -ErrorAction SilentlyContinue)) {
    Write-Host "MANQUANT: $name. Installez-le : $hint" -ForegroundColor Yellow
    exit 1
  }
}
Need "git"    "winget install Git.Git"
Need "python" "winget install Python.Python.3.12"

# --- Clone / mise a jour -----------------------------------------------------
if (Test-Path "$AppDir\.git") {
  Write-Host "==> Mise a jour du depot existant"
  git -C $AppDir fetch origin $Branch
  git -C $AppDir checkout $Branch
  git -C $AppDir pull origin $Branch
} else {
  Write-Host "==> Clonage du depot"
  git clone -b $Branch $Repo $AppDir
}

Set-Location $AppDir

# --- venv + dependances ------------------------------------------------------
Write-Host "==> Environnement Python"
python -m venv .venv
$py = ".\.venv\Scripts\python.exe"
& $py -m pip install --upgrade pip
& $py -m pip install -r requirements-dealhunter.txt

# --- .env --------------------------------------------------------------------
if (-not (Test-Path ".env")) {
  Copy-Item ".env.example" ".env"
  Write-Host "==> .env cree depuis le modele (A COMPLETER)"
}

Write-Host ""
Write-Host "============================================================" -ForegroundColor Green
Write-Host " Installation terminee dans $AppDir"
Write-Host ""
Write-Host " 1) Demo immediate (sans cle, resultats d'exemple) :"
Write-Host "      `$env:USE_FIXTURES='true'; `$env:NOTIFY_CHANNELS='console'; $py -m dealhunter --once"
Write-Host ""
Write-Host " 2) Editez vos secrets (cle eBay, SMTP) :"
Write-Host "      notepad .env"
Write-Host ""
Write-Host " 3) Cycle reel unique :"
Write-Host "      $py -m dealhunter --once"
Write-Host ""
Write-Host " 4) Mode non-stop :"
Write-Host "      $py -m dealhunter"
Write-Host "============================================================" -ForegroundColor Green
