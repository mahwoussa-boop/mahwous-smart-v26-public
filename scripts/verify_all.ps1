# نفس فحوصات CI محلياً (من جذر المشروع حيث يقع app.py)
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot\..
python -m pip install -q -r requirements.txt
python -m compileall -q .
python -m unittest discover -s tests -v
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
Write-Host "OK: compileall + unittest"
