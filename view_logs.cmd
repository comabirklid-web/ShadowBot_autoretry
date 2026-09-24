@echo off
powershell.exe -NoExit -Command "$path = '%~dp0logs\retry_controller.log'; while ($true) { Clear-Host; if (Test-Path -LiteralPath $path) { Get-Content -LiteralPath $path -Encoding UTF8 -Tail 60 }; Start-Sleep -Seconds 1 }"
