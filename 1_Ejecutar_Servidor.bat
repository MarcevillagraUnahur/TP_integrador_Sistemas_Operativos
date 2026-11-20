@echo off
title Servidor Autorizador de Tarjetas POS (Central)
echo =========================================================================
echo Iniciando Servidor Autorizador de Tarjetas y GUI Web en WSL (.NET 8)...
echo Panel Web disponible en: http://localhost:8080
echo =========================================================================
wsl -e bash -c "killall -9 ServidorPOS 2>/dev/null || true"
start "" cmd /c "timeout /t 4 >nul & start http://localhost:8080"
wsl -e bash -c "export TZ='America/Argentina/Buenos_Aires' && cd '/mnt/f/UNAHUR/SISTEMAS_OPERATIVOS/TP Integrador/ServidorPOS' && dotnet run"
pause
