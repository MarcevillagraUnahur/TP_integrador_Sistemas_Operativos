@echo off
set /a NUM=%RANDOM% %% 90 + 10
title Terminal POS-%NUM%
echo =========================================================================
echo Abriendo Nueva Terminal POS-%NUM% en WSL (Ubuntu .NET 8)...
echo Al abrirse elija el Rubro Comercial para comenzar a cobrar.
echo =========================================================================
wsl -e bash -c "export TZ='America/Argentina/Buenos_Aires' && cd '/mnt/f/UNAHUR/SISTEMAS_OPERATIVOS/TP Integrador/ClientePOS' && dotnet run -- POS-%NUM%"
pause
