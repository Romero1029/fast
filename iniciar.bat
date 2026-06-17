@echo off
chcp 65001 > nul
echo.
echo  ================================
echo   Fast Escova - Painel Disparos
echo  ================================
echo.
echo  Instalando dependencias...
call npm install
echo.
echo  Iniciando servidor...
echo  Acesse: http://localhost:3000
echo.
node server.js
pause
