@echo off
setlocal

set VD_SERVER_HOME=%~dp0..

set WRAPPER_EXE=%VD_SERVER_HOME%\bin\wrapper.exe
set WRAPPER_CONF="%VD_SERVER_HOME%\conf\wrapper.conf"

if "%1"=="install" goto install
if "%1"=="uninstall" goto uninstall

:help

echo Usage:
echo   vd-service.bat ^<install^|uninstall^>
goto end

:install

"%WRAPPER_EXE%" -i %WRAPPER_CONF%
if not errorlevel 1 goto end
pause
goto end

:uninstall

"%WRAPPER_EXE%" -r %WRAPPER_CONF%
if not errorlevel 1 goto end
pause
goto end

:end
endlocal