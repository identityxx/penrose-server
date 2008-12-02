@echo off
setlocal

set VD_SERVER_HOME=%~dp0..

set _WRAPPER_EXE=%VD_SERVER_HOME%\bin\wrapper.exe
set _WRAPPER_CONF="%VD_SERVER_HOME%\conf\wrapper.conf"

if "%1"=="install" goto install
if "%1"=="uninstall" goto uninstall

:help

echo Usage:
echo   vd-service.bat ^<install^|uninstall^>
goto end

:install

"%_WRAPPER_EXE%" -i %_WRAPPER_CONF%
if not errorlevel 1 goto end
pause
goto end

:uninstall

"%_WRAPPER_EXE%" -r %_WRAPPER_CONF%
if not errorlevel 1 goto end
pause
goto end

:end
endlocal