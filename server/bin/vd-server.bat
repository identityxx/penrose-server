@echo off
setlocal

set VD_SERVER_HOME=%~dp0..

set CMD_LINE_ARGS=%1
if ""%1""=="""" goto doneStart
shift

:setupArgs

if ""%1""=="""" goto doneStart
set CMD_LINE_ARGS=%CMD_LINE_ARGS% %1
shift
goto setupArgs

:doneStart

set _JAVACMD=%JAVACMD%

if "%JAVA_HOME%" == "" goto noJavaHome
if not exist "%JAVA_HOME%\bin\java.exe" goto noJavaHome
if "%_JAVACMD%" == "" set _JAVACMD=%JAVA_HOME%\bin\java.exe
goto run

:noJavaHome
if "%_JAVACMD%" == "" set _JAVACMD=java.exe
echo Error: JAVA_HOME is not defined.
goto end

:run

set LOCALLIBPATH=%JAVA_HOME%\lib\ext;%JAVA_HOME%\jre\lib\ext
set LOCALLIBPATH=%LOCALLIBPATH%;%VD_SERVER_HOME%\lib
set LOCALLIBPATH=%LOCALLIBPATH%;%VD_SERVER_HOME%\lib\ext
set LOCALLIBPATH=%LOCALLIBPATH%;%VD_SERVER_HOME%\server\lib
set LOCALLIBPATH=%LOCALLIBPATH%;%VD_SERVER_HOME%\server\lib\ext

cd %VD_SERVER_HOME%

set JAVA_OPTS=%VD_SERVER_OPTS% -Dcom.sun.management.jmxremote -Djava.ext.dirs="%LOCALLIBPATH%" -Djava.library.path="%LOCALLIBPATH%" -Dpenrose.home="%VD_SERVER_HOME%"
"%_JAVACMD%" %JAVA_OPTS% org.safehaus.penrose.server.PenroseServer %CMD_LINE_ARGS%

:end
endlocal
