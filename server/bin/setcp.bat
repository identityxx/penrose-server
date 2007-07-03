set CLASSPATHCOMPONENT=%1
if ""%1""=="""" goto gotAllArgs
shift

:argCheck
if ""%1""=="""" goto gotAllArgs
set CLASSPATHCOMPONENT=%CLASSPATHCOMPONENT% %1
shift
goto argCheck

:gotAllArgs
set LOCALCLASSPATH=%LOCALCLASSPATH%;%CLASSPATHCOMPONENT%


