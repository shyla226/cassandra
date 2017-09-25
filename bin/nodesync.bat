@REM
@REM  Copyright DataStax, Inc.
@REM
@REM  Please see the included license file for details.

@echo off
if "%OS%" == "Windows_NT" setlocal

pushd "%~dp0"
call cassandra.in.bat

if NOT DEFINED CASSANDRA_MAIN set CASSANDRA_MAIN=com.datastax.apollo.tools.NodeSync
if NOT DEFINED JAVA_HOME goto :err

REM ***** JAVA options *****
set JAVA_OPTS=^
 -Dlogback.configurationFile=logback-tools.xml

set TOOLS_PARAMS=

"%JAVA_HOME%\bin\java" %JAVA_OPTS% %CASSANDRA_PARAMS% -cp %CASSANDRA_CLASSPATH% "%CASSANDRA_MAIN%" %*
goto finally

:err
echo JAVA_HOME environment variable must be set!
pause

:finally

ENDLOCAL
