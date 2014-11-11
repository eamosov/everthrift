@echo off

if not defined JAVA_HOME (
  echo JAVA_HOME is not defined
  exit /b 1
)

setlocal

set JAVA_HOME="%JAVA_HOME:"=%"
set root=%~d0%~p0..
set root="%root:"=%"

cd %root%

set jetty_home=%root%\jetty-distribution
set jetty_start=%jetty_home%\start.jar
set stop_port=9887

if not exist %jetty_start% (
  echo %jetty_start% not found
  exit /b 1
)

echo Stopping Terracotta Management Console
%JAVA_HOME%\bin\java -Djetty.home=%jetty_home% ^
 -DSTOP.PORT=%stop_port% ^
 -DSTOP.KEY=secret ^
 -jar %jetty_start% ^
 --stop >NUL 2>&1

if not %ERRORLEVEL%==0 (
  echo Failed to stop Jetty. Did you have one running?
  exit /b 1
) else (
  echo Jetty stopped
)
endlocal