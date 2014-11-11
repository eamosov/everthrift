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
set root="%CD:"=%"
if not exist logs mkdir logs

rem look up license key
rem first, assume it's inside tmc kit
set license_key=%root%\terracotta-license.key
if not exist %license_key% (
  rem if not, assume it's inside the ehcache-ee kit
  set license_key="..\terracotta-license.key"
)

if not exist %license_key% (
  rem if still not found, assume it's inside the big memory kit
  set license_key="..\..\terracotta-license.key"
)

set root_dir=%root%
set jetty_home=%root%\jetty-distribution
set jetty_start=%jetty_home%\start.jar
set stop_port=9887

set license_opt=
if exist "%license_key%" (
  set license_opt=-Dcom.tc.productkey.path=%license_key%
)

set java_opts=%JAVA_OPTS% -Xmx256m -XX:MaxPermSize=128m %license_opt% -DSTOP.PORT=%stop_port% -DSTOP.KEY=secret

rem license check
echo License check
echo.

%JAVA_HOME%\bin\java  %java_opts%  -jar %root%\lib\terracotta-license-1.1.0.jar TMC
IF %ERRORLEVEL% NEQ 0 (
  exit /b %ERRORLEVEL%
)

echo Starting Terracotta Management Console at http://localhost:9889/tmc
echo.

%JAVA_HOME%\bin\java -Djetty.home=%jetty_home% -Droot.dir=%root_dir% ^
 %java_opts% ^
 -jar %jetty_start% ^
 etc\jetty.xml

exit /b %ERRORLEVEL%
endlocal
