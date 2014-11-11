@echo off

REM This script can not run on its own, it must be invoked by usermanagement.bat or keychain.bat
if not defined cli_name (
  echo This script cannot be called directly, you should use usermanagement.bat or keychain.bat
  exit /b 1
)


if not defined JAVA_HOME (
  echo JAVA_HOME is not defined
  exit /b 1
)

set JAVA_HOME="%JAVA_HOME:"=%"
set root=%~d0%~p0
set root="%root:"=%"

rem cd %root%

set cli_home=%root%
set cli_runner="%cli_home:"=%..\lib\management-cli-1.2.2.jar"
IF NOT EXIST %cli_runner% (
   set cli_runner="%cli_home:"=%..\lib\management-cli\management-cli-1.2.2.jar"
)

set CMD_LINE_ARGS=
:setArgs
 if ""%1""=="""" goto doneSetArgs
 set CMD_LINE_ARGS=%CMD_LINE_ARGS% %1
 shift
 goto setArgs
:doneSetArgs

set java_opts=
if not "%cli_name%"=="__hidden__" (
  echo Terracotta Command Line Tools %cli_name:"=%
)
%JAVA_HOME%\bin\java -Xmx256m -XX:MaxPermSize=128m ^
 %java_opts% ^
 -cp %cli_runner% %main_class:"=% %CMD_LINE_ARGS%

exit /B
