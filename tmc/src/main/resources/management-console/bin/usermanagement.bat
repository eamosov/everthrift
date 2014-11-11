@echo off
setlocal
set main_class=com.terracotta.management.cli.auth.UserManagementCli
set cli_name=User Management Client

set root=%~d0%~p0
set root="%root:"=%"
rem pushd %root%

set CMD_LINE_ARGS=
:setArgs
 if ""%1""=="""" goto doneSetArgs
 set CMD_LINE_ARGS=%CMD_LINE_ARGS% %1
 shift
 goto setArgs
:doneSetArgs

call %root%management-cli-common.bat %CMD_LINE_ARGS%
rem popd
endlocal
