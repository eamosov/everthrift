@echo off
setlocal ENABLEDELAYEDEXPANSION
set main_class=com.terracotta.management.cli.rest.RestCli
set cli_name=__hidden__

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

call set CMD_LINE_ARGS=!CMD_LINE_ARGS:%%=%%%%%%%%!
call %root%management-cli-common.bat %CMD_LINE_ARGS%
rem popd
endlocal
