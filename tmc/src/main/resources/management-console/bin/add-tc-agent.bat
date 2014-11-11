@echo off
setlocal
set argC=0
for %%x in (%*) do Set /A argC+=1

if %argC% NEQ 1 (
 :printhelp
  echo You need to provide 1 argument to this script : the URL of the REST agent you wish to add trust for - which should correspond exactly to the URL you provide for the connection within the TMC with no trailing "/".
  echo Example : add-tc-agent.sh http://localhost:9888 
  exit /b 1
)

if ""%1"" == ""-h"" goto printhelp

set url=%1
set main_class=com.terracotta.management.cli.keychain.KeyChainCli
set cli_name=Keychain Client

set root=%~d0%~p0
set root="%root:"=%"
pushd %root%

set CMD_LINE_ARGS=
:setArgs
 if ""%1""=="""" goto doneSetArgs
 set CMD_LINE_ARGS=%CMD_LINE_ARGS% %1
 shift
 goto setArgs
:doneSetArgs

set creation_flag=-c
set keychain_file=%USERPROFILE%\.tc\mgmt\keychain
IF EXIST "%keychain_file%" (
  set creation_flag=
)

call management-cli-common.bat -O -S %creation_flag% "%keychain_file%" %url%/tc-management-api/
popd
endlocal
