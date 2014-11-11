@echo off
SETLOCAL

SET root=%~d0%~p0
SET root="%root:"=%"

SET USERNAME=""
SET PASSWORD=""
SET TMS_LOCATION=http://localhost:9889
SET IGNORE_SSL_CERT=

:PARSE_ARGS_LOOP
IF '%1'=='' (  GOTO PARSE_ARGS_END
) ELSE IF '%1'=='-u' ( SHIFT & set USERNAME=%2
) ELSE IF '%1'=='-p' ( SHIFT & set PASSWORD=%2
) ELSE IF '%1'=='-l' ( SHIFT & set TMS_LOCATION=%2
) ELSE IF '%1'=='-k' ( set IGNORE_SSL_CERT=-k
) ELSE (
  ECHO Usage: %0 [-l TMS URL] [-u username] [-p password] [-k]
  ECHO   -l specify the TMS location with no trailing "/", defaults to %TMS_LOCATION%
  ECHO   -u specify username, only required if TMS has authentication enabled 
  ECHO   -p specify password, only required if TMS has authentication enabled 
  ECHO   -k ignore invalid SSL certificate 
  ECHO   -h this help message
  GOTO:EOF
)
SHIFT
GOTO PARSE_ARGS_LOOP
:PARSE_ARGS_END

CALL %root%rest-client.bat %IGNORE_SSL_CERT% -e -g %TMS_LOCATION%/tmc/api/agents "" %USERNAME% %PASSWORD% "$.[?(@.agencyOf == 'TSA')].agentId"

