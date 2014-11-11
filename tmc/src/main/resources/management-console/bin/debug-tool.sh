#!/bin/bash

rest_client=`dirname $0`/rest-client.sh
tms_location=http://localhost:9889

function usage {
  echo "Usage: $0 [-l TMS URL] [-u username] [-p password] [-a agentId] [-k] [-z]"
  echo "  -l specify the TMS location with no trailing \"/\" (defaults to ${tms_location})"
  echo "  -u specify username, only required if TMS has authentication enabled"
  echo "  -p specify password, only required if TMS has authentication enabled"
  echo "  -a specify agent ID to get the thread dump from. If not set, a list of agent IDs configured in the TMS will be returned"
  echo "  -z create a ZIP file with the result instead of displaying it"
  echo "  -k ignore invalid SSL certificate"
  echo "  -h this help message"
  exit 1
}

while getopts l:u:p:a:zkh opt
do
   case "${opt}" in
      l) tms_location=$OPTARG;;
      u) username=$OPTARG;;
      p) password=$OPTARG;;
      a) agentId=$OPTARG;;
      z) doZip="-z";;
      k) ignoreSslCert="-k";;
      h) usage & exit 0;;
      *) usage;;
   esac
done

if [[ "${agentId}" == "" ]]; then
  echo "Missing agent ID, available IDs:"
  exec `dirname $0`/list-agent-ids.sh ${ignoreSslCert} -u "${username}" -p "${password}" -l "${tms_location}"
fi

echo "Getting cluster thread dump of ${agentId} ..."
if [[ "${doZip}" == "" ]]; then
  ${rest_client} ${ignoreSslCert} -g "${tms_location}/tmc/api/agents;ids=${agentId}/diagnostics/threadDump" "" "${username}" "${password}" '$.[*].dump'
else
  if ! ${rest_client} ${ignoreSslCert} -e -f -g "${tms_location}/tmc/api/agents" "" "${username}" "${password}" '$.[?(@.agencyOf == 'TSA')].[?(@.agentId == '${agentId}')].agentId' &> /dev/null ; then
    echo "Invalid agent ID, available IDs:"
    exec `dirname $0`/list-agent-ids.sh ${ignoreSslCert} -u "${username}" -p "${password}" -l "${tms_location}"
  fi
  ${rest_client} ${ignoreSslCert} -g "${tms_location}/tmc/api/agents;ids=${agentId}/diagnostics/threadDumpArchive" "" "${username}" "${password}" > ${agentId}-ThreadDump.zip
fi
exit $?
