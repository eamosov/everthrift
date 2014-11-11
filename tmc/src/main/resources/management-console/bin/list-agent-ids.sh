#!/bin/bash

rest_client=`dirname $0`/rest-client.sh
tms_location=http://localhost:9889

function usage {
  echo "Usage: $0 [-l TMS URL] [-u username] [-p password] [-k]"
  echo "  -l specify the TMS location with no trailing \"/\" (defaults to ${tms_location})"
  echo "  -u specify username, only required if TMS has authentication enabled"
  echo "  -p specify password, only required if TMS has authentication enabled"
  echo "  -k ignore invalid SSL certificate"
  echo "  -h this help message"
  exit 1
}

while getopts l:u:p:a:kh opt
do
   case "${opt}" in
      l) tms_location=$OPTARG;;
      u) username=$OPTARG;;
      p) password=$OPTARG;;
      k) ignoreSslCert="-k";;
      h) usage & exit 0;;
      *) usage;;
   esac
done

${rest_client} ${ignoreSslCert} -e -g "${tms_location}/tmc/api/agents" "" "${username}" "${password}" '$.[?(@.agencyOf == 'TSA')].agentId'
exit $?