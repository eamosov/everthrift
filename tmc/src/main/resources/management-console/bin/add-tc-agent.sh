#!/bin/bash
if [ $# != 1 ] || [ $1 == "-h" ]; then
  echo "You need to provide 1 argument to this script : the URL of the REST agent you wish to add trust for - which should correspond exactly to the URL you provide for the connection within the TMC (with no trailing "/")."
  echo "Example : add-tc-agent.sh http://localhost:9888" 
  exit 1
fi

url=$1

export main_class="com.terracotta.management.cli.keychain.KeyChainCli"
export cli_name="Keychain Client"

unset CDPATH
root=`dirname $0`
root=`cd $root && pwd`

cd $root

keychain_file=~/.tc/mgmt/keychain
creation_flag=-c
if [ -f $keychain_file ]
then
    creation_flag=
fi

$root/management-cli-common.sh  -O -S $creation_flag $keychain_file $url/tc-management-api/
