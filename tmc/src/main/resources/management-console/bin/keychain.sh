#!/bin/bash

export main_class="com.terracotta.management.cli.keychain.KeyChainCli"
export cli_name="Keychain Client"

unset CDPATH
root=`dirname $0`

$root/management-cli-common.sh -S  "$@"
