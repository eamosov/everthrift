#!/bin/bash

export main_class="com.terracotta.management.cli.auth.UserManagementCli"
export cli_name="User Management Client"
unset CDPATH
root=`dirname $0`

$root/management-cli-common.sh "$@"
