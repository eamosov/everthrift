#!/bin/bash
#This script can not run on its own, it must be invoked by usermanagement.sh, keychain.sh or rest-client.sh
if [ "$cli_name" = "" ]; then
  echo "This script cannot be called directly, you should use usermanagement.sh, keychain.sh or rest-client.sh"
  exit 1
fi

#cygwin=false
if [ `uname | grep CYGWIN` ]; then
 # cygwin=true
  echo "cygwin shell is not supported, please use the corresponding .bat script "
  echo "(for example keychain.bat in place of keychain.sh) from a Windows cmd.exe terminal."
  exit 1
fi

if [ "$JAVA_HOME" = "" ]; then
  echo "JAVA_HOME is not defined"
  exit 1
fi

unset CDPATH
root=`dirname $0`/..
cli_runner=$root/lib/management-cli-1.2.2.jar
#support for terracotta kit
if [ ! -f $cli_runner ]
then
    cli_runner=$root/lib/management-cli/management-cli-1.2.2.jar
fi
edg_opt=
tmpdir_opt=

#if $cygwin; then
#  cli_runner=`cygpath -w $cli_runner`
#fi

#if [ `uname | grep Darwin` ]; then
#  tmpdir_opt=-Djava.io.tmpdir=/tmp
#fi

java_opts=""

if [[ "${cli_name}" != "__hidden__" ]]; then
  echo Terracotta Command Line Tools - $cli_name
fi

"$JAVA_HOME"/bin/java -Xmx256m -XX:MaxPermSize=128m \
 $java_opts \
 -cp $cli_runner $main_class "$@"
exit_code=$?
#sleep 1
echo
exit $exit_code