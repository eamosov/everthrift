#!/bin/bash

cygwin=false
if [ `uname | grep CYGWIN` ]; then
  cygwin=true
fi

if [ "$JAVA_HOME" = "" ]; then
  echo "JAVA_HOME is not defined"
  exit 1
fi

unset CDPATH
root=`dirname $0`/..
root=`cd $root && pwd`

cd $root

jetty_home=$root/jetty-distribution
jetty_start=$jetty_home/start.jar
stop_port=9887

if [ ! -f "${jetty_start}" ]; then
  echo "${jetty_start} not found"
  exit 1
fi

if $cygwin; then
  jetty_home=`cygpath -w $jetty_home`
  jetty_start=`cygpath -w $jetty_start`
fi

echo Stopping Terracotta Management Console
"$JAVA_HOME"/bin/java -Djetty.home=$"jetty_home" \
 -DSTOP.PORT=$stop_port \
 -DSTOP.KEY=secret \
 -jar "$jetty_start" \
 --stop > /dev/null 2>&1
exitcode=$?

sleep 1
if [ ${exitcode} -ne 0 ]; then
  echo Failed to stop Jetty. Did you have one running?
  exit 1
else
  echo Jetty stopped
fi