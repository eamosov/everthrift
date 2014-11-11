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
mkdir -p logs

# look up license key
# first, assume it's inside tmc kit
license_key=$root/terracotta-license.key
if [ ! -f "$license_key" ]; then
  # if not, assume it's inside the ehcache-ee kit
  license_key=$root/../terracotta-license.key
fi

if [ ! -f "$license_key" ]; then
  # if still not found, assume it's inside the big memory kit
  license_key=$root/../../terracotta-license.key
fi

root_dir=$root
jetty_home=$root/jetty-distribution
jetty_start=$jetty_home/start.jar
stop_port=9887

edg_opt=
tmpdir_opt=

if $cygwin; then
  root_dir=`cygpath -w $root_dir`
  jetty_home=`cygpath -w $jetty_home`
  jetty_start=`cygpath -w $jetty_start`
  license_key=`cygpath -w $license_key`
else
  edg_opt=-Djava.security.egd=file:/dev/./urandom
fi

license_opt=
if [ -f "$license_key" ]; then
  license_opt=-Dcom.tc.productkey.path="$license_key"
fi

if [ `uname | grep Darwin` ]; then
  tmpdir_opt=-Djava.io.tmpdir=/tmp
fi

java_opts="$JAVA_OPTS -Xmx256m -XX:MaxPermSize=128m ${edg_opt} ${tmpdir_opt} ${license_opt} -DSTOP.PORT=$stop_port -DSTOP.KEY=secret"

# license check
echo License check
"$JAVA_HOME"/bin/java  $java_opts  -jar $root/lib/terracotta-license-1.1.0.jar TMC
RETVAL=$?
[ $RETVAL -ne 0 ] &&  exit $RETVAL

echo Starting Terracotta Management Console at http://localhost:9889/tmc
"$JAVA_HOME"/bin/java -Djetty.home="$jetty_home" -Droot.dir="$root_dir" \
 $java_opts \
 -jar "$jetty_start" \
 etc/jetty.xml \

