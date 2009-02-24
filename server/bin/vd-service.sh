#!/bin/sh

if [ -f "/etc/vd.conf" ] ; then
  . /etc/vd.conf
fi

cygwin=false;
darwin=false;
case "`uname`" in
  CYGWIN*) cygwin=true ;;
  Darwin*) darwin=true ;;
esac

if [ -z "$VD_SERVER_HOME" ] ; then

  PRG="$0"
  progname=`basename "$0"`
  saveddir=`pwd`

  dirname_prg=`dirname "$PRG"`
  cd "$dirname_prg"

  while [ -h "$PRG" ] ; do
    ls=`ls -ld "$PRG"`
    link=`expr "$ls" : '.*-> \(.*\)$'`
    if expr "$link" : '.*/.*' > /dev/null; then
	PRG="$link"
    else
	PRG=`dirname "$PRG"`"/$link"
    fi
  done

  VD_SERVER_HOME=`dirname "$PRG"`/..

  cd "$saveddir"

  VD_SERVER_HOME=`cd "$VD_SERVER_HOME" && pwd`
fi

cd "$VD_SERVER_HOME"

if [ "$1" = "install" ] ; then

  cp etc/init.d/vd-server /etc/init.d
  cp etc/init.d/vd-monitor /etc/init.d

elif [ "$1" = "uninstall" ] ; then

  rm /etc/init.d/vd-server
  rm /etc/init.d/vd-monitor

else

  echo "Usage:"
  echo "  vd-service.sh <install|uninstall>"
  
fi

