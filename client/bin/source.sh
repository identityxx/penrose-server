#!/bin/sh

# load system-wide configuration
if [ -f "/etc/vd.conf" ] ; then
  . /etc/vd.conf
fi

# OS specific support.  $var _must_ be set to either true or false.
cygwin=false;
darwin=false;
case "`uname`" in
  CYGWIN*) cygwin=true ;;
  Darwin*) darwin=true
           if [ -z "$JAVA_HOME" ] ; then
             JAVA_HOME=/System/Library/Frameworks/JavaVM.framework/Home
           fi
           ;;
esac

if [ -z "$VD_CLIENT_HOME" ] ; then

  # if [ -d /opt/vd-client ] ; then
  #   VD_CLIENT_HOME=/opt/vd-client
  # fi

  # if [ -d "$HOME/opt/vd-client" ] ; then
  #   VD_CLIENT_HOME="$HOME/opt/vd-client"
  # fi

  PRG="$0"
  progname=`basename "$0"`
  saveddir=`pwd`

  # need this for relative symlinks
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

  VD_CLIENT_HOME=`dirname "$PRG"`/..

  cd "$saveddir"

  # make it fully qualified
  VD_CLIENT_HOME=`cd "$VD_CLIENT_HOME" && pwd`
fi

# For Cygwin, ensure paths are in UNIX format before anything is touched
if $cygwin ; then
  [ -n "$VD_CLIENT_HOME" ] &&
    VD_CLIENT_HOME=`cygpath --unix "$VD_CLIENT_HOME"`
  [ -n "$JAVA_HOME" ] &&
    JAVA_HOME=`cygpath --unix "$JAVA_HOME"`
fi

if [ -z "$JAVACMD" ] ; then
  if [ -n "$JAVA_HOME"  ] ; then
    if [ -x "$JAVA_HOME/jre/sh/java" ] ; then
      # IBM's JDK on AIX uses strange locations for the executables
      JAVACMD="$JAVA_HOME/jre/sh/java"
    else
      JAVACMD="$JAVA_HOME/bin/java"
    fi
  else
    JAVACMD=`which java 2> /dev/null `
    if [ -z "$JAVACMD" ] ; then
        JAVACMD=java
    fi
  fi
fi

if [ ! -x "$JAVACMD" ] ; then
  echo "Error: JAVA_HOME is not defined correctly."
  echo "  We cannot execute $JAVACMD"
  exit 1
fi

LOCALLIBPATH="$JAVA_HOME/jre/lib/ext"
LOCALLIBPATH="$LOCALLIBPATH:$VD_CLIENT_HOME/lib"
LOCALLIBPATH="$LOCALLIBPATH:$VD_CLIENT_HOME/lib/ext"

# For Cygwin, switch paths to Windows format before running java
if $cygwin; then
  VD_CLIENT_HOME=`cygpath --windows "$VD_CLIENT_HOME"`
  JAVA_HOME=`cygpath --windows "$JAVA_HOME"`
  LOCALLIBPATH=`cygpath --path --windows "$LOCALLIBPATH"`
fi

exec "$JAVACMD" $VD_SERVER_OPTS \
-Djava.ext.dirs="$LOCALLIBPATH" \
-Djava.library.path="$LOCALLIBPATH" \
-Dorg.safehaus.penrose.client.home="$VD_CLIENT_HOME" \
org.safehaus.penrose.source.SourceManagerClient "$@"
