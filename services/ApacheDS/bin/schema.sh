#!/bin/sh

# load system-wide Penrose configuration
if [ -f "/etc/penrose.conf" ] ; then
  . /etc/penrose.conf
fi

# load user Penrose configuration
if [ -f "$HOME/.penroserc" ] ; then
  . "$HOME/.penroserc"
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

if [ -z "$VD_SERVER_HOME" ] ; then
  # try to find PENROSE
  if [ -d /opt/penrose ] ; then
    VD_SERVER_HOME=/opt/penrose
  fi

  if [ -d "$HOME/opt/penrose" ] ; then
    VD_SERVER_HOME="$HOME/opt/penrose"
  fi

  ## resolve links - $0 may be a link to Penrose's home
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

  VD_SERVER_HOME=`dirname "$PRG"`/../../..

  cd "$saveddir"

  # make it fully qualified
  VD_SERVER_HOME=`cd "$VD_SERVER_HOME" && pwd`
fi

# For Cygwin, ensure paths are in UNIX format before anything is touched
if $cygwin ; then
  [ -n "$VD_SERVER_HOME" ] &&
    VD_SERVER_HOME=`cygpath --unix "$VD_SERVER_HOME"`
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

APACHEDS_HOME=$VD_SERVER_HOME/services/ApacheDS

LOCALLIBPATH="$JAVA_HOME/lib/ext:$JAVA_HOME/jre/lib/ext"
LOCALLIBPATH="$LOCALLIBPATH:$VD_SERVER_HOME/lib"
LOCALLIBPATH="$LOCALLIBPATH:$VD_SERVER_HOME/lib/ext"
LOCALLIBPATH="$LOCALLIBPATH:$VD_SERVER_HOME/server/lib"
LOCALLIBPATH="$LOCALLIBPATH:$APACHEDS_HOME/SERVICE-INF/lib"

# For Cygwin, switch paths to Windows format before running java
if $cygwin; then
  VD_SERVER_HOME=`cygpath --windows "$VD_SERVER_HOME"`
  APACHEDS_HOME=`cygpath --windows "$APACHEDS_HOME"`
  JAVA_HOME=`cygpath --windows "$JAVA_HOME"`
  LOCALLIBPATH=`cygpath --path --windows "$LOCALLIBPATH"`
fi

exec "$JAVACMD" $VD_SERVER_OPTS \
-Djava.ext.dirs="$LOCALLIBPATH" \
-Djava.library.path="$LOCALLIBPATH" \
-Dpenrose.home="$VD_SERVER_HOME" \
-Dorg.safehaus.penrose.apacheds.home="$APACHEDS_HOME" \
org.safehaus.penrose.apacheds.SchemaGenerator "$@"
