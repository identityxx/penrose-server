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

if [ -z "$PENROSE_HOME" ] ; then
  # try to find PENROSE
  if [ -d /opt/penrose ] ; then
    PENROSE_HOME=/opt/penrose
  fi

  if [ -d "$HOME/opt/penrose" ] ; then
    PENROSE_HOME="$HOME/opt/penrose"
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

  PENROSE_HOME=`dirname "$PRG"`/..

  cd "$saveddir"

  # make it fully qualified
  PENROSE_HOME=`cd "$PENROSE_HOME" && pwd`
fi

# For Cygwin, ensure paths are in UNIX format before anything is touched
if $cygwin ; then
  [ -n "$PENROSE_HOME" ] &&
    PENROSE_HOME=`cygpath --unix "$PENROSE_HOME"`
  [ -n "$JAVA_HOME" ] &&
    JAVA_HOME=`cygpath --unix "$JAVA_HOME"`
  [ -n "$CLASSPATH" ] &&
    CLASSPATH=`cygpath --path --unix "$CLASSPATH"`
fi

# set PENROSE_LIB location
PENROSE_LIB="$PENROSE_HOME/lib"

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

if [ -n "$CLASSPATH" ] ; then
  LOCALCLASSPATH="$CLASSPATH"
fi

# add in the required dependency .jar files
for i in "$PENROSE_LIB"/*.jar
do
  # if the directory is empty, then it will return the input string
  # this is stupid, so case for it
  if [ -f "$i" ] ; then
    if [ -z "$LOCALCLASSPATH" ] ; then
      LOCALCLASSPATH="$i"
    else
      LOCALCLASSPATH="$i":"$LOCALCLASSPATH"
    fi
  fi
done

LOCALCLASSPATH="$PENROSE_HOME/conf:$LOCALCLASSPATH"

# For Cygwin, switch paths to Windows format before running java
if $cygwin; then
  PENROSE_HOME=`cygpath --windows "$PENROSE_HOME"`
  JAVA_HOME=`cygpath --windows "$JAVA_HOME"`
  CLASSPATH=`cygpath --path --windows "$CLASSPATH"`
  LOCALCLASSPATH=`cygpath --path --windows "$LOCALCLASSPATH"`
  CYGHOME=`cygpath --windows "$HOME"`
fi

cd "$PENROSE_HOME"
mkdir -p "$PENROSE_HOME/var"
PENROSE_PID="$PENROSE_HOME/var/penrose.pid"

if [ "$1" = "start" ] ; then

  if [ -n "$CYGHOME" ]; then
    exec "$JAVACMD" $PENROSE_DEBUG_OPTS $PENROSE_OPTS -classpath "$LOCALCLASSPATH" -Dpenrose.home="$PENROSE_HOME" -Dcygwin.user.home="$CYGHOME" org.apache.ldap.server.ServerMain "$PENROSE_HOME\\conf\\apacheds.properties" $PENROSE_ARGS "$@" >> "$PENROSE_HOME/var/penrose.out" 2>&1 &
  else
    exec "$JAVACMD" $PENROSE_DEBUG_OPTS $PENROSE_OPTS -classpath "$LOCALCLASSPATH" -Dpenrose.home="$PENROSE_HOME" org.apache.ldap.server.ServerMain "$PENROSE_HOME/conf/apacheds.properties" $PENROSE_ARGS "$@" >> "$PENROSE_HOME/var/penrose.out" 2>&1 &
  fi

  echo $! > "$PENROSE_PID"

elif [ "$1" = "stop" ] ; then

  kill -9 `cat "$PENROSE_PID"` > /dev/null 2>&1

else

  echo "Usage: penrose.sh COMMAND"
  echo
  echo "Commands:"
  echo "  start             Start Penrose Server"
  echo "  stop              Stop Penrose Server"
  exit 1

fi
