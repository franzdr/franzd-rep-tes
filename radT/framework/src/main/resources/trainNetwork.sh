#!/bin/bash

## Load includes
if [ "x$RADARGUN_HOME" = "x" ]; then DIRNAME=`dirname $0`; RADARGUN_HOME=`cd $DIRNAME/..; pwd` ; fi; export RADARGUN_HOME
. ${RADARGUN_HOME}/bin/includes.sh

### read in any command-line params
FILE=""
LR=" -lr 0.1"
MOM=" -m 0.01"
NORM=""
ERROR=" -e 0.05"

while ! [ -z $1 ]
do
  case "$1" in
    "-file")
      FILE=" -file $2"
      shift
      ;;
    "-lr")
      LR=" -lr $2"
      shift
      ;;
    "-mom")
      MOM=" -m $2"
      shift
      ;;
    "-e")
      ERROR=" -e $2"
      ;;
    "-norm")
      NORM=" -norm"
      ;;
    *)
      ;;
  esac
  shift
done

CONF=$FILE$LR$MOM$ERROR$NORM

add_fwk_to_classpath
set_env

java -classpath $CP org.radargun.LaunchNNTrainer ${CONF}


