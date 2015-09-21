#!/bin/bash
JAR=$1

if [ ! -f $JAR ];
then
   echo "Executable JAR [$JAR] does not exist."
   echo "Create by executing 'gradle jar' in current directory."
fi

hadoop jar "$@"