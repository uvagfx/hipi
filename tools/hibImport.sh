#!/bin/bash
JAR=./hibImport/build/install/hibImport/lib/hibImport.jar

if [ ! -f $JAR ];
then
   echo "Executable JAR [$JAR] does not exist."
   echo "Create by executing 'gradle hibImport:installDist' in current working directory or 'gradle installDist' in parent tools directory."
fi

hadoop jar ./hibImport/build/install/hibImport/lib/hibImport.jar "$@"