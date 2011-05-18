#!/bin/bash
ant -f ../build.xml dumphib
hadoop dfs -rm $HDFS_HOME/dumphib.jar
hadoop dfs -put dumphib.jar $HDFS_HOME/
hadoop jar dumphib.jar $1 $2
