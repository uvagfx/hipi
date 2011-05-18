#!/bin/bash
ant -f ../build.xml hib2jpg
hadoop dfs -rm $HDFS_HOME/hib2jpg.jar
hadoop dfs -put hib2jpg.jar $HDFS_HOME/
hadoop jar hib2jpg.jar $1 $2 $3
