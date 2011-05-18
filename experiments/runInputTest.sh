#!/bin/bash
ant -f ../build.xml inputtest
hadoop dfs -rm $HDFS_HOME/inputtest.jar
hadoop dfs -put inputtest.jar $HDFS_HOME/
hadoop jar inputtest.jar $1 $2
