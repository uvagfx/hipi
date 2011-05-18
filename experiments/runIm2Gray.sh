#!/bin/bash
ant -f ../build.xml im2gray
hadoop dfs -rm $HDFS_HOME/im2gray.jar
hadoop dfs -put im2gray.jar $HDFS_HOME/
hadoop jar im2gray.jar $1 $2 $3
