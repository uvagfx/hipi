#!/bin/bash
ant -f ../build.xml seqfile
hadoop dfs -rm $HDFS_HOME/createsequencefile.jar
hadoop dfs -put createsequencefile.jar $HDFS_HOME/
hadoop jar createsequencefile.jar $1 $2
