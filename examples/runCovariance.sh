#!/bin/bash
ant -f ../build.xml covariance
hadoop dfs -rm $HDFS_HOME/covariance.jar
hadoop dfs -put covariance.jar $HDFS_HOME/
hadoop jar covariance.jar $1 $2
