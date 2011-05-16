#!/bin/bash
ant -f ../build.xml seqfile
hadoop dfs -rm /virginia/uvagfx/createsequencefile.jar
hadoop dfs -put createsequencefile.jar /virginia/uvagfx/
hadoop jar createsequencefile.jar $1 $2
