#!/bin/bash
ant -f ../build.xml hib2jpg
hadoop dfs -rm /virginia/uvagfx/hib2jpg.jar
hadoop dfs -put hib2jpg.jar /virginia/uvagfx/
hadoop jar hib2jpg.jar $1 $2 $3
