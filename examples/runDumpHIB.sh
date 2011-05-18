#!/bin/bash
ant -f ../build.xml dumphib
hadoop dfs -rm /virginia/uvagfx/dumphib.jar
hadoop dfs -put dumphib.jar /virginia/uvagfx/
hadoop jar dumphib.jar $1 $2
