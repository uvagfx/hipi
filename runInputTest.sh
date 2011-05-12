#!/bin/bash
ant -buildfile build.xml.inputtest
hadoop dfs -rm /virginia/uvagfx/inputtest.jar
hadoop dfs -put inputtest.jar /virginia/uvagfx/
hadoop jar inputtest.jar $1 $2
