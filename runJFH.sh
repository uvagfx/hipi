#!/bin/bash
ant -buildfile build.xml.jpegfromhib
hadoop dfs -rm /virginia/uvagfx/jpegfromhib.jar
hadoop dfs -put jpegfromhib.jar /virginia/uvagfx/
hadoop jar jpegfromhib.jar $1 $2 $3
