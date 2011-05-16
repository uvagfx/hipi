#!/bin/bash
ant hib2jpg
hadoop dfs -rm /virginia/uvagfx/jpegfromhib.jar
hadoop dfs -put jpegfromhib.jar /virginia/uvagfx/
hadoop jar jpegfromhib.jar $1 $2 $3
