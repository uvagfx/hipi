#!/bin/bash
ant -f ../build.xml hib2jpg
hadoop jar hib2jpg.jar $1 $2 $3
