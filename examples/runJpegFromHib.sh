#!/bin/bash
ant -f ../build.xml jpegfromhib
hadoop jar jpegfromhib.jar $1 $2 $3
