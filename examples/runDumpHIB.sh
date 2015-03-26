#!/bin/bash
ant -f ../build.xml dumphib
hadoop jar dumphib.jar $1 $2
