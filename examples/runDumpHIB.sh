#!/bin/bash
ant -f ../build.xml dumphib
hadoop jar dumphib.jar -libjars ../3rdparty/json-simple-1.1.1.jar $1 $2
