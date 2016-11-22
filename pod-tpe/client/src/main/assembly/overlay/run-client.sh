#!/bin/bash

java -cp 'lib/jars/*' -Dname='52066-54449-clusterRaptor'  -Dpass=raptor  -DinPath="/afs/it.itba.edu.ar/pub/pi/dataset-1000.csv" -DoutPath="$HOME/output.txt" -Daddresses='10.16.33.197' -Dquery="5" "ar.itba.edu.pod.hazel.client.TPEClient" $*

