#!/bin/bash

java -cp 'lib/jars/*' -Dname='52066-54449-clusterRaptor'  -Dpass=raptor  -DinPath="/afs/it.itba.edu.ar/pub/pi/dataset-10000.csv" -Daddresses='10.16.33.197'  -DlineStart="0" -DlineEnd="5000"  "ar.itba.edu.pod.hazel.client.MapLoader" $*

