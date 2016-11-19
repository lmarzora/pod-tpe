#!/bin/bash

java -cp 'lib/jars/*' -Dname='52066-54449-clusterRaptor'  -Dpass=raptor  -DinPath="C:/Users/Lucas/Documents/POD/tpe/pod-tpe/clase8-ejer01/data/dataset-1000.csv" -DoutPath="C:/Users/Lucas/Documents/POD/tpe/pod-tpe/clase8-ejer01/data/output.txt" -Daddresses='127.0.0.1' -Dquery="5" "ar.itba.edu.pod.hazel.client.TPEClient" $*

