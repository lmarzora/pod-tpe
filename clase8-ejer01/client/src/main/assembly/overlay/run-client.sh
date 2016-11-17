#!/bin/bash

java -cp 'lib/jars/*' -Dname='clusterRaptor'  -Dpass=raptor  -Daddresses='10.16.33.196'  "ar.itba.edu.pod.hazel.client.TPEClient" $*

