#!/bin/bash

# build project
mvn clean package

# store curret working directory
cwd=$PWD

# unpack client
cd client/target
tar -xzvf pod-tpe-client-1.0-SNAPSHOT-bin.tar.gz
cd pod-tpe-client-1.0-SNAPSHOT
chmod u+x run-client.sh
chmod u+x load-map.sh

cd $cwd

# unpack server
cd server/target
tar -xzvf pod-tpe-server-1.0-SNAPSHOT-bin.tar.gz
cd pod-tpe-server-1.0-SNAPSHOT
chmod u+x run-node.sh

cd $cwd
