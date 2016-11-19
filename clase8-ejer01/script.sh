#!/bin/bash

# build project
mvn clean package

# store curret working directory
cwd=$PWD

# unpack client
cd client/target
tar -xzvf clase8-ejer01-client-1.0-SNAPSHOT-bin.tar.gz
cd clase8-ejer01-client-1.0-SNAPSHOT
chmod u+x run-client.sh

cd $cwd

# unpack server
cd server/target
tar -xzvf clase8-ejer01-server-1.0-SNAPSHOT-bin.tar.gz
cd clase8-ejer01-server-1.0-SNAPSHOT
chmod u+x run-node.sh

cd $cwd
