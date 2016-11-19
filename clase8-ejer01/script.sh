#!/bin/bash
mvn clean package
cd client/target
tar -xzvf clase8-ejer01-client-1.0-SNAPSHOT-bin.tar.gz
cd clase8-ejer01-client-1.0-SNAPSHOT
chmod u+x run-client.sh
cd ..
cd .. 
cd ..
cd server/target
tar -xzvf clase8-ejer01-server-1.0-SNAPSHOT-bin.tar.gz
cd clase8-ejer01-server-1.0-SNAPSHOT
chmod u+x run-node.sh
cd .. 
cd ..
cd ..