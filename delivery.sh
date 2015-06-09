#!/bin/bash

mkdir target/delivery
cp -a target/dependencies/. target/delivery/dependencies/
cp cluster.xml target/delivery/
cp run_vertx_delivery.sh target/delivery/run_vertx.sh
chmod +x target/delivery/run_vertx.sh
cp heartbeat.conf target/delivery/
cp usual.conf target/delivery
mkdir -pv target/delivery/com/my/sibyl/itemsets/rest/
cp target/classes/com/my/sibyl/itemsets/rest/SibylVerticle.class target/delivery/com/my/sibyl/itemsets/rest/
cp target/sibyl-1.0-SNAPSHOT.jar target/delivery/dependencies/
tar -czf target/delivery.tar target/delivery/
