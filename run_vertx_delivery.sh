#!/bin/bash

export CLASSPATH="/home/abykovsky/target/delivery/dependencies/*:/home/abykovsky/target/delivery/cluster.xml"
#echo $CLASSPATH

vertx run com.my.sibyl.itemsets.rest.SibylVerticle -cluster -instances $1 -ha -hagroup $2 -conf $3
