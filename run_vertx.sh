#!/bin/bash

export CLASSPATH="/Users/abykovsky/Work/Projects/sibyl/target/dependencies/*:/Users/abykovsky/Work/Projects/sibyl/cluster.xml"
#echo $CLASSPATH

cd target/classes
vertx run com.my.sibyl.itemsets.rest.SibylVerticle -cluster -instances $1 -ha -hagroup $2 -conf $3
