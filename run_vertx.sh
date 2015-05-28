#!/bin/bash

export CLASSPATH="/Users/abykovsky/Work/Projects/sibyl/target/dependencies/*"
#echo $CLASSPATH

cd target/classes
vertx run com.my.sibyl.itemsets.rest.SibylVerticle -instances 5
