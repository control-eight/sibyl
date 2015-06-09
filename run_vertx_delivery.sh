#!/bin/bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
export CLASSPATH="$DIR/dependencies/*:$DIR/cluster.xml"
#echo $CLASSPATH

vertx run com.my.sibyl.itemsets.rest.SibylVerticle -cluster -instances $1 -ha -hagroup $2 -conf $3
