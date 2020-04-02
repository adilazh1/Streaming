#!/bin/bash

LAMBDA="/home/$USER/lambda"

export JAVA_HOME="/home/$USER/jdk1.8.0_144/"

if [ -d $LAMBDA ];
then
	if [ -f $LAMBDA/pids ];
	then
		kill -9 `cat $LAMBDA/pids | tr \\n ' '` &> /dev/null
	fi
	sudo rm -r $LAMBDA
fi
mkdir -p $LAMBDA/logs
mkdir -p $LAMBDA/zookeeper
cd /home/$USER/kafka_2.11
nohup ./bin/zookeeper-server-start.sh config/zookeeper.properties &> $LAMBDA/logs/zookeeper.log &
echo $! >> $LAMBDA/pids
nohup ./bin/kafka-server-start.sh config/server.properties &> $LAMBDA/logs/kafka.log &
echo $! >> $LAMBDA/pids
cd ~