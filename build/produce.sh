#!/bin/sh

set -m # Enable Job Control

if [ -z "$1" ]; then
	topic="alok-topic-1"
else
	topic=$1
fi

if [ -z "$2" ]; then
	messages=10
else
	messages=$2
fi

timestamp=$(date +%d/%m/%Y_%H%M%S)

for i in $(seq 1 $messages); do # start 10 jobs in parallel
	d="${i}-${timestamp}"
	echo "producing message | topic: $topic | data: $d"
	plumber write message kafka --address=a6f50e841fe284aea870ea716ecf0623-1714444736.ap-south-1.elb.amazonaws.com:9094 --key tipocakey --topic="${topic}" --input-data="$d"
done

# Wait for all parallel jobs to finish
while [ 1 ]; do fg 2> /dev/null; [ $? == 1 ] && break; done
