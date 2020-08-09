#!/bin/sh

set -m # Enable Job Control

for i in `seq 10`; do # start 10 jobs in parallel
	echo "producing $i (trigger)"
	plumber write message kafka --address=a6f50e841fe284aea870ea716ecf0623-1714444736.ap-south-1.elb.amazonaws.com:9094 --key alok-key --topic="alok-topic-2" --input-data="data${i}"
done

# Wait for all parallel jobs to finish
while [ 1 ]; do fg 2> /dev/null; [ $? == 1 ] && break; done