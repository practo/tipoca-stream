#!/bin/sh

# ./load.sh 100 topic-prefix
# run from local in k8s
# k get pods -n kafka | grep mysql  | awk '{print $1}' | xargs -I {} kubectl exec {} /bin/sh -n kafka /var/lib/mysql/load.sh

set -m # Enable Job Control

if [ -z "$1" ]; then
	inserts=10
else
	inserts=$1
fi

echo "Inserting $inserts rows is customers table"

last_id=$(mysql -N -s -uroot -pdebezium -Dinventory -e "select id from customers order by id desc limit 1")
new_id=$((last_id+1))

for i in $(seq 1 $inserts); do # start 10 jobs in parallel
    echo "inserting: ${new_id}"
    mysql -N -s -uroot -pdebezium -Dinventory -e "insert into customers (id, first_name, last_name, email) values ('${new_id}', 'first_name_${new_id}', 'last_name_${new_id}', 'email_${new_id}');"
    new_id=$((new_id+1))
done

# Wait for all parallel jobs to finish (add & and make it parallel)
# while [ 1 ]; do fg 2> /dev/null; [ $? == 1 ] && break; done
