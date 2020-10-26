#!/bin/bash

redis_path=$(which redis-server)
if [ $? -eq 0 ]
then
    echo "Redis already installed"
    exit 0
fi

set -e

current_bin=$(dirname $(which python)) 
if [ ! -w "$current_bin" ] 
then
    echo "$current_bin is not writeable!  Activate a virtualenv first."
    exit 1
fi
echo "Will install redis to $current_bin"

echo "Downloading redis"
curl http://download.redis.io/redis-stable.tar.gz > redis-stable.tar.gz
tar xvzf redis-stable.tar.gz
cd redis-stable

echo "Building redis"
make

echo "Copying binaries to $current_bin"
for fname in {redis-server,redis-sentinel,redis-cli,redis-benchmark}
do
    cp src/$fname $current_bin
done
cd -
rm redis-stable.tar.gz
rm -r redis-stable/
echo "Done!"
