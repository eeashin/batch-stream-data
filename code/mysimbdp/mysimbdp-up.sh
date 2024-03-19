#!/bin/bash
# This is a Unix-style bash script
set -e
trap 'echo "An error occurred. Exiting..."; exit 1;' ERR
echo " Generating keyfile for MongoDB cluster..."
openssl rand -base64 756 > ./mongo/mongodb.key
chmod 600 ./mongo/mongodb.key
mkdir ./mongo/mongodb > /dev/null 2>&1
sleep 5
echo "Starting MongoDB Shard cluster init..."
docker-compose up -d --build
sleep 60
echo seed the CONFIG server
## Config servers setup
docker exec -it mongo-config /bin/bash -c "/usr/bin/mongosh --port 27017 < /config.init.js"
sleep 15
## Shard servers setup
docker exec -it mongo-shard-1 /bin/bash -c "/usr/bin/mongosh --port 27017 < /shardrs.init.js" 
sleep 15
## Apply sharding configuration
sleep 15
echo configure ROUTER sharding
docker exec -it mongo-router /bin/bash -c "/usr/bin/mongosh --port 27017 < /router.init.js"
sleep 15
## Enable admin account
echo USER init
docker exec -it mongo-router /bin/bash -c "/usr/bin/mongosh --port 27017 < /user.init.js"
sleep 15
docker exec -it mongo-router /bin/bash -c "/usr/bin/mongosh --port 27017 < /enable-shard.js"
sleep 15
echo "Sharding ENABLED for tenant 1 & 2 using 'hashed'."
echo "Installing apache spark..."
brew install apache-spark
echo "Downloading Kafka Confluent-7.4.4..."
curl -O https://packages.confluent.io/archive/7.4/confluent-7.4.4.tar.gz
tar -xvf confluent-7.4.4.tar.gz
rm -rf confluent-7.4.4.tar.gz
echo "SPARK and MongoDB cluster are RUNNING."
echo "Now Proceed to set manually..."
echo "1. - Kafka Confluent-7.4.4 environment variables..."
echo "2. - SPARK environment variables..."

