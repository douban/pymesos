#! /bin/bash

wget 'https://github.com/apache/mesos/raw/master/include/mesos/mesos.proto' -O mesos.proto
wget 'https://github.com/apache/mesos/raw/master/src/messages/messages.proto' -O messages.proto
sed -i 's/mesos\/mesos\.proto/mesos\/interface\/mesos\.proto/g' messages.proto
mkdir -p mesos/interface
ln -f mesos.proto mesos/interface/
protoc --python_out pymesos messages.proto
rm -rf mesos mesos.proto
