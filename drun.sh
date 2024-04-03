#!/bin/bash

e=
for i in $(cat .env)
do
  e="$e -e $i"
  echo adding $i
done

# patch="
# ELASTIC_CONN=https://host.docker.internal:9200
# MONGO_HOST=host.docker.internal
# REDIS_HOST=host.docker.internal
# "

# for i in $patch
# do
#   e="$e -e $i"
#   echo adding/patching $i
# done

s="docker run --name analyzer --network host --rm ${e} analyzer"
# s="docker run ${e} analyzer"
echo "$s"

$s