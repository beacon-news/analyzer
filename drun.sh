#!/bin/bash

e=
for i in $(cat .env)
do
  e="$e -e $i"
  echo adding $i
done

s="docker run --name analyzer --network host --rm ${e} analyzer"
echo "$s"

$s