#!/bin/bash -ex

# drop cascading build back onto host machine
# to make IDE resolution work for local development
rm -rf /root/m2-host/repository/net/wensel/
mkdir -p /root/m2-host/repository/net/wensel/
ls /root/.m2/repository/net/wensel/
cp -r /root/.m2/repository/net/wensel/ /root/m2-host/repository/net/
