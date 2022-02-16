#!/bin/bash -ex

RELEASE_LINK=https://github.com/cwensel/cascading/archive/refs/tags/$CASCADING_RELEASE.zip

# get the source for the target release tag
mkdir -p /scalding/tmp
apt-get install curl unzip git -y
curl $RELEASE_LINK -L -o /scalding/tmp/$CASCADING_RELEASE.zip
cd /scalding/tmp/
unzip $CASCADING_RELEASE
cd cascading-$CASCADING_RELEASE

# build cascading into maven cache so that scalding can pick it up
./gradlew publishToMavenLocal -x signMavenPublication
ls -R /root/.m2/repository/net/wensel/
