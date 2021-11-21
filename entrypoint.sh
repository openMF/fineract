#!/bin/sh

set -e

echo .
date
echo .
java --version

JVM_OPTS="-XX:+UnlockExperimentalVMOptions -Xmx2048m -Xms2048m -Djava.awt.headless=true -Dprism.fontdir=/opt/fonts -Duser.country=US -Duser.language=en"

java -Dloader.path=/app/libs/ -jar /app/fineract-provider.jar

java -Dloader.path=/app/libs/ $JVM_OPTS -jar /app/fineract-provider.jar
