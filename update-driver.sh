#!/bin/bash
set -x
cd $(dirname $0)
mvn clean package
cp driver/target/driver-2.0.jar ../challenge-app/framework/res/runner/nv/driver-2.0.jar
cp runner/target/runner-2.0-jar-with-dependencies.jar ../challenge-app/framework/res/runner/nv/runner-2.0-jar-with-dependencies.jar