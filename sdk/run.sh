#!/bin/bash
BASEDIR=$(dirname $0)
mvn clean package
java -cp "$BASEDIR/runner/target/runner-1.0-jar-with-dependencies.jar:$BASEDIR/simulator/target/simulator-1.0.jar" medallia.sim.Submission 