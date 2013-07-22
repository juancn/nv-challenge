#!/bin/bash
set -x
cd $(dirname $0)
rm sdk2.zip
zip -9 sdk2.zip $(find -E sdk -follow -not  -regex ".*/(target|\.DS_Store).*" -type file)
mv sdk2.zip ../challenge-app/framework/res/runner/nv/sdk2.zip
