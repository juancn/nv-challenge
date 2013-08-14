#!/bin/bash
set -x
cd $(dirname $0)
rm sdk3.zip
zip -9 sdk3.zip $(find -E sdk -follow -not  -regex ".*/(target|\.DS_Store).*" -type file)
mv sdk3.zip ../challenge-app/framework/res/runner/nv/sdk3.zip
