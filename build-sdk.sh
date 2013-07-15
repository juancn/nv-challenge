#!/bin/bash
set -x
cd $(dirname $0)
rm sdk.zip
zip -9 sdk.zip $(find -E sdk -follow -not  -regex ".*/(target|\.DS_Store).*" -type file)
mv sdk.zip ../challenge-app/framework/res/runner/nv/sdk.zip
