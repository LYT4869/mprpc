# !/bin/bash

set -e

rm -rf `pwd`/build/*
cd `pwd`/build &&
    cmake .. &&
    cmake --build .
cd ..
cp -r `pwd`/src/include  `pwd`/lib 