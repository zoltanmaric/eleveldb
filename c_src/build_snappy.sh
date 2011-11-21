#!/bin/bash

set -e

PWD=`pwd`
SNAPPY_VSN=1.0.4
unset CFLAGS LDFLAGS

if [ `basename $PWD` != "c_src" ]; then
    pushd c_src
fi

BASEDIR=$PWD/snappy

case "$1" in
    clean)
        rm -rf snappy-$SNAPPY_VSN snappy
        ;;

    *)
        test -f snappy/lib/libsnappy.a && exit 0

        mkdir -p $BASEDIR

        tar -xzf snappy-$SNAPPY_VSN.tar.gz
        (cd snappy-$SNAPPY_VSN && ./configure --prefix="$BASEDIR" && make && make install)

        ;;
esac

