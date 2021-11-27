#!/usr/bin/env sh

if [ $# -ne 2 ]
then
    echo "Usage $(basename $0) <db-file> <expected-dir>"
    exit 1
fi

dbfile=$1
expected=$2

trap '/bin/rm -f /tmp/$(basename $0)_*.$$; exit 1' 1 2 15

for exp in $(ls -1 $expected/*)
do
    exp=$(realpath $exp)
    tmp=/tmp/$(basename $0)_$(basename $exp).$$
    echo "select * from \`$(basename $exp)\`" | sqlite3 $dbfile > $tmp
    diff -q $exp $tmp >/dev/null
    if [ $? -ne 0 ]
    then
        actual=$(realpath $(basename $exp)_actual)
        /bin/mv -f $tmp $actual
        echo "The actual contents of $(basename $exp) are not as expected. Saving them in ${actual}"
        exit 1
    fi
    rm -f $tmp
done
