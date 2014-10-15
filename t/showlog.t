#!/bin/sh
echo "1..6"
basedir=`dirname $0`/..

($basedir/perl -MAnyEvent::MySQL::Client::ShowLog $basedir/t/basic.t 2> $basedir/t/test-showlog-log.txt > /dev/null && echo "ok 1") || echo "not ok 1"

(grep "sql:select id from foo" $basedir/t/test-showlog-log.txt > /dev/null && echo "ok 2") || echo "not ok 2"
(grep 'sql:insert into foo (id) values (?)	sql_binds:(42)' $basedir/t/test-showlog-log.txt > /dev/null && echo "ok 3") || echo "not ok 3"
(grep "runtime:" $basedir/t/test-showlog-log.txt > /dev/null && echo "ok 4") || echo "not ok 4"
(grep "dsn:DBI:mysql:" $basedir/t/test-showlog-log.txt > /dev/null && echo "ok 5") || echo "not ok 5"
(grep "rows:1" $basedir/t/test-showlog-log.txt > /dev/null && echo "ok 6") || echo "not ok 6"
