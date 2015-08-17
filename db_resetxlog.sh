#!/bin/sh
MYCMD="/usr/lib/postgresql/9.4/bin/pg_resetxlog -o $(/usr/lib/postgresql/9.4/bin/pg_controldata /var/lib/postgresql/9.4/main|grep 'NextOID'|awk ' {print $4} ') -x $(/usr/lib/postgresql/9.4/bin/pg_controldata /var/lib/postgresql/9.4/main|grep 'NextXID'|awk '{ sub(/0\//, ""); print $4 }') -f /var/lib/postgresql/9.4/main"
echo $MYCMD
service postgresql stop
su -c "$MYCMD" postgres
service postgresql start
