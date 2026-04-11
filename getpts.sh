#!/usr/bin/bash
export PGPASSWORD="$(cat ~/dbpasswd)"
CMD="select extract(epoch from (time - now()))::integer,t1,t2 from pipes"
CMD="$CMD where time > now()-interval '6 hours' order by time asc"
echo "$CMD" \
	| psql -h raspberrypi-2 -t -U telemetry \
	| tr -d ' ' \
	| tr '|' '\t' \
	| sed '/^$/d'
