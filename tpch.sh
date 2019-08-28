#!/bin/sh

RESULTS=${1:-./results}
DBNAME=${2:-tpch_origin}
USER=${3:-$USER}
HOST=${4:-localhost}
PORT=${5:-9997}

PSQLCMD="$HOME/tmp_basedir_for_pgsql_bld/bin/psql -h $HOST -p $PORT -d $DBNAME -U $USER"
echo "$PSQLCMD"

# delay between stats collections (iostat, vmstat, ...)
DELAY=15

# DSS queries timeout (5 minutes or something like that)
DSS_TIMEOUT=300 # 5 minutes in seconds

# log
LOGFILE=bench.log

function benchmark_prepare() {

	# store the settings
	$PSQLCMD -c "select name,setting from pg_settings" > $RESULTS/settings.log 2> $RESULTS/settings.err

	# create database, populate it with data and set up foreign keys
    #$PSQLCMD < dss/tpch-create.sql > $RESULTS/create.log 2> $RESULTS/create.err

	print_log "  loading data"

	$PSQLCMD < dss/tpch-load.sql > $RESULTS/load.log 2> $RESULTS/load.err

	print_log "  creating primary keys"

	$PSQLCMD < dss/tpch-pkeys.sql > $RESULTS/pkeys.log 2> $RESULTS/pkeys.err

	print_log "  creating foreign keys"

	$PSQLCMD < dss/tpch-alter.sql > $RESULTS/alter.log 2> $RESULTS/alter.err

	print_log "  creating indexes"

	$PSQLCMD < dss/tpch-index.sql > $RESULTS/index.log 2> $RESULTS/index.err

	print_log "  analyzing"

	$PSQLCMD -c "analyze" > $RESULTS/analyze.log 2> $RESULTS/analyze.err

}

function benchmark_run() {

    print_log "running TPC-H benchmark"

	mkdir -p $RESULTS/vmstat-s $RESULTS/vmstat-d $RESULTS/explain $RESULTS/results $RESULTS/errors

	# get bgwriter stats
	$PSQLCMD -c "SELECT * FROM pg_stat_bgwriter" > $RESULTS/stats-before.log 2>> $RESULTS/stats-before.err
	$PSQLCMD -c "SELECT * FROM pg_stat_database WHERE datname = '$DBNAME'" >> $RESULTS/stats-before.log 2>> $RESULTS/stats-before.err

	vmstat -s > $RESULTS/vmstat-s-before.log 2>&1
	vmstat -d > $RESULTS/vmstat-d-before.log 2>&1

	print_log "running queries defined in TPC-H benchmark"

	for n in `seq 1 22`
	do

		q="dss/queries/$n.sql"
		qe="dss/queries/$n.explain.sql"

		if [ -f "$q" ]; then

			print_log "  running query $n"

			echo "======= query $n =======" >> $RESULTS/data.log 2>&1;

			# run explain
			$PSQLCMD < $qe > $RESULTS/explain/$n 2>> $RESULTS/explain.err

			vmstat -s > $RESULTS/vmstat-s/before-$n.log 2>&1
			vmstat -d > $RESULTS/vmstat-d/before-$n.log 2>&1

			# run the query on background
			/usr/bin/time -a -f "$n = %e" -o $RESULTS/results.log $PSQLCMD < $q > $RESULTS/results/$n 2> $RESULTS/errors/$n &

			# wait up to the given number of seconds, then terminate the query if still running (don't wait for too long)
			for i in `seq 0 $DSS_TIMEOUT`
			do

				# the query is still running - check the time
				if [ -d "/proc/$!" ]; then

					# the time is over, kill it with fire!
					if [ $i -eq $DSS_TIMEOUT ]; then

						print_log "    killing query $n (timeout)"

						# echo "$q : timeout" >> $RESULTS/results.log
						$PSQLCMD -c "SELECT pg_terminate_backend(procpid) FROM pg_stat_activity WHERE datname = '$DBNAME'" >> $RESULTS/queries.err 2>&1;

						# time to do a cleanup
						sleep 10;

						# just check how many backends are there (should be 0)
						$PSQLCMD -c "SELECT COUNT(*) AS tpch_backends FROM pg_stat_activity WHERE datname = '$DBNAME'" >> $RESULTS/queries.err 2>&1;

					else
						# the query is still running and we have time left, sleep another second
						sleep 1;
					fi;

				else

					# the query finished in time, do not wait anymore
					print_log "    query $n finished OK ($i seconds)"
					break;

				fi;

			done;

			vmstat -s > $RESULTS/vmstat-s/after-$n.log 2>&1
			vmstat -d > $RESULTS/vmstat-d/after-$n.log 2>&1

		fi;

	done;

	# collect stats again
	$PSQLCMD -c "SELECT * FROM pg_stat_bgwriter" > $RESULTS/stats-after.log 2>> $RESULTS/stats-after.err
	$PSQLCMD -c "SELECT * FROM pg_stat_database WHERE datname = '$DBNAME'" >> $RESULTS/stats-after.log 2>> $RESULTS/stats-after.err

	vmstat -s > $RESULTS/vmstat-s-after.log 2>&1
	vmstat -d > $RESULTS/vmstat-d-after.log 2>&1

    print_log "finished TPC-H benchmark"

}

function stat_collection_start()
{

	# run some basic monitoring tools (iotop, iostat, vmstat)
	for dev in $DEVICES
	do
		iostat -t -x /dev/$dev $DELAY >> $RESULTS/iostat.$dev.log &
	done;

	vmstat $DELAY >> $RESULTS/vmstat.log &

}

function stat_collection_stop()
{

	# wait to get a complete log from iostat etc. and then kill them
	sleep $DELAY

	for p in `jobs -p`; do
		kill $p;
	done;

}

function print_log() {

	local message=$@

	echo `date +"%Y-%m-%d %H:%M:%S"` "["`date +%s`"] : $message" >> $RESULTS/$LOGFILE;

}

mkdir -p $RESULTS;

# start statistics collection
stat_collection_start

# run the benchmark
benchmark_prepare

benchmark_run

# stop statistics collection
stat_collection_stop
