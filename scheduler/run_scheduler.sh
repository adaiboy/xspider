#!/bin/bash

usage="usage: bash run_scheduler.sh (start|stop)"

if [ $# -lt 1 ]; then
    echo $usage
    exit -1
fi

operation=$1

file_path=$(readlink -f $0)
dir_name=$(dirname $file_path)

addr=`/sbin/ifconfig eth1 | sed -n '2p'| awk '{print $2}'`
port=20000
dbaddr="127.0.0.1:27017"
handler="127.0.01:40000"

cd $dir_name
bin="python -u scheduler.py"
cmd="$bin --addr=$addr:$port --dbaddr=$dbaddr --handler=$handler"

function_start() {
    echo "start with: $cmd"
    ulimit -c unlimited
    rm log.txt
    $cmd > log.txt 2>&1 &
    time_left=10
    while [ $time_left -gt 0 ]; do
        sleep 1
        pid=$(ps aux | grep "$bin" | grep "addr=$addr:$port" | awk '{print $2}')
        if [ "$pid" != "" ]; then
            echo "start $bin with pid $pid"
            return 0
        fi
        time_left=$(($time_left-1))
    done
    return 1
}

function_stop() {
    pid=$(ps aux | grep "$bin" | grep "addr=$addr:$port" | awk '{print $2}')
    echo "pid:$pid"
    if [ "$pid" == "" ]; then
        echo "$bin not found, regard as stop succ."
        return 0
    fi

    echo "stop $bin with pid $pid"
    kill $pid
    time_left=10
    while [ $time_left -gt 0 ]; do
        if [ ! -e /proc/$pid ]; then
            return 0
        fi
        sleep 1
        time_left=$(($time_left-1))
    done
    return 1
}

if [ $operation == "start" ]; then
    function_start
elif [ $operation == "stop" ]; then
    function_stop
else
    echo $usage
fi

succ=$?
if [ $succ -eq 0 ]; then
    echo $bin $operation accomplished
else
    echo $bin $operation failed
fi
