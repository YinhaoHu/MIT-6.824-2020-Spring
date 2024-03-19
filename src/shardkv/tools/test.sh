#!/bin/bash

function help() {
    echo "*** Test script for ShardKV Lab ***"
    echo "$0 help: show help page"
    echo "$0 test <case> <times>: test the specific case for specified times repeatedly"
    echo "$0 all <times>: test the number of selected all tests for times."
    exit 0
}

function test() {
    if [ $# -ne 2 ]; then
        echo "error: $# parameters were given, want 2"
        exit 1
    fi
    for ((run=1;run <= $2;run++));do
        echo "Run: #$run"
        date
        go test -race -run "$1"
        echo "--------------------------------"
    done
    exit 0
}


function test_all() { 
    go test -race -run TestStaticShards
    go test -race -run TestJoinLeave
    go test -race -run TestSnapshot
    go test -race -run TestMissChange
    go test -race -run TestConcurrent1
    go test -race -run TestConcurrent2
    go test -race -run TestUnreliable1
    go test -race -run TestUnreliable2
    go test -race -run TestUnreliable3 
    go test -race -run TestChallenge1Delete
    go test -race -run TestChallenge1Concurrent
    go test -race -run TestChallenge2Unaffected
}

if [ $# -eq 0 ]; then
    help
    exit 1
fi

case "$1" in
    "help")
    help
    ;;
    "test")
    test "$2" "$3" 
    ;;
    "all")
    total=$2
    for((i=1;i<=$total;i++)) do
        echo "=============="
        echo "| Run $i/$total |"
        echo "=============="
        test_all 
    done
    ;;
    *)
    echo "Error: $1 is not a valid option, enter $0 help to learn how to use me."
    ;;
esac