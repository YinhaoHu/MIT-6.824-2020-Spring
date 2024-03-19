function help() {
    echo "*** Test script for KVRaft Lab ***"
    echo "$0 help: show help page"
    echo "$0 test <case> <times>: test the specific case for specified times repeatedly"
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
  echo "    Test case 1"; go test -race -run TestSnapshotRPC3B
  echo "    Test case 2"; go test -race -run TestSnapshotSize3B
  echo "    Test case 3"; go test -race -run TestSnapshotRecover3B
  echo "    Test case 4"; go test -race -run TestSnapshotRecoverManyClients3B
  echo "    Test case 5"; go test -race -run TestSnapshotUnreliable3B
  echo "    Test case 6"; go test -race -run TestSnapshotUnreliableRecover3B
  echo "    Test case 7"; go test -race -run TestSnapshotUnreliableRecoverConcurrentPartition3B
  echo "    Test case 8"; go test -race -run TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable3B
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
    "pressure_test")
    pressure_test "$2"
    ;;
    "all")
    for((i=1;i<=100;i++)) do
        echo "=============="
        echo "| Run $i/100 |"
        echo "=============="
        test_all 
    done
    ;;
    *)
    echo "Error: $1 is not a valid option, enter $0 help to learn how to use me."
    ;;
esac