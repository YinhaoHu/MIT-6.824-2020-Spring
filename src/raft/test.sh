function help() {
    echo "*** Test script for Raft Lab ***"
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
        go test -race -run "$1"
        echo "--------------------------------"
    done
    exit 0
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
    *)
    echo "Error: $1 is not a valid option, enter $0 help to learn how to use me."
    ;;
esac