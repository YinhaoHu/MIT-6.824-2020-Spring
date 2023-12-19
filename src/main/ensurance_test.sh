# To ensure that the implementation is indeed correct.

total_iterations=10
# shellcheck disable=SC1073
for((i=1;i<=total_iterations;i++))
do
  echo "Run #$i: "
  ./test-mr.sh
  echo "------------------"
done