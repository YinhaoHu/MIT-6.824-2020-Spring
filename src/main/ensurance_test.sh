# To ensure that the implementation is indeed correct.

total_iterations=10
temp_file="ensurance_test_tmp"
# shellcheck disable=SC1073
for((i=1;i<=total_iterations;i++))
do
  echo "Run #$i: "
  ./test-mr.sh > $temp_file
  cat $temp_file | grep "PASSED ALL TESTS"
  rm $temp_file
  echo "------------------"
done