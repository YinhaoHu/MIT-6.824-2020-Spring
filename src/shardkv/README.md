# ShardKV

## Status 

PASSed. Reliably tested by running the whole test suite for 30 times. And average time for a whole test is around 140  seconds. Specifically, time for every iteration of the whole test suite is confirmed to be in the range(130s,160s).

## Introduction to implementation 

<!-- TODO(Future): Add content for this section. Including: tools, 2pc and so on -->

## Experience


## Note 

Due to the strong leader snapshot mechanism which is different with the snapshot mechanism mentioned in the Raft paper, the snapshot size check in the suit cases might FAIL with around 1% probability which is acceptable. This is a small bug here so far and will be fixed in the future. Besides this, everything works fine.
