# Raft

---

## Status

PASS all tests. Reliably tested by running the whole test suite for 50 times.

## Note

AppendEntryRPC could append more than one entry in one RPC.

Optimization is made to cut off unnecessary synchronization time
mentioned in the hint `You will probably need the optimization that backs up nextIndex by more than one entry at a 
time. Look at the extended Raft paper starting at the bottom of page 7 and top of page 8 (marked by a gray line). 
The paper is vague about the details; you will need to fill in the gaps, perhaps with the help of the 6.824 Raft lectures.

## Experience

* A reference time for all tests is around 180 seconds.

* To make sure the test result is reliable. Run at least
30 times the whole test suite.
