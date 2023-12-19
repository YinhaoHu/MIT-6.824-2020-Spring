# Raft

---

Status: PASS all tests.(using command`./test.sh test 2 10`)

AppendEntryRPC could append more than one entry in one RPC.

Optimization is made to cut off unnecessary synchronization time
mentioned in the hint `You will probably need the optimization that backs up nextIndex by more than one entry at a 
time. Look at the extended Raft paper starting at the bottom of page 7 and top of page 8 (marked by a gray line). 
The paper is vague about the details; you will need to fill in the gaps, perhaps with the help of the 6.824 Raft lectures.`