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

## Implementation

### Fast backup

The fast backup mechanism is implemented to speed up the log synchronization process and 
is specially optimized for the three cases mentioned by Robert T. Morris. The cases
and reactions are listed below: (Note that the number in the log below is term).

**Point 0**     The log of follower has a lag compared to the first entry in the Entries sent
by AppendEntries. Specifically: 
```
Leader Log:[1,2,2,3,3,3]
Follower Log:[1]
AppendEntiresArgs: (PrevLogIndex=4, PrevLogTerm=3, Entires=[3])
```

**Point 1**     Old leader becomes the follower and received AppendEntries from the 
new leader. Specifically:
``` 
Leader Log:[1,3,3]
Follower Log:[1,2]
AppendEntiresArgs: (PrevLogIndex=1, PrevLogTerm=3, Entires=[3])
```

**Point 2**     Conflict occurs. Specifically:
``` 
Leader Log:[1,2,2,3,3,3]
Follower Log:[1,2,2]
AppendEntriesArgs: (PrevLogIndex=0, PrevLogTerm=1, Entries[2,2,3,3,3])
```

Generally, if conflict occurs, the fast backup asks the follower to send the first index
after the final equivalent entry.

## Experience

* A reference time for all tests is around 180 seconds.

* To make sure the test result is reliable. Run at least
30 times the whole test suite.
