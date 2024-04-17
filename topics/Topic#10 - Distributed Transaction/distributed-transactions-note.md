# 分布式事务(Distributed Transactions)

---

## 学习安排(Learning Plan)

Day 1: 9.1.5 & 9.1.6 

Day 2: 9.5.2 & 9.5.3 & 9.6.3

Day 3: Question + Lecture + Note + Practice

---

## 关键概念(Key Concepts)

`Sequence coordination`: action X must happen before Y. Exactly, the first action must complete before the second action begins.

`Before-or-after atomicity`: In the invokers' view, the actions as if occured completed before or after one another.

`Correctness`: The behaviour of an action is correct in a system if 1) the old state of the system is correct and 2) any correct state of the old system is transformed correctly to the new state. This concept should be independent of the application.

`Serializable`: There is a serial order of concurrent transactions that would, if followed, leads to the same ending state.

`Simple Locking`: Acquire all locks before reading/modifying any shared data object a transaction might use and release the locks in the end of the transaction. Two related conceptes are `locking set` and `locking point`.

`Two Phase Locking`: Identical with the Two Phase Locking protocol in DBMS concurrency control which consists of growing phase and shrinking phase. For a transaction that aborts, it should release the locks in the end of the transaction. For system recovery, it is correct to not log the lock and not acquire the locks in the recovery process.

`Two Phase Commit`: This protocol consists of voting phase and commit phase. In the voting phase, the coordinator inquires the workers whether they can commit or not. In the commit phase, the coordinator decides to abort or commit the transaction. This protocol obtains the ability to atomic commit in the distributed setting but provides low availability, which means this is not a fault tolerant protocol and hence totally different with raft.

In the voting phase, the coordinator will think the participant is dead if the participant has not responsed for a long time. In the commit phase, the participant will block until the commit message comes.

To implement the 2PC protocol, the normal case is easy to make correct. The hard pieces are in coping with the crash. The optimization would be presumed commit/abort.`Three Phase Commit` remains to be explored.

---

## 课堂收获(Gains from Lecture)

To achive high availability and gain the ability to atomic commit, we can combine fault-tolerant protocols and atomic commit protocols, for example, we can build a distributed transaction processsing system with raft and two phase commit.
