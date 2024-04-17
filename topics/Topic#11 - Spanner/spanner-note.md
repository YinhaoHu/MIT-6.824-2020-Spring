# Spanner

---

Spanner的亮点在于其是一个全球化的支持分布式事务的分布式系统，并且提供客观的性能。支持这一亮点的设计主要是

* 区别read-only transaction, read-write transaction

* 考虑时间误差的TrueTime库和快照隔离

* 提供external consistency的两个规则

# Distributed Transactions

---

**Read-write transaction**

涉及：Two-phase commit, Two-phase locking, Fault-tolerant protocol(Paxos)

**Read-only transaction**

涉及：Multi-version

# TrueTime Library & Snapshot Isolation

---

TrueTime考虑时间误差，TrueTime.Now()返回一个时间区间[earliest,latest]，保证调用时刻的绝对时间落在区间中。

时间戳的分配：

read-write transaction: TS = commit time

read-only transaction: TS = start time

时间区间误差的影响：

过大-> 太慢

过小-> 出现正确性问题

# Two Rules

---

**Start Rule**: TS=TT.Now().latest. For read-write txn, TS is the start time. For read-only txn, TS is the commit time.

**Commit Wait**: read-write transaction can commit only if the current time is guaranteed to  be greater than commit time.


