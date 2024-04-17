# 分布式事务

---

## 基本理论

分布式事务是一个涉及到多个网络主机参与的数据库事务。事务管理器和资源管理器共同实现对分布式事务的支持，事务由应用发起。同单机数据库事务一样，分布式事务也需要保证ACID。在实际情况中，系统设计者可能会根据应用需求的不同而对ACID做灵活地调整，比如部分的支持ACID而选择BASE。

并发控制和原子提交是实现一个分布式事务的两个重点。

---

## 并发控制

分布式事务的并发控制策略包括悲观并发策略和乐观并发策略两种。该小节只讨论悲观并发策略中的两阶段锁协议2PL。

2PL分为增长(growing)和收缩(shrinking)两个阶段。在增长阶段，只允许事务获取锁。在收缩阶段，只允许事务释放锁。

关于2PL以及更多并发策略的讨论，参考关于数据库管理系统的事务处理篇章。

---

## 原子提交

分布式事务的最常见的原子提交协议是两阶段提交(Two Phase Commit, 2PC)。

2PC的工作流程如下：

1. CollectVotes阶段：coordinator问询participants的提交意愿。如果所有的participants都愿意提交，那么该事务的结局就是commit，反之，则是abort。此阶段，participants还会写入prepare日志。

2. Decision阶段：coordinator把事务的结局发送给所有的participants。participants写入commit/abort日志。

Presumed Abort/Commit是2PC的一个优化技术，其思想是假定在participant指定一段时间后没有收到decision消息，那么就假定事务abort/commit。

2PC的最大缺点就是其是一个阻塞协议，并且coordinator和部分participants都fail的时候，其他的participants只有等到所有的participants都恢复或者coordinator恢复的时候才能确定事务的结局。

三阶段提交通过引入PreCommit阶段来解决该问题。PreCommit阶段在CollectVotes和Decision阶段之间，是三阶段的第二阶段。在该阶段coordinator把precommit消息发送给k个participants，这样就能在coordinator故障后、只要这k个participants中存活一个，那么新的coordinator就可以通过问询其他participants来确定事务结局。

---

## 实例

以下图示展示了一个从Alice账户中跨行转账100到Bob账户的分布式事务流程(2PC+2PL)。

![](D:\PersonalFilesBase\Study\distributed-system\topics\Topic%230%20-%20Foundational%20Theory\images\distributed-txn-example.png)

---

## 总结

分布式事务涉及到多个网络主机通过相互协调来共同完成事务处理。并发控制和原子提交是保证分布式事务ACID属性不可或缺的技术。


