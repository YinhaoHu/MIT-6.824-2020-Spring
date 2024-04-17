# Topic 04 - Raft Consensus Algorithm

---

## 背景(Background)

分布式系统中的节点对一个数据值达成一致的问题被称为*共识问题(consensus problem)*。 例如，在使用复制技术的*容错系统(fault-tolerant system)* 中的共识问题就是服务器对日志内容达成一致，从而保证一致性。*共识算法(consensus algorithm)* 允许一个集群在部分成员发生故障的情况下仍能工作并且保持一致性。

---

## Raft简介(Raft Overview)

Raft共识算法被分为三个部分：领袖选举(leader election), 日志复制(log replication)和安全(safety)。每个服务器在任意时间只能是这三个状态之一：追随者(follower)，候选者(candiate)和领袖(leader)。Raft将时间划分为任意长度的任期(term)。服务器之间通过RPC通信，基本的RPC只有RequestVote和AppendEntries两个。

---

## 领袖选举(Leader Election)

当追随者在随机选择的选举超时(election timeout)时间内没有收到领袖的心跳消息时，会发生领袖选举。这一条件会在系统中的两个场景下满足：1）系统初始化，所有的服务器均处于追随者状态；2）领袖发生故障。

当选举开始时，处于追随者状态的服务器转变为候选者状态并且增加currentTerm，然后为自己投票并且调用RquestVote RPC为一个候选者投票。

候选者的选举过程结束，当且仅当这三个条件中的一个满足时：1）自己获得了大多数投票，成为了领袖；2）有效领袖并且发送了AppendEntires；3）选举超时，自己成为领袖。

一旦选举结束，新的领袖负责让所有追随者的日志与自己的日志保持一致。具体的同步行为参考论文第7页。

为保证安全性，领袖选举还需要满足限制：一个参与者如果没有所有的已提交日志项，那么它不能获得选举胜利。如果一个候选者至少拥有一个多数中的*最新(up-to-date)* 日志，那么该候选者就拥有所有的已提交日志。其中，最新的定义以及该限制的实现参考论文第8页。

---

## 日志复制(Log Replication)

日志由一个日志项序列组成。一个日志项由命令(command)和任期(term)构成。

在正常执行情况下，领袖收到客户端的请求，然后添加到日志中，接下来将该日志项发送给所有的其他服务器。一旦多数服务器返回成功，那么就应用该命令。

在日志不同步的情况下，由领袖的心跳消息来和其他服务器通信，定位到匹配的日志项索引，然后再进行同步日志。

---

## 快照(Snapshot)

为了避免存储的日志越来越多而耗尽存储空间，服务器需要在适当的时候将日志压缩。Raft通过使用快照来压缩日志。在快照中，整个系统的状态被写入到一个存储在稳定存储器上的快照文件中，所有到快照文件最后一个日志项的日志都被丢弃。

一般情况下，每个服务器独立地压缩日志。但存在某些追随者落后领袖过多而领袖已经压缩了需要发送给他们的日志部分，这个时候领袖就需要发送快照给这些追随者。追随者收到领袖发送的快照后会有两种情况：1）收到的快照包含不在当前日志中的内容，此时追随者丢弃整个日志并且整个日志被压缩为该快照；2）收到的快照描述了当前日志的前缀内容，此时追随者将快照涉及到的日志部分压缩为该快照，未被涉及到的日志部分依然有效并且被保留。