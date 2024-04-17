# Aurora

---

## 持久性规模(Durability at Scale)

基于法定人数的协议(Quorum-based protocols)需要能够1）在读中，容忍一个可用区故障和一个另外的节点故障；2）在写中，容忍一个可用区故障。

段存储(Segment Storage)技术减少了平均修复时间，从而提高了论文中提及的模型的可用性。

对长时间的故障的容忍度能够使得系统进行单节点的软件更新、热点管理以及操作系统安全补丁。

---

## 日志即数据库(The Log is Database)

传统的分布式数据库处理技术会在Aurora中产生富写现象。Aurora通过只在可用区间传输元信息和日志，从而减少了网络IO次数。通过将所有的网络操作实现为异步操作以及更多的操作在后台进行，Aurora实现了其以减少前台写请求时延的核心目标。