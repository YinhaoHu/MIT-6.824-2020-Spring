# FaRM

---

> FaRM是一个分布式内存计算平台，其提供强一致性的同时也提供了低延迟、高吞吐量以及高可用性。其关键的设计包括新事务、复制以及故障恢复。

## Fault Tolerance

与Raft这样的多数共识算法不同，FaRM要求至少有一个server是可用的，也就是一个数据对象共有f+1个，其中f是允许故障的机器数量。

---

## Performance

FaRM的高性能得益于其综合利用了sharding, NVRAM scheme, RDMA等技术。

**sharding:** FaRM要求一个数据至少有一个shard server是可用的。这就加快了read的时间。

**NVRAM:** 数据放在大多数时间放在主存中。并且为每个机架提供一个电池，在停电时自动为服务器继续供电，将数据从RAM中写入磁盘。

**RDMA:** 应用代码直接绕过内核调用NIC的RDMA来替换传统RPC，这使得网络IO耗时下降不少。

---

## New Transaction

论文中提到事务的提交有五个阶段：lock -> validation -> commit backup -> commit primary -> commit

对于read-only transaction，只需要执行validation阶段，这样的设计也大大提高的read-only transaction的性能。

对于读比较多而冲突比较少的系统，new transaction能够提供很好的性能。相反，冲突很频繁的系统中，new transaction就不太合适。


