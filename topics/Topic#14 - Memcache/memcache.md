# Scaling memcache at Facebook

---

## 设计缓存系统(Design a Cache System)

设计缓存系统需要有如下的考虑：

- 缓存数据无效

- 缓存命中

- 数据存放策略：replication/partition

- 与数据库的关系：像Facebook这样把Memcache与MySQL分开 OR 集成Cache Layer和Storage Layer实现高定制化存储系统

---

## 设计考虑(Design Consideration)

Facebook在扩展memcache的时候有如下的考虑：

- 工作负载：Facebook是read-heavy的，WebServer的fanout比较大。在Figure9中提及的all requests中几乎有20%的请求需要与超过100台memcache server通信。

- 缓存目标：memcache用于减少存储层MySQL的负担，不只是为了减少延迟

---

## 规模演进(Scale Evolution)

当分析一个系统规模的变化时，可以参考一般的规模演进路径：Single server -> Multiple Server in a Single Cluster -> Multiple Clusters -> Multiple Datacenters(Regions)。

随着系统规模演进，系统的主要问题也发生了变化。Single server: CPU resource -> Multiple servers in a Single Cluster: Storage and Concurrency Control -> Multiple Clusters: Consistency and Availability -> Multiple Datacenters: Consistency and Performance. 

---

## 复制与分区(Replication and Partition)

**Replication:**

- Pros
  
  - Provide high performance for hot keys
  
  - Less TCP connections overhead

- Cons
  
  * Less total data for a memcache server 

**Partition:**

* Pros
  
  * Good for RAM

* Cons
  
  * Not firendly for hot keys
  
  * More TCP connection overhead
