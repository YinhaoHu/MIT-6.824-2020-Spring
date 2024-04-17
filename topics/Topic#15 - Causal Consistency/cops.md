# COPS: Causal Consistency

---

## 基础概念(Basic Concepts)

列出必要的基础概念及其解释在论文的对应位置。

- Potential causality: page3.

- Causal consistency: page3.

- Conflict: page3

- Convergent conflict handling: page3.

---

## 一致性模型(Consistency Model)

在常见的一致性模型中，按照一致性强度递减分为：linearizability -> sequential -> causal -> eventual。理解global order和real time order时，将其代入一个multiple datacenter setting distributed system中会更好理解。

- linearizability consitency: support a global, real time operation execution order.

- sequential consistency: suuport a global order, real time is not guranteed.

- causal consistency: guarentee the partial order between the dependent operations.

- eventual consistency: guarantee that an update to an item will be eventually read.

---

## ALPS系统(ALPS System)

availibility, low latency, partition tolerant和stronger consistency是COPS系统的设计目标。在COPS中，以下的设计有助于实现ALPS系统。

- client只是把请求发送到自己所在的数据中心的数据存储服务器，并且数据中心之间的写通知是异步的。

- 每个数据项都有一个版本号以及依赖关系。这可以为causal+ consistency提供实现基础。

---

## COPS

COPS提供put, get以及get_transaction三个操作。其中，get_transaction通过使用key的denpendencies为用户提供了获取多个key时的一致性，并且只需要最多两次通信。

正是因为数据中心之间的写通知是异步的，为COPS提供了高的可扩展性。

Causal Consitency依赖于dependency，当dependency chain过长，系统的响应时间会更长。

---

## 情况(Situation)

Causal consistency是比较流行的研究领域，但是在工业中很罕见。更为常见的是strong consistency(Spanner) 和eventual conssitency(Cassandra)以及memcached这样的primary site。  


