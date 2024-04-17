# SUNDR: Secure Untrusted Data Repository

---

## 关键概念(Key Concepts)

- Byzantine Fault

- Computer system security

- Fork consistency

- Decentralized distributed system

---

## 概述(Outline)

当去中心化的分布式系统存在不可信任的服务器时，需要解决拜占庭问题。恶意的服务器可能会带来这些问题：1）发送错误的信息给客户；2）发送过时的信息给客户；3）发送所请求信息不存在的通知给客户（实际上所请求信息是存在的）。 

就系统安全性而言，SUNDR聚焦于确保数据完整性(Data Integrity)的技术。SUNDR通过使用数字签名技术解决了上述问题1），其具体做法是每个日志项有一个签名元数据，接收到数据的客户端通过公钥检查签名与数据是否匹配。上述的问题2）会产生fork问题，具体来说，两个的客户端向同一个服务器在某一固定状态时收到两份不同的日志并在不同的日志上操作。SUNDR通过保证fork consistency来解决该问题。有两个方法来检测到fork：1）客户端互相通信；2）Timestamp box：指定某个可信赖的服务器周期性地发送更新某个文件的请求到文件系统服务器。通过合并fork，系统的日志可以一致。

论文提及了一个straw-man filesystem的实现来介绍fork问题以及上段所述的基本的解决方法。实现fork consistency的前提是要能够发现fork。此后通过serialzed SUNDR来介绍通过请求文件系统的快照信息来通信，从而减少宽带消耗。


