# Frangipani

---

> 课堂中Robert Morris讲授的Frangipani系统设计的亮点

## 系统组分简介(System Components Overview)

工作站(workstation)是Frangipani系统中的客户端，服务器(server)是Frangipani文件系统服务器，锁服务器(lock server)提供Frangipani中的锁服务。

---

## 锁协议(Locking Protocol)

Frangipani引入request, grant, revoke和release四种消息类型来实现锁协议。锁协议在Frangipani系统中缓存一致性协议、分布式事务以及故障恢复中起到同步的作用。

---

## 缓存一致性(Cache Coherence)

为了提供更高的性能，Frangipani引入缓存，使得工作站可以在本地处理数据，减少网络IO开销。

当客户端要请求读或写某个文件或目录时，客户端需要获得合适的锁。锁在此起到的作用可以参考常规的锁在并发控制中起到的作用。锁服务器通过响应超时来判断一个客户端是否关闭。

---

## 分布式事务(Distributed Transaction)

简单来说就是获得所有相关数据的锁，最后再释放。类似于DBMS中的事务。

---

## 分布式故障恢复(Distributed Crash Recovery)

使用WAL技术来实现日志。通过日志重演来实现故障恢复。每个客户端一个日志并且每个日志存储在petal服务器中。
