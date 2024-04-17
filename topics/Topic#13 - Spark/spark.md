# Spark

---

## 简介(Brief)

Spark是一个用于大规模数据处理的分析引擎，提供了分布式内存的抽象。它在目前被广泛用于数据分析。其是MapReduce的一个演进，弥补了MapReduce的不足，能够把中间结果保存在内存中支持迭代处理。

---

## 弹性分布式数据集(Resilient Distributed Dataset,RDD)

RDD是一个不可变的、分区的数据集，其支持transformation和action两种操作。其可以驻留在内存中，也可以写入磁盘中。

RDD首先从HDFS中读入，随后的RDD通过transformation而来。若干个RDD形成了一个lineage，一个lineage是一个有向无环图。每个RDD的依赖关系有narrow dependency和wide dependency两种。

---

## 容错(Fault Tolerance)

在lineage的帮助下，Spark可以通过重新计算RDD来实现容错。这区别于replication容错技术。当lineage过长并且存在wide dependency时，从头计算来恢复系统耗时较长。为此，Spark提供了检查点，让用户自由地选择检查点。一般在wide dependency的父结点处写检查点。

---

## 适用场景(Suitable Use Case)

Spark适用于大规模数据的批处理，典型的场景包括PageRank、购物平台对消费者的消费习惯分析等。现在，Spark Streaming能够处理流式数据。但像Web应用的存储系统这类细粒度处理的系统，Spark并不适用。
