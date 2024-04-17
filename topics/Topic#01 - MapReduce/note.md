# 映射-规约（MapReduce）

---

## 1 简介（Introduction）

包括爬取文档、Web日志等计算任务的输入是相当多的，为了在合理的时间内得到结果，需要将这类任务分布在成百上千台计算机上运算。大多数这类任务在概念上都是直接的，但如何将计算并行化、分布数据以及容灾这些问题将这类任务变得复杂。Google的开发人员在`Map`和`Reduce`原语的启迪下，设计了一种能够简单的表达计算任务而隐藏如何将计算并行化、分布数据、容灾设计以及负载均衡等细节的抽象。

---

## 2 编程模型（Programming Model）

在`MapReduce` 编程模型中，输入和输出均是一个由若干个键值对组成的集合。`Map` 操作接受一个键值对输入并产生一个中间键值对集合。由`MapReduce` 库将所有与同一个中间键相关的值组合在一起并传递给`Reduce` 操作。`Reduce`操作接受一个Key和一个与之相关的值的集合作为输入，并形成一个更小的值集合。

---

## 3 实现（Implementation）

许多不同的`MapReduce` 的实现都是可行的，不同的具体环境中（受每台机器操作系统、CPU以及内存机器间的网络连接速率等因素影响）可能有不同的最优实现。

### 3.1 执行概览（Execution Overview）

执行流程大致如下：

* `MapReduce` 库把大量的输入数据分成若干个分块，然后启动Master和Worker进程。Master进程接受Map Worker进程的通知以及通知Reduce Worker进程执行任务。

* Map Worker进程读取对应的分块，解析数据并将数据传递给用户定义的Map操作。Map操作生成的结果先缓存在内存中，然后周期性的写入本地磁盘并把存放位置告知Master。

* Reduce Worker进程接收到Master进程的任务分配通知后，调用RPC从远处获得数据，并对数据进行按键排序，组合同一个键的数据后传递给用户定义的Reduce操作。Reduce操作产生的结果被添加到结果文件中。

* 一旦所有的任务完成，Master进程会唤醒调用进程并返回。

### 3.2 Master数据结构（Master Data Structures）

在`MapReduce`库中，Master维护的信息有：每个Worker的状态以及标识，Map Worker存储结果的位置以及大小。

### 3.3 容灾（Fault Tolerance）

考虑到集群中有相当多的计算机，`MapReduce库` 必须要能够容灾。

**Worker Failure:** 当Master向某一个Worker发送ping时，没有在设定时间内收到回复。系统认为该Worker出现故障，并设置其为空闲状态。当Map Worker出现故障，其负责的任务应该被重新执行并由Master通知所有的Reduce Worker，让准备从故障Map Worker产生的结果位置读取数据的Reduce Worker做出调整。

**Master Failure：** 如果Master出现了故障，那么就重新启动一个Master进程，并从最近的检查点（Master周期性的存储检查点）中进行恢复。

**Semantics in the Presence of Failures：** 系统依赖原子提交来保证容灾性。只要用户提供的`Map` 和`Reduce` 操作是确定的，那么系统的结果也是确定的。

### 3.4 局部性（Locality）

GFS把文件分为多个64MB的块，每个块存在于多台机器上。当Master调度Map Worker时，它们会考虑输入数据所在块的位置，以尽可能多的使用本地数据从而减少网络带宽。

### 3.5 任务粒度（Task Granularity）

Map和Reduce的任务数量设定应该考虑到Master会做的调度决定上界O(M + R)以及维持 O(M ∗ R)个状态。

### 3.6 后援任务（Backup Tasks）

落伍者(Straggler)进程是延长整个计算任务时间的Worker进程。导致进程落伍的原因可能出现在磁盘、CPU、内存以及网络带宽上。可以通过后援执行来减轻落伍者造成的影响。

---

## 4 改善（Refinement）

**分块函数（Partitioning Function）:** 一般的分块函数是以键直接为参数的哈希函数。在特定情况下，可以调整哈希函数的参数。

**顺序保证（Ordering Guarantee）：** Map Worker将生成的中间键值对结果排序。 

**组合函数（Combiner Function）：** 在Map Worker将结果发给Reduce Worker之前在本机调用与Reduce函数(一般来说)相同的函数来去重。

**输入和输出类型（Input and Output Types）：** MapReduce库的用户自定义输入类型来让库进行解析输入以及自定义输出类型来满足用户的需求。

**副作用（Side Effects）：** Map和Reduce操作产生的辅助文件有时是有用的。

**跳过坏记录（Skipping Bad Records）：** 有时Map或者Reduce在产生某些记录时发生了崩溃，导致这些记录不是预期的。那么就忽视这些记录。

**本地执行（Local Execution）：** 为便于用户调试、测评和小规模测试而支持的允许在本机上执行MapReduce所需要的所有工作。

**状态信息（Status Information）：** 在Worker发送给Master的HTTP报文中添加状态信息以便于分析系统。状态信息包括任务是否成功、任务的耗时等。

**计数器（Counters）：** 用于统计Map或者Reduce操作中出现某一类情况的次数。

---

## 5 性能（Performance）

在集群配置不变的情况下，Map/Reduce操作的性能、后援任务、局部性优化以及容灾效果等的变化均会对系统性能产生较大影响。

---

## 6 经历（Experience）

*略*

---

## 7 相关工作（Related Work）

*略*

---

## 8 总结（Conclusion）

Google的MapReduce库在当时取得成功的主要原因是：容易使用、容易表达和能够利用大规模集群。
