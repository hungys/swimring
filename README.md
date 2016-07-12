SwimRing
========

SwimRing is our final project for Fault Tolerant Computing course in NCTU. The main goal is to build a minimal **distributed** **fault-tolerant** key-value store with *SWIM Gossip Protocol* and *Consistent Hash Ring*. The project is implemented in Go, and many of design and architecture are adapted from [Uber](http://www.uber.com)'s open source project - [Ringpop](https://github.com/uber/ringpop-go).

Note that this project is **NOT** intended to be deployed in production environment.

# System Design

## Partitioning

One of the key features of SwimRing is the ability to scale incrementally. The horizontal partitioning mechanism, which is also known as **sharding**, is used to achieve this. SwimRing partitions data across the cluster using *consistent hashing*, which is a special kind of hashing such that when a hash table is resized, only K/n keys need to be remapped in average, where K is the number of keys, and n is the number of slots. In consistent hashing, the output range of a hash function is treated as a fixed circular space or **ring**. Each node in the cluster is assigned a random value within this space which represents its position on the ring. Each data item identified by a key is assigned to a node by hashing the data item’s key to yield its position on the ring, and then walking the ring clockwise to find the first node with a position larger than the item’s position.

One obvious challenge of the basic consistent hashing is that the random position assignment of each node on the ring leads to non-uniform data and load distribution. SwimRing solves this by adopting the approach proposed in [Dynamo](http://www.read.seas.harvard.edu/~kohler/class/cs239-w08/decandia07dynamo.pdf), to add some *virtual nodes* (vnode) to the hash ring and assign multiple positions to each real node.

In SwimRing, every node plays the same role except the data items they actually store; that is, we don’t need to distinguish the role of master and slave, and all the nodes are able to act as a coordinator to handle and forward the incoming request. With this design, there will be no single point of failure, although we can still leverage some existing systems like [ZooKeeper](https://zookeeper.apache.org/), or implement consensus algorithm like [Paxos](http://www.cs.utexas.edu/users/lorenzo/corsi/cs380d/past/03F/notes/paxos-simple.pdf) to elect a primary coordinator.

## Replication

SwimRing uses replication to achieve high availability and durability. Each data item is replicated at N nodes, where N is the *replication factor* configured by the administrator. When a write request comes to coordinator, the coordinator will calculate the hash based on key to determine which node is the primary replica. The other N-1 replicas are chosen by picking N-1 **successors** of the primary replica on the ring. Then, the write request is forwarded to all the replicas for updating the data items. Like [Cassandra](http://cassandra.apache.org/), SwimRing provides different level of read/write consistency, including *ONE*, *QUORUM*, and *ALL*. If the write consistency level specified by the client is *QUORUM*, the response will not be sent back to client until more than half writes complete.

For the read request, the most recent data item (based on timestamp) will be forwarded back to the client. To ensure that all replicas have the most recent version of frequently-read data, the coordinator also contacts and compares the data from all replicas in the background. If the replicas are inconsistent, the **read-repair** process will be executed to update the out-of-date data items.

## Membership / Failure Detection

Membership information in SwimRing is maintained in a ring-like structure, and disseminated to other nodes by the *Gossip* module, which is also used for failure detection. The gossip protocol in SwimRing is based on [SWIM](http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.19.5253&rep=rep1&type=pdf). Failure detection is done by periodic random probing (*ping*). If the node fails to send ACK back within a reasonable time, then an indirect probe (*ping-req*) is attempted. An indirect probe asks a configurable number of random nodes to probe the same node, in case there are network issues causing our own node to fail the probe. If both our probe and the indirect probes fail within a reasonable time, then the node is marked *suspect* and this knowledge is gossiped to the cluster. A suspected node is still considered a member of cluster. If the suspect member of the cluster does not dispute the suspicion within a configurable period of time, the node is finally considered faulty, and this state is then gossiped to the cluster.

## Local Persistence

The SwimRing system relies on the local file system for data persistence. Since it’s not the main scope of this project, the storage engine is simplified to provide only the basic crash recovery ability. When a write request comes, the data item is first written into an **append-only** *commit log* on disk, and then written to the *memtable* in memory. SwimRing periodically checkpoints memtable by making a snapshot into *dump file* and stored it on disk. To recover from node crash caused by power failure, the *dump file* is loaded into *memtable* and the *commit log* will be replayed.

# Get Started

To get SwimRing,

```bash
$ go get github.com/hungys/swimring
```

Then switch to your GOPATH and build the project,

```bash
$ cd $GOPATH/src/github.com/hungys/swimring
$ glide install
$ go build
```

To launch a node, please first check `config.yml` is configured correctly, and run,

```bash
(term1) $ ./swimring
```

This command will load configurations from `config.yml`, including RPC listen port, bootstrap seeds, timeout, and **replication factor** (Make sure the value must be no more than total number of nodes in the cluster)...

For testing purpose, you can form a cluster locally, and you can set command line flags to specify external and internal RPC port manually,

```bash
(term2) $ ./swimring -export=8000 -inport=8001
(term3) $ ./swimring -export=9000 -inport=9001
(term4) $ ./swimring -export=10000 -inport=10001
(term5) $ ./swimring -export=11000 -inport=11001
(term6) $ ./swimring -export=12000 -inport=12001
```

Now, a cluster of 6 SwimRing nodes are running.

To use client program,

```bash
$ cd client
$ go build
```

By default, it will connect to port 7000 (Node 1 in this example), and both read consistency level and write consistency level are set to *QUORUM*. See `./client -h` for more custom options,

```bash
$ ./client -h
Usage of ./client:
  -host string
    	address of server node (default "127.0.0.1")
  -port int
    	port number of server node (default 7000)
  -rl string
    	read consistency level (default "QUORUM"): ONE, QUORUM, ALL
  -wl string
    	write consistency level (default "QUORUM"): ONE, QUORUM, ALL
```

Now you are able to use `get <key>`, `put <key> <value>`, `del <key>` to operate on databases. To check current status of the cluster, simply use `stat` command.

```
$ ./client
connected to 127.0.0.1:7000
> get 1
error: key not found
> put 1 1
ok
> get 1
1
> stat
+--------------------+--------+-----------+
|      ADDRESS       | STATUS | KEY COUNT |
+--------------------+--------+-----------+
| 192.168.1.63:7001  | alive  |         1 |
| 192.168.1.63:8001  | alive  |         0 |
| 192.168.1.63:9001  | alive  |         0 |
| 192.168.1.63:10001 | alive  |         1 |
| 192.168.1.63:11001 | alive  |         0 |
| 192.168.1.63:12001 | alive  |         1 |
+--------------------+--------+-----------+
```

## Docker container

We also provide a Dockerfile for deploying SwimRing. To build the Docker image,

```bash
$ docker build -t swimring .
```

Then you can start to run SwimRing in Docker containers. To configure the **seeds** (nodes that can help join the cluser), simply specify their addresses and ports in environment variable `SEEDS`.

For example, the following commands will launch a cluster of 4 nodes locally,

```bash
$ docker run -d -p 5001:7000 --name s1 swimring
$ docker run -d -p 5002:7000 -e 'SEEDS=["172.17.0.2:7001"]' --name s2 swimring
$ docker run -d -p 5003:7000 -e 'SEEDS=["172.17.0.2:7001"]' --name s3 swimring
$ docker run -d -p 5004:7000 -e 'SEEDS=["172.17.0.2:7001"]' --name s4 swimring
```

# Reference

- Lakshman, Avinash, and Prashant Malik. "Cassandra: a decentralized structured storage system." *ACM SIGOPS Operating Systems Review* 44.2 (2010): 35-40.
- DeCandia, Giuseppe, et al. "Dynamo: amazon's highly available key-value store." *ACM SIGOPS Operating Systems Review*. Vol. 41. No. 6. ACM, 2007.
- Das, Abhinandan, Indranil Gupta, and Ashish Motivala. "Swim: Scalable weakly-consistent infection-style process group membership protocol." *Dependable Systems and Networks*, 2002. DSN 2002. Proceedings. International Conference on. IEEE, 2002.
- Ringpop by Uber. http://uber.github.io/ringpop/ 
