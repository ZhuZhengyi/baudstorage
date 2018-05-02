# Key design points of BaudStorage

## Concepts

single datacenter storage - by design

multi-tenancy - a Baudstorage cluster may host multiple filesystem instances. 

a filesystem instance = a namespace + one or multiple blockgroups

a namespace = one or multiple inode ranges

an inode range = one inode table + one dentry BTree

inode attributes: nlink, extentmap, size

extentmap: array of (offset, length, extentID)

## the Cluster Master

* data structures

RangeMap

VolumeMap

* persistence

in-memory but also persisted to a key-value store like rocksdb

* replication

raft

## the Metadata Store

* Free Ino Space Management

Reclaiming or not? No. 

1, ino is 64bit. we can add new ranges.

2, bitmap etc. impact performance and bring engineering cost.

* Partitioning by Inode Range

Partitioning or not? Yes. 

But we do not break a namespace into fixed ino ranges

say a namespace initially has one inode range [0, ++],

when the memory usage of this range has reached to a threshold say 8GB, and the current maxinum ino is 2^20, then the Baudstorage Master creates a new range [2^21, ++] and seals the original one as [0, 2^21) -- note the first range still can allocate new inodes. 


* Data structures

the in-memory dentry B+Tree, and the in-memory inode B+Tree, both implemented via the Google's btree pkg

also written to the underlying key-value store

## the Extent Volume

TODO: No need to divide exent as blocks? 

* persistence

extents as local files

* replication protocol

TODO: review the dataflow in the normal case and exceptional cases. 

## Append

1, consistency

the leader locks the extent to avoid concurrent append operations.

'commit length' = the minimum size of the three extent replicas; moreover, it is remembered by the extent leader. 

only offset < commit length can be read

2, performance

the client firstly writes to the extent and then to the inode - double write overhead. 

observation: 

1) actually we don't need absolutely accurate file size. 

2) the last extent length indicates the total file size. 

optimization: 

* synchronously update the inode's extentmap and size when sealing the last extent and creating a new extent

* update size when closing the file



