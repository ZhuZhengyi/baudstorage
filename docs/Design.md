# Key design points of BaudStorage

## Concepts

Baudstorage is a single datacenter storage by design

multi-tenancy - a Baudstorage cluster usually hosts multiple filesystem instances. 

a filesystem instance = a namespace + one or multiple blockgroups (volumes)

a namespace = one or multiple inode ranges

an inode range = one inode table + one dentry BTree

* single-writer-multi-reader

a file streaming has at most one writer client. 

## the Cluster Master

* data structures

RangeMap

VolumeMap

* persistence

in-memory but also persisted to a key-value store like rocksdb

* replication

raft

## the Metadata Store

### Free Ino Space Management

Reclaiming or not? No. 

1, ino is 64bit. we can add new ranges.

2, bitmap etc. impact performance and bring engineering cost.

### Partitioning by Inode Range

Partitioning or not? Yes. 

But we do not break a namespace into fixed ino ranges

say a namespace initially has one inode range [0, ++],

when the memory usage of this range has reached to a threshold say 8GB, and the current maxinum ino is 2^20, then the Baudstorage Master creates a new range [2^21, ++] and seals the original one as [0, 2^21) -- note the first range still can allocate new inodes. 

### Replication

inode ranges as the replication unit

multi-raft

### Data structures

the in-memory dentry B+Tree, and the in-memory inode B+Tree, both implemented via the Google's btree pkg

also written to the underlying key-value store

* Inode

nlink
size
exentMap: the list of extents (offset, length, exentID)
generation: i.e. the version number, the times of updates, for compare-and-set

* Directory entry

(parentIno, name) -> (childIno, type)

## the Extent Volume

volumes as local directories, and extents as local files

TODO: No need to divide exent as blocks? Or blocks as the checksumming unit


volumes work as the replication unit, and have two possible states: Active, or Sealed.

a leader, one or two followers

extents can only be appended and also have the two states: active or sealed, which is recorded in the corresponding inodes.

## Append

Write-Lease or not? No. It is the upper-tier application that guarantees a file has a single writer. 

the dataflow in the normal case and exceptional cases:

* open a file in the append-only mode, fetch the inode information, particularly the 'generation' attribute

* the first append operation will create a new extent and update the inode by CAS

* the following appends just append to the extent if it does not reach the length limit. 


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


## Pub/Sub and Streaming Read

Multiple reader clients need to read a file stream in real time. So they subscribe to the current extent and consume the newest changes without need to first read the inode. 


## Cross Media

A BaudStorage cluster or even a datanode is usually equipped with one or multiple types of storage media: HDD, SSD, and NVM. 

When creating a filesystem volume on BaudStorage, the application can choose a media type. 


