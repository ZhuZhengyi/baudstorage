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

## the Metadata Range

## the Extent Volume

No need to divide exent as blocks. 

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



