# BaudStorage

## Overview

BaudStorage is a distributed storage system of immutable objects and streaming files. And it provides several pragmatic abstractions: 

**L1**: object store without namespaces - particularly for images or short video etc. Put an object and the system returns a unique key. Objects are immutable and can be delete however. 

**L2**: object store with plat namespaces - compatible with the S3 API. 

**L3**: filesystems with hierachical namespaces, random read and append-only write.  

**L4**: filesystems with hierachical namespaces, random read/write and complelete filesystem semantics. 

## Architecture

BS consists of several subsystems: 

* the cluster master. single raft replication

* the metanode cluster. multi-raft replication, a metadata range (ino range) per raft; a namespace is partitioned by ino range. 

* the objectnode cluster. de-clustering volumes of objects or object segments;  

* the extentnode cluster. multi-raft replication of stream extents (.e.g, BaudEngine's redo logging)

a namespace = a filesystem instance = an object bucket


## APIs

- RESTful s3-compatible API 
- FUSE
- Java SDK
- Go SDK
- NFS

## Use Cases and Ecosystem

BaudEngine on BaudStorage

minio integration

HBase on BaudStorage

nginx integration for image service
