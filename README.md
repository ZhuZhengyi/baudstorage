# BaudStorage

## Overview

BaudStorage is a datacenter storage system for immutable objects and streaming files. And it provides several pragmatic abstractions: 

* BLOBs like images and short videos

* hierachical directories

* file streams of append-only extents

You can create any number of object buckets and filesystem instances on BaudStorage.

## Architecture

BS consists of several components:

* the cluster master. single raft replication

* metanode. multi-raft replication, a metadata range (inode range) per raft; a namespace is partitioned to inode ranges 

* datanode. de-clustering volumes of objects or extents; volume works as the replication unit and every volume is replicated via a consistent replication protocol. 

Note that BS is a highly available storage system with strong consistency: the master, the metadata store, the object store, and the extent store are all consistently replicated. 

The detailed architecture design is in docs/Design.md


## APIs

- Go SDK
- Java SDK
- RESTful S3-compatible API 
- FUSE
- NFS

## Use Cases and Ecosystem

BaudEngine on BaudStorage

HBase on BaudStorage

MyRocks on BaudStorage

minio integration

nginx integration for image service

