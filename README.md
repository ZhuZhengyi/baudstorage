# BaudStorage

## Overview

BaudStorage is a distributed storage system of immutable objects and streaming files. And it provides several pragmatic abstractions: 

* BLOBs like images or short video etc

* hierachical directories

* file streams of append-only extents

## Architecture

BS consists of several subsystems: 

* the cluster master. single raft replication

* the metanode cluster. multi-raft replication, a metadata range (ino range) per raft; a namespace is partitioned by ino range. 

* the datanode cluster. de-clustering volumes of objects or extents


## APIs

- RESTful S3-compatible API 
- FUSE
- Java SDK
- Go SDK
- NFS

## Use Cases and Ecosystem

BaudEngine on BaudStorage

minio integration

HBase on BaudStorage

nginx integration for image service

