概述：


一.master的概述：

1.master主要管理datanode,metanode,并提供volGroup的视图和metaGroup的视图,以及namespace信息

　　1.1 datanode的管理：初始化由管理员通过http接口，将其加入到集群。并不间断的发送命令给datanode，检测存活信息以及存储空间信息

　　1.2 metanode的管理：初始化由管理员通过http接口，将其加入到集群。并不间断的发送命令给metanode，检测存活信息以及metaRange的信息

　　1.3 volGroup的视图，由sdk发起，获取当前整个所有的volGroup视图，包含volGroup的成员信息

　　1.4 metaGroup的视图：由client发起，获取某个namespace下所有的metaGroup的信息，包含metaGroup的成员信息，
以及metaGroup负责的inode范围，以及当前metaGroup的leader

　
2.接口信息描述:

　　　/metanode/add? 注册一个metanode进程

　　　/datanode/add? 注册一个datanode进程

　　　/client/volgroup?　获取一个vol的视图

　　　/client/metagroup?　获取某一个metaGroup的详细信息

　　　/client/namespace? 获取整个namespace下所有的metagroup视图信息


3.持久化,持久化通过raft+rocksdb实现。持久化包含:

    3.1　metanode的持久化信息  
        
　  ３.2 datanode的持久化信息

　  ３.3 volGroup的持久化信息

    3.4 metagroup的持久化信息
    
4.主动发起的接口命令：，此接口由于耗时很长，所以master端发起命令后。

　　由于metanode和datanode均采用tcp协议。。因此master需要发送相应的tcp命令，以完成相应的功能

　　4.1.向metanode发送检测存活指令

　　4.2.向metanode发送创建metaRange的信息

　　4.3.向metanode发送loadMetaRange的指令

　　4.4.向datanode发送检测存活的指令

　　4.5.向datanode发送创建vol的指令

　　4.6.向datanode发送loadVol的指令

　
　　
　　