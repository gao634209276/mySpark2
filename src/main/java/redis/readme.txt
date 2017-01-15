http://lxw1234.com/archives/2015/05/224.htm
一般情况下，Redis Client端发出一个请求后，通常会阻塞并等待Redis服务端处理，Redis服务端处理完后请求命令后会将结果通过响应报文返回给Client。
这有点类似于HBase的Scan，通常是Client端获取每一条记录都是一次RPC调用服务端。
在Redis中，有没有类似HBase Scanner Caching的东西呢，一次请求，返回多条记录呢？
有，这就是Pipline。官方介绍 http://redis.io/topics/pipelining

通过pipeline方式当有大批量的操作时候，我们可以节省很多原来浪费在网络延迟的时间，需要注意到是用pipeline方式打包命令发送，redis必须在处理完所有命令前先缓存起所有命令的处理结果。打包的命令越多，缓存消耗内存也越多。所以并不是打包的命令越多越好。

使用Pipeline在对Redis批量读写的时候，性能上有非常大的提升。