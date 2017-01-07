### dfs is whta?
dfs is Stand-alone file system, Read and write functions are currently available。
The use of multi-threaded file segmentation merge。

### dfs which services?
The current service is as follows:
```text

    1、NameNode => Read or write files need to query the existence of metadata, all requests through the NameNode

    2、DataNode => File data storage

    3、MetaServer => Metadata is provided
```
### how to use?
The NameNode and DataNode and MetaServer services need to be started before they can be used。

``` text

    First configure dfs-site.properties

```

```shell

    1、cd dfs
    2、java Start.java
    3、java MetaServer.java
```


Starting NameNode and DataNode creates a _SUCCESS file in the configuration directory

At this point you can run
```shell
    
    java DFSClient.java
```

### Operation flow chart
![runing](https://github.com/basebase/dfs/blob/0.10.0/image/run.png?raw=true)

