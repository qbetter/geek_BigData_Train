1，先配置信息以及创建连接  
`            //获取配置信息
             Configuration conf = HBaseConfiguration.create();
             conf.set("hbase.zookeeper.quorum","127.0.0.1");
             conf.set("hbase.zookeeper.property.clientPort","2181");
             conf.set("hbase.master","127.0.0.1:60000");
             //创建连接
             connection = ConnectionFactory.createConnection(conf);
             admin = connection.getAdmin();`  
主要的操作都是通过connection和admin完成的  
2，创建命名空间

     

执行的结果：


