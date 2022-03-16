1，先配置信息以及创建连接  

            Configuration conf = HBaseConfiguration.create();  
            conf.set("hbase.zookeeper.quorum","127.0.0.1");  
            conf.set("hbase.zookeeper.property.clientPort","2181");  
            conf.set("hbase.master","127.0.0.1:60000");  
            //创建连接  
            connection = ConnectionFactory.createConnection(conf);  
            admin = connection.getAdmin();  

主要的操作都是通过connection和admin完成的  

2，创建命名空间

    //创建命名空间
    public static void create_namespace(String namespace){
        //创建命名空间描述器
        NamespaceDescriptor build = NamespaceDescriptor.create(namespace).build();
        //创建空间
        try{
            admin.createNamespace(build);
        } catch (NamespaceExistException ne){
            System.out.println("命名空间"+namespace+"已经存在.");
            ne.printStackTrace();
        }
        catch (IOException e) {
            e.printStackTrace();
        }

    }

3,创建表

    //创建表
    public static void create_tabel(String tablename,String ... columnFamily) throws IOException {
        TableName tableName = TableName.valueOf(tablename);
        if (admin.tableExists(tableName)){
            System.out.println("表"+tablename+"已经存在.");
        }
        else {
            //创建表描述器
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            //创建列描述器
            for (String cn :columnFamily ){
                hTableDescriptor.addFamily(new HColumnDescriptor(cn));
            }
            admin.createTable(hTableDescriptor);
            System.out.println("Table "+tablename+" create successful");
        }
    }
    
   
4，加入数据，增加rowKey一个columnFamily的数据
   
     //加入数据，增加rowKey一个columnFamily的数据
    public static void put_data(String tablename, String rowkey, String columnFamily, String[] columnNames, String[] values) throws IOException {
        Put prkey = new Put(Bytes.toBytes(rowkey)); //rowkey
        System.out.println("tablename:"+tablename+";rowkey"+rowkey+";columnFamily:"+columnFamily);
        System.out.println("columnNames and values");
        for (int i=0;i<columnNames.length;i++){
            prkey.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnNames[i]),Bytes.toBytes(values[i]));
        }
        connection.getTable(TableName.valueOf(tablename)).put(prkey);
    } 


5，结果如下所示：
![image](https://user-images.githubusercontent.com/21261099/158672092-52300dfc-5fef-470d-b5d7-fa16081497d9.png)



