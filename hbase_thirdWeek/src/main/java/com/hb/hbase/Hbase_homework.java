package com.hb.hbase;

import com.sun.tools.internal.xjc.reader.xmlschema.bindinfo.BIConversion;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.Array;

public class Hbase_homework {

    private static Connection connection = null;
    private static Admin admin = null;

    static {
        try {
            //获取配置信息
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum","127.0.0.1");
            conf.set("hbase.zookeeper.property.clientPort","2181");
            conf.set("hbase.master","127.0.0.1:60000");
            //创建连接
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (IOException e){
            e.printStackTrace();
        }
    }

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

    //加入数据，增加rowKey一个columnFamily的数据
    public static void put_data(String tablename, String rowkey, String columnFamily, String[] columnNames, String[] values) throws IOException {
        Put prkey = new Put(Bytes.toBytes(rowkey)); //rowkey
//        prkey.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName),Bytes.toBytes(value));

        System.out.println("tablename:"+tablename+";rowkey"+rowkey+";columnFamily:"+columnFamily);
        System.out.println("columnNames and values");
        for (int i=0;i<columnNames.length;i++){
            prkey.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnNames[i]),Bytes.toBytes(values[i]));
//            System.out.println(columnNames[i]+"\t"+values[i]);
        }
        connection.getTable(TableName.valueOf(tablename)).put(prkey);

    }

    public static void main(String[] args) throws IOException {
        System.out.println("第三周作业Hbase");
        String namespace = "huaibei";
        String tableName = "huaibei:student";
        String columnFamily1 = "info";
        String columnFamily2 = "score";

        //创建命名空间
//        create_namespace(namespace);
        //创建student表
//        create_tabel(tableName,columnFamily1,columnFamily2);

        //向表中插入数据
        String rowkey1 = "Tom";
        String rowkey2 = "Jerry2";
        String rowkey3 = "jack";
        String rowkey4 = "Rose";
        String rowkey5 = "huiabei";
        String[]  columnNameArray1 = {"student_id","class"};
        String[]  columnNameArray2 = {"understanding","programming"};

        String[] rowkey1columnValueFamily1 = {"20210000000001","1"};
        String[] rowkey1columnValueFamily2 = {"75","82"};

        String[] rowkey2columnValueFamily1 = {"20210000000002","1"};
        String[] rowkey2columnValueFamily2 = {"85","67"};

        String[] rowkey3columnValueFamily1 = {"20210000000003","2"};
        String[] rowkey3columnValueFamily2 = {"80","80"};

        String[] rowkey4columnValueFamily1 = {"20210000000004","2"};
        String[] rowkey4columnValueFamily2 = {"60","61"};

        String[] rowkey5columnValueFamily1 = {"20210000000005","3"};
        String[] rowkey5columnValueFamily2 = {"90","97"};

        //增加rowkey1的信息。
        put_data(tableName,rowkey1,columnFamily1 ,columnNameArray1,rowkey1columnValueFamily1);
        put_data(tableName,rowkey1,columnFamily2 ,columnNameArray2,rowkey1columnValueFamily2);
        //增加rowkey2的信息。
        put_data(tableName,rowkey2,columnFamily1 ,columnNameArray1,rowkey2columnValueFamily1);
        put_data(tableName,rowkey2,columnFamily2 ,columnNameArray2,rowkey2columnValueFamily2);
        //增加rowkey3的信息。
        put_data(tableName,rowkey3,columnFamily1 ,columnNameArray1,rowkey3columnValueFamily1);
        put_data(tableName,rowkey3,columnFamily2 ,columnNameArray2,rowkey3columnValueFamily2);
        //增加rowkey4的信息。
        put_data(tableName,rowkey4,columnFamily1 ,columnNameArray1,rowkey4columnValueFamily1);
        put_data(tableName,rowkey4,columnFamily2 ,columnNameArray2,rowkey4columnValueFamily2);
        //增加rowkey5的信息。
        put_data(tableName,rowkey5,columnFamily1 ,columnNameArray1,rowkey5columnValueFamily1);
        put_data(tableName,rowkey5,columnFamily2 ,columnNameArray2,rowkey5columnValueFamily2);

    }



//    public void class_check() throws IOException {
//        System.out.println("课堂演示.");
//
//        //第一步创建配置conf
//        Configuration conf =  new HBaseConfiguration();
////        Configuration conf =  new HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum", "127.0.0.1");
//        conf.set("hbase.zookeeper.property.clientPort", "2181");
//        conf.set("hbase.master", "127.0.0.1:60000");
//        //第二步建立连接
//        Connection connection = ConnectionFactory.createConnection(conf);
//        Admin admin = connection.getAdmin();
//        //第三步建表和插入数据
//        TableName test_hbase = TableName.valueOf("Hbtest_Hbase");
//        String colFamily = "User";
//        int rowKey = 1;
//        if(admin.tableExists(test_hbase)){
//            System.out.println(test_hbase.toString()+" exists.");
//        }
//        else{
//            //建表
//            HTableDescriptor hTableDescriptor = new HTableDescriptor(test_hbase);
//            // 建列族
//            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(colFamily);
//            hTableDescriptor.addFamily(hColumnDescriptor);
//            admin.createTable(hTableDescriptor);
//            System.out.println("\n"+"Table create successful");
//        }
//
////        插入数据
////        得到行的信息
////        Put put = new Put(Bytes.toBytes(rowKey));  // rowKey
////        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes("uid"), Bytes.toBytes("001"));
////        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes("name`"), Bytes.toBytes("Tom"));
////        connection.getTable(test_hbase).put(put);
////        System.out.println("Data insert success");
//
//        //查询数据
////        Get get = new Get(Bytes.toBytes(rowKey));
////        if(!get.isCheckExistenceOnly()){
////            Result result = connection.getTable(test_hbase).get(get);
////            for (Cell cell : result.rawCells()){
////                String ColName =  Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
////                String value = Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
////                System.out.println("Data get success, colName: " + ColName + ", value: " + value);
////            }
////        }
////
////        //删除数据
////        Delete delete = new Delete(Bytes.toBytes(rowKey));
////        connection.getTable(test_hbase).delete(delete);
////        System.out.println("Delete Success");
////
////
////        //查询数据表
////        Get get2 = new Get(Bytes.toBytes(rowKey));
////        if(!get2.isCheckExistenceOnly()){
////            Result result = connection.getTable(test_hbase).get(get2);
////            for (Cell cell : result.rawCells()){
////                String ColName =  Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
////                String value = Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
////                System.out.println("Data get success, colName: " + ColName + ", value: " + value);
////            }
////        }
//
//        //删除数据
////        System.out.println("删除表：");
////        admin.disableTable(test_hbase);
////        admin.deleteTable(test_hbase);
//
//
//    }

}
