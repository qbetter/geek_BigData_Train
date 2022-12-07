package com.hb.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class Hbase_homework {

    public static void main(String[] args) throws IOException {

        Hbase_homework hhh = new Hbase_homework();
        hhh.class_check();

    }

    public void class_check() throws IOException {
        System.out.println("课堂演示.");

        //第一步创建配置conf
        Configuration conf =  new HBaseConfiguration();
//        Configuration conf =  new HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "127.0.0.1");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.master", "127.0.0.1:60000");
        //第二步建立连接
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        //第三步建表和插入数据
        TableName test_hbase = TableName.valueOf("Hbtest_Hbase_new");
        String colFamily = "User";
        int rowKey = 1;
        if(admin.tableExists(test_hbase)){
            System.out.println(test_hbase.toString()+" exists.");
        }
        else{
            //建表
            HTableDescriptor hTableDescriptor = new HTableDescriptor(test_hbase);
            // 建列族
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(colFamily);
            hTableDescriptor.addFamily(hColumnDescriptor);
            admin.createTable(hTableDescriptor);
            System.out.println("\n"+"Table create successful");
        }

//        插入数据
//        得到行的信息
        Put put = new Put(Bytes.toBytes(rowKey));  // rowKey
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes("uid"), Bytes.toBytes("001"));
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes("name`"), Bytes.toBytes("Tom"));
        connection.getTable(test_hbase).put(put);
        System.out.println("Data insert success");

        //查询数据
        Get get = new Get(Bytes.toBytes(rowKey));
        if(!get.isCheckExistenceOnly()){
            Result result = connection.getTable(test_hbase).get(get);
            for (Cell cell : result.rawCells()){
                String ColName =  Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
                System.out.println("Data get success, colName: " + ColName + ", value: " + value);
            }
        }

        //删除数据
//        Delete delete = new Delete(Bytes.toBytes(rowKey));
//        connection.getTable(test_hbase).delete(delete);
//        System.out.println("Delete Success");
//
//
//        //查询数据表
//        Get get = new Get(Bytes.toBytes(rowKey));
//        if(!get.isCheckExistenceOnly()){
//            Result result = connection.getTable(test_hbase).get(get);
//            for (Cell cell : result.rawCells()){
//                String ColName =  Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
//                String value = Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
//                System.out.println("Data get success, colName: " + ColName + ", value: " + value);
//            }
//        }

//        //删除数据
//        System.out.println("删除表：");
//        admin.disableTable(test_hbase);
//        admin.deleteTable(test_hbase);


    }

}
