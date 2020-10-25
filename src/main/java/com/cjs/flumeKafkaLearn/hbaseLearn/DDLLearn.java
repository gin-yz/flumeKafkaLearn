/*
 * JDK1.8下，１１报错
 *
 * */

package com.cjs.flumeKafkaLearn.hbaseLearn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;
import java.io.InvalidObjectException;

public class DDLLearn {

    private static HBaseAdmin admin;
    private static Connection connection;

    static {
        //配置连接
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop1,hadoop2,hadoop3");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            //搞一个工厂创建ｃｏｎｎｅｃｔｉｏｎ对象
            connection = ConnectionFactory.createConnection(conf);
            //ｄｄｌ操作使用getAdmin()
            admin = (HBaseAdmin) connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    //关闭连接
    public static void close() {
        try {
            admin.close();
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //判断表是否存在
    public static boolean isTableExist(String tableName) throws IOException {

        boolean exists = admin.tableExists(TableName.valueOf(tableName));

        return exists;
    }

    /**
     * 创建表
     * @param tableName 表名
     * @param columnFamily　列族，数组
     * @throws IOException 呵呵
     */
    public static void createTable(String tableName, String[] columnFamily) throws IOException {
        if (admin.tableExists(tableName) | columnFamily.length <= 0) throw new InvalidObjectException("呵呵");

        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for (String s : columnFamily) {

            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(s);
            hTableDescriptor.addFamily(hColumnDescriptor);

        }

        admin.createTable(hTableDescriptor);
    }

    //删除表
    public static void deleteTable(String tableName) throws IOException {
        if (!admin.tableExists(tableName)) throw new InvalidObjectException("呵呵");

        admin.disableTable(tableName);
        admin.deleteTable(tableName);
    }

    //创建命名空间
    public static void createNameSpace(String nameSpace){
        NamespaceDescriptor.Builder builder = NamespaceDescriptor.create(nameSpace);
        NamespaceDescriptor build = builder.build();

        try {
            admin.createNamespace(build);

        }catch (NamespaceExistException e){
            System.out.println("namespace已经存在");
        } catch (IOException e) {
            e.printStackTrace();
        }
        //当表有重复的时候进行实验
        System.out.println("哈哈哈，即使有异常，我依然能执行");
        //删除命名空间
//        admin.deleteNamespace(nameSpace);
    }

    //增加列族
    public static void addFamilyColumn(String tableName,String familyColumn) throws IOException {
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(familyColumn);

        admin.addColumn(TableName.valueOf(tableName),hColumnDescriptor);
    }


    public static void main(String[] args) throws IOException {
        addFamilyColumn("cjstest","info2");
//        boolean student = isTableExist("student");
//        System.out.println(student);

//        createTable("cjs", new String[]{"info","info1"});
//        deleteTable("cjs");

//        createNameSpace("bigdata");

        close();

    }
}
