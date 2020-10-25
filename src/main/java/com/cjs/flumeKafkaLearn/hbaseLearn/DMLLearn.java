package com.cjs.flumeKafkaLearn.hbaseLearn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class DMLLearn {
    private static Connection connection;

    static {
        //配置连接
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop1,hadoop2,hadoop3");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            //搞一个工厂创建ｃｏｎｎｅｃｔｉｏｎ对象
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //关闭连接
    public static void close() {
        try {
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 插入或者修改数据(修改数据直接指定原rowKey，以及新值就行)
     *
     * @param tableName    表名
     * @param rowKey       　键值
     * @param columnFamily 　列族
     * @param column       　列
     * @param value        　值
     */
    public static void putData(String tableName, String rowKey, String columnFamily, String column, String value) {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));

            Put put = new Put(Bytes.toBytes(rowKey));

            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));

            table.put(put);

            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    //对应命令行的scan
    public static void scanAll(String tableName) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));

            //可以加各种过滤器，具体过滤器请百度
            Scan scan = new Scan();

            scan.setMaxVersions(10);

            ResultScanner scanner = table.getScanner(scan);

            for (Result result : scanner) {

                for (Cell cell : result.rawCells()) {
                    System.out.println("rowFamily:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                            ",clumn:" + Bytes.toString(CellUtil.cloneQualifier(cell))+",value:"+Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //读数据，对应命令行get操作
    public static void getValue(String tableName,String rowKey){
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));

            Get get = new Get(Bytes.toBytes(rowKey));

            //限定列族或者列
//            get.addFamily(Bytes.toBytes("info"));
//            get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"));

            Result result = table.get(get);

            for (Cell cell:result.rawCells()){
                System.out.println("rowFamily:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                        ",clumn:" + Bytes.toString(CellUtil.cloneQualifier(cell))+",value:"+Bytes.toString(CellUtil.cloneValue(cell)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //删除数据
    public static void deleteValue(String tableName,String rowKey,String columnFamily,String column){

        try {
            Table table = connection.getTable(TableName.valueOf(Bytes.toBytes(tableName)));

            //在１．３．１版本中，如果只指定rowKey,则打DeleteFamily标识，按照columnFamily来删，因为是按照列族来存的，方便批量删除．
            Delete delete = new Delete(Bytes.toBytes(rowKey));

            //若指定了列，这打上deleteColumn标记
            //指定列的数据全部删掉，不管有多少versions,后可接时间戳，表示小于等于时间撮的版本全部干掉
            delete.addColumns(Bytes.toBytes(columnFamily),Bytes.toBytes(column));

            //只干掉最新版本(version)最大的，若指定了时间戳,只删除那个时间撮下的值
            //此方法打DELETE标识，不会删除数据，只会打标识,在versions设置为１时，若缓存里面还有旧值（未刷写到ｈｄｆｓ），则会出现旧的值"复活"的现象
//            delete.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(column),1603466525211L);

            //指定列族,打DeleteFamily标识
//            delete.addFamily(Bytes.toBytes(columnFamily));

            table.delete(delete);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    public static void main(String[] args) {
//        putData("cjstest","428","info","name","cjs1");
//        scanAll("student");
//        getValue("student","428");
        deleteValue("cjstest","428","info","name");
        close();
    }
}
