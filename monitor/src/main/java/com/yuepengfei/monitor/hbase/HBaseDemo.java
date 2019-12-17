package com.yuepengfei.monitor.hbase;

import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class HBaseDemo {
    Configuration conf = null;
    Connection connection = null;

    @Before
    public void init(){

        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","192.168.240.131:2181");

        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("连接创建失败");
        }

    }

    @After
    public void close(){
        if (connection!=null){
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    //创建表
    @Test
    public void createTable() throws IOException {
        //获得操作表的对象
        Admin admin = connection.getAdmin();

        //确定表名字
        TableName student1 = TableName.valueOf("student1");
        //获得表描述器
        HTableDescriptor  desc= new HTableDescriptor(student1);
        //表描述器中添加列族
        HColumnDescriptor info1 = new HColumnDescriptor("info1");
        HColumnDescriptor info2 = new HColumnDescriptor("info2");
        desc.addFamily(info1);
        desc.addFamily(info2);
        //判断表是否存在
        if (!admin.tableExists(student1)){
            //创建表
            admin.createTable(desc);
        }
        admin.close();
    }
    //查看所有表
    @Test
    public void listTable() throws IOException {
        //获取操作表的对象
        Admin admin = connection.getAdmin();
        //获取表列表
        TableName[] tableNames = admin.listTableNames();
        for (TableName tableName : tableNames) {
            System.out.println(tableName);
        }
        admin.close();
    }

    //修改表
    @Test
    public void alterTable() throws IOException {
        //创建操作表的对象
        Admin admin = connection.getAdmin();
        TableName student1 = TableName.valueOf("student1");

        if(admin.tableExists(student1)){
            //修改前先将表置为不可用
            admin.disableTable(student1);

            //获取表描述器
            HTableDescriptor hTableDescriptor = admin.getTableDescriptor(student1);
            //不可以在new,只能采用上面的凡是获取
//            HTableDescriptor hTableDescriptor = new HTableDescriptor(student1);
            //移除一个列族
//            hTableDescriptor.remove("info1");
            //添加一个列族
            HColumnDescriptor info4 = new HColumnDescriptor("info4");
            hTableDescriptor.addFamily(info4);
            //提交修改后的表描述器
            admin.modifyTable(student1,hTableDescriptor);
        }
        admin.close();

    }

    //删除表
    @Test
    public void deleteTable() throws IOException {
        //获取操作表的对象
        Admin admin = connection.getAdmin();
        TableName student1 = TableName.valueOf("student1");

        if(admin.tableExists(student1)){
            //将表置为不可用
            admin.disableTable(student1);
            //删除学生表
            admin.deleteTable(student1);
        }

        admin.close();
    }

    //scan todo 其实过滤器的使用才是工作中常用方式
    @Test
    public void scan() throws IOException {
        //获取表操作对象
        TableName student = TableName.valueOf("user");
        Table table = connection.getTable(student);
        Scan scan = new Scan();
        ResultScanner results = table.getScanner(scan);
        for (Result result : results) {
            byte[] row = result.getRow();
            System.out.println(new String(row));
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.print("列族："+new String(CellUtil.cloneFamily(cell))+":/t");
                System.out.print(new String(CellUtil.cloneQualifier(cell))+":/t");
                System.out.println(new String(CellUtil.cloneValue(cell)));
            }
        }
        table.close();
    }
    //put
    @Test
    public void put() throws IOException {
        //当然标准的写法应该是先判断表是否存在
        TableName student = TableName.valueOf("student");
        Table table = connection.getTable(student);


        //创建put对象,参数为row kay
        Put put = new Put(Bytes.toBytes("0002"));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes("zhangsanfeng"));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes("30"));

        table.put(put);

        table.close();

    }

    //get
    @Test
    public void get() throws IOException {

        TableName student = TableName.valueOf("student");
        Table table = connection.getTable(student);
        //获取get对象，传入参数为row key
        Get get = new Get(Bytes.toBytes("0002"));

        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println("RowKey:"+
                    new String(CellUtil.cloneRow(cell))+"/t"+
                    new String(CellUtil.cloneFamily(cell)) +"/t"+
                    new String(CellUtil.cloneQualifier(cell))+"/t"+
                    new String(CellUtil.cloneValue(cell))
            );
        }

        table.close();

    }
    //delete
    @Test
    public void deleteData() throws IOException {
        //获取表操作对象
        TableName student = TableName.valueOf("student");
        Table table = connection.getTable(student);
        //获取delete操作对象
        Delete delete = new Delete(Bytes.toBytes("0002"));
        delete.addColumns(Bytes.toBytes("info"),Bytes.toBytes("age"));//删除所有版本
        //删除操作
        table.delete(delete);

        table.close();
    }
}
