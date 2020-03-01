package com.yuepengfei.monitor.hbase;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class HBaseUtils {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseUtils.class);

    static {
        PropertyConfigurator.configureAndWatch("log4j.properties", 10000);
    }

    // 加锁变量，避免锁整个类
    private static Object object = new Object();
    private static org.apache.hadoop.conf.Configuration conf;
    private static HConnection connection;

    public static HConnection getConnection() throws IOException {
        if (null == connection) {
            synchronized (object) {
                if (null == connection) {
                    conf = HBaseConfiguration.create();
                    System.getProperties().setProperty("HADOOP_USER_NAME",
                            "HADOOP_USER_NAME");
                    System.getProperties().setProperty("HADOOP_GROUP_NAME",
                            "HADOOP_GROUP_NAME");
                    conf.set("hbase.zookeeper.quorum", "HBASE_ZK_QUORUM");
                    conf.set("hbase.zookeeper.property.clientPort",
                            "HBASE_ZK_PROP_PORT");
                    conf.set("zookeeper.znode.parent", "HBASE_ZK_ZNODE_PARENT");
                    // 重试次数，默认为14，可配置为3
                    conf.set("hbase.client.retries.number", "2");
                    // 重试的休眠时间，默认为1s，可减少，比如100ms
                    conf.set("hbase.client.pause", "100");
                    // zk重试的休眠时间，默认为1s，可减少，比如：200ms
                    conf.set("zookeeper.recovery.retry.intervalmill", "200");
                    conf.set("ipc.socket.timeout", "2000");
                    conf.set("hbase.rpc.timeout", "2000");
                    conf.set("hbase.client.scanner.timeout.period", "2000");
                    conf.set("hbase.client.operation.timeout", "5000");
                    connection = HConnectionManager.createConnection(conf);
                    LOG.info("1.connect hbase success!" + "HBASE_ZK_QUORUM");
                }
            }
        }
        return connection;
    }

    public static HConnection getConnection(String hadoopUserName, String hadoopGroupName, String hbaseZkQuorum,
                                            String hbaseZkPropPort, String hbaseZkZnodeParent) throws IOException {
        HConnection connection = null;
        synchronized (object) {
            conf = HBaseConfiguration.create();
            System.getProperties().setProperty("HADOOP_USER_NAME", hadoopUserName);
            System.getProperties().setProperty("HADOOP_GROUP_NAME", hadoopGroupName);
            conf.set("hbase.zookeeper.quorum", hbaseZkQuorum);
            conf.set("hbase.zookeeper.property.clientPort", hbaseZkPropPort);
            conf.set("zookeeper.znode.parent", hbaseZkZnodeParent);
            // 重试次数，默认为14，可配置为3
            conf.set("hbase.client.retries.number", "2");
            // 重试的休眠时间，默认为1s，可减少，比如100ms
            conf.set("hbase.client.pause", "100");
            // zk重试的休眠时间，默认为1s，可减少，比如：200ms
            conf.set("zookeeper.recovery.retry.intervalmill", "200");
            conf.set("ipc.socket.timeout", "2000");
            conf.set("hbase.rpc.timeout", "2000");
            conf.set("hbase.client.scanner.timeout.period", "2000");
            conf.set("hbase.client.operation.timeout", "5000");
            connection = HConnectionManager.createConnection(conf);
            LOG.info("2.connect hbase success!" + hbaseZkQuorum);
        }
        return connection;
    }

    /**
     * 根据key查询hbase数据库
     *
     * @param tabName
     * @param key
     * @return
     * @throws IOException
     */
    public static Result selectByKey(String tabName, String key) throws IOException {
        Result result = null;
        HTableInterface table = (HTableInterface) getConnection().getTable(tabName);
        Get get = new Get(Bytes.toBytes(key));
        result = table.get(get);
        table.close();
        return result;
    }

    public static Result selectByKey(HConnection hConnection, String tabName, String key) throws IOException {
        Result result = null;
        if (null != hConnection) {
            HTableInterface table = (HTableInterface) hConnection.getTable(tabName);
            Get get = new Get(Bytes.toBytes(key));
            result = table.get(get);
            table.close();
        }
        return result;
    }

    /**
     * 查询hbase表
     *
     * @param tabName
     * @return
     * @throws IOException
     */
    public static ResultScanner scan(String tabName) throws IOException {
        HTableInterface table = getConnection().getTable(tabName);
        Scan scan = new Scan();
        ResultScanner rs = table.getScanner(scan);
        return rs;
    }

    public static ResultScanner scan(HConnection hConnection, String tabName) throws IOException {
        ResultScanner rs = null;
        if (null != hConnection) {
            HTableInterface table = hConnection.getTable(tabName);
            Scan scan = new Scan();
            rs = table.getScanner(scan);
        }
        return rs;
    }

    /**
     * 往hbase数据库中插入单条数据
     *
     * @param tabName
     * @param rowKey
     * @param colFamily
     * @param colName
     * @param map
     * @throws IOException
     */
    public static void insert(String tabName, String rowKey, String colFamily, String colName, Map<String, String> map)
            throws IOException {
        JSONObject obj = new JSONObject();
        HTable table = (HTable) getConnection().getTable(tabName);
        Put put = new Put(Bytes.toBytes(rowKey));
        for (Entry<String, String> entry : map.entrySet()) {
            obj.put(entry.getKey(), entry.getValue());
        }
        put.add(Bytes.toBytes(colFamily), Bytes.toBytes(colName), Bytes.toBytes(obj.toString()));
        table.put(put);
        table.flushCommits();
        table.close();
    }

    public static void insert(HConnection hConnection, String tabName, String rowKey, String colFamily, String colName,
                              Map<String, String> map) throws IOException {
        JSONObject obj = new JSONObject();
        HTable table = (HTable) hConnection.getTable(tabName);
        Put put = new Put(Bytes.toBytes(rowKey));
        for (Entry<String, String> entry : map.entrySet()) {
            obj.put(entry.getKey(), entry.getValue());
        }
        put.add(Bytes.toBytes(colFamily), Bytes.toBytes(colName), Bytes.toBytes(obj.toString()));
        table.put(put);
        table.flushCommits();
        table.close();
    }

    public static void insert(String tabName, String rowKey, String colFamily, String colName, JSONObject obj)
            throws IOException {
        HTable table = (HTable) getConnection().getTable(tabName);
        Put put = new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes(colFamily), Bytes.toBytes(colName), Bytes.toBytes(obj.toString()));
        table.put(put);
        table.flushCommits();
        table.close();
    }

    /**
     * 单条json插入hbase表中
     *
     * @param hConnection
     * @param tabName
     * @param rowKey
     * @param colFamily
     * @param colName
     * @param obj
     * @return
     * @throws IOException
     */
    public static boolean insert(HConnection hConnection, String tabName, String rowKey, String colFamily,
                                 String colName, JSONObject obj) throws IOException {
        boolean flg = false;
        if (null != hConnection) {
            HTable table = (HTable) hConnection.getTable(tabName);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(colFamily), Bytes.toBytes(colName), Bytes.toBytes(obj.toString()));
            table.put(put);
            table.flushCommits();
            table.close();
            flg = true;
        }
        return flg;
    }

    public static void insert(String tabName, String rowKey, String colFamily, String colName, String str)
            throws IOException {
        HTable table = (HTable) getConnection().getTable(tabName);
        Put put = new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes(colFamily), Bytes.toBytes(colName), Bytes.toBytes(str));
        table.put(put);
        table.flushCommits();
        table.close();
    }

    public static void insert(String tabName, String rowKey, String colFamily, String colName, String str,long version)
            throws IOException {
        HTable table = (HTable) getConnection().getTable(tabName);
        Put put = new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes(colFamily), Bytes.toBytes(colName),version, Bytes.toBytes(str));
        table.put(put);
        table.flushCommits();
        table.close();
    }

    public static void insert(HConnection hConnection, String tabName, String rowKey, String colFamily, String colName,
                              String str) throws IOException {
        HTable table = (HTable) hConnection.getTable(tabName);
        Put put = new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes(colFamily), Bytes.toBytes(colName), Bytes.toBytes(str));
        table.put(put);
        table.flushCommits();
        table.close();
    }

    /**
     * 批量往hbase插入数据
     *
     * @param tabName
     * @param colFamily
     * @param colName
     * @param map
     * @throws IOException
     */
    public static void bachInsert(String tabName, String colFamily, String colName,
                                  Map<String, Map<String, String>> map) throws IOException {

        HTable table = (HTable) getConnection().getTable(tabName);
        table.setAutoFlush(false, true);
        table.setWriteBufferSize(24L * 1024L * 1024L);
        List<Put> list = new ArrayList<Put>();
        int index = 0;
        for (Entry<String, Map<String, String>> entry : map.entrySet()) {
            Put put = new Put(Bytes.toBytes(entry.getKey()));
            Map<String, String> mapInfo = entry.getValue();
            JSONObject obj = new JSONObject();
            for (Entry<String, String> entryItem : mapInfo.entrySet()) {
                obj.put(entryItem.getKey(), entryItem.getValue());
            }
            put.add(Bytes.toBytes(colFamily), Bytes.toBytes(colName), Bytes.toBytes(obj.toString()));
            list.add(put);
            if (list.size() % 1000 == 0) {
                table.put(list);
                list.clear();
                table.flushCommits();
                LOG.info("第" + (index + 1) + "次插入hbase");
            }
        }
        if (list.size() > 0) {
            table.put(list);
            list.clear();
            table.flushCommits();
            table.close();
            LOG.info("第" + (index + 1) + "次插入hbase");
        }

    }

    /**
     * 删除对应表的主键数据
     *
     * @param tabName
     * @param rowKey
     * @throws IOException
     */
    public static void deleteByKey(String tabName, String rowKey) throws IOException {
        HTable table = (HTable) getConnection().getTable(tabName);
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
    }

    public static void deleteByKey(HConnection hConnection, String tabName, String rowKey) throws IOException {
        if (null != hConnection) {
            HTable table = (HTable) hConnection.getTable(tabName);
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
        }
    }


    /**
     * 获取多版本结果
     * @param tabName
     * @param key
     * @param limit
     * @return
     * @throws IOException
     */
    public static Result selectMultiVersionByKey(String tabName, String key, int limit)
            throws IOException {
        HTableInterface table = (HTableInterface) getConnection().getTable(tabName);
        Get get = new Get(Bytes.toBytes(key));
        get.setMaxVersions();
        get.setMaxResultsPerColumnFamily(limit);
        Result result = table.get(get);
        table.close();
        return result;
    }


    /**
     * 获取多版本结果
     * @param hConnection
     * @param tabName
     * @param key
     * @param limit
     * @return
     * @throws IOException
     */
    public static Result selectMultiVersionByKey(HConnection hConnection, String tabName, String colFamily, String colName, String key, int limit)
            throws IOException {
        HTableInterface table = (HTableInterface) getConnection().getTable(tabName);
        if (hConnection != null) {
            table = hConnection.getTable(tabName);
        }
        Get get = new Get(Bytes.toBytes(key));
        get.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(colName));
        get.setMaxVersions();
        get.setMaxResultsPerColumnFamily(limit);
        Result result = table.get(get);
        table.close();
        return result;
    }

    /**
     * 删除指定列族下的指定列的指定版本的数据
     *
     * @param tabName
     * @param rowKey
     * @param family
     * @param timestamp
     * @throws IOException
     */
    public static void deleteByKeyFamilyQualifierTs(String tabName, String rowKey, String family, long timestamp) throws IOException {
        HTable table = (HTable) getConnection().getTable(tabName);
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.deleteFamilyVersion(Bytes.toBytes(family),timestamp);
        table.delete(delete);
    }

    /**
     * 批量往hbase插入数据
     * @param hConnection
     * @param tabName
     * @param colFamily
     * @param insertList
     * @throws IOException
     */
    public static void bachMultiVersionInsert(HConnection hConnection, String tabName, String colFamily,
                                              List<String> insertList) throws IOException {

        // 连接默认hbase
        HTableInterface table = (HTableInterface) getConnection().getTable(tabName);
        if(hConnection != null){
            table = (HTable) hConnection.getTable(tabName);
        }
        table.setAutoFlush(false, true);
        table.setWriteBufferSize(24L * 1024L * 1024L);
        List<Put> list = new ArrayList<Put>();
        int index = 0;
        long ts = System.currentTimeMillis();
        for (int i = 0; i <insertList.size() ; i++) {
            String insertStr = insertList.get(i);
            String[] insertStrArr = insertStr.split("#");
            String rowKey = insertStrArr[0];
            String value = insertStrArr[1];
            String colName = insertStrArr[2];

            Put put = null;
            String mdKey = null;
            try {
                mdKey = MD5Utils.evaluate(rowKey);
            } catch (UnsupportedEncodingException e) {
                LOG.error("evaluate failed-->", e);
            } catch (NoSuchAlgorithmException e) {
                LOG.error("evaluate failed-->", e);
            }

            put = new Put(Bytes.toBytes(mdKey));
            put.add(Bytes.toBytes(colFamily), Bytes.toBytes(colName), ts, Bytes.toBytes(value));
            list.add(put);
            ts++;
            if (list.size() % 1000 == 0) {
                table.put(list);
                list.clear();
                table.flushCommits();
//                LOG.info("第" + (index + 1) + "次插入hbase");
            }
        }
        if (list.size() > 0) {
            table.put(list);
            list.clear();
            table.flushCommits();
            table.close();
//            LOG.info("第" + (index + 1) + "次插入hbase");
        }
    }

    /**
     * 批量往hbase插入数据
     *
     * @param tabName
     * @param colFamily
     * @param map
     * @throws IOException
     */
    public static void batchInsertColumns(HConnection hConnection, String tabName,
                                          String colFamily, Map<String, Map<String, String>> map) throws IOException {

        HTable table = (HTable) hConnection.getTable(tabName);
        table.setAutoFlush(false, true);
        table.setWriteBufferSize(24 * 1024 * 1024L);
        List<Put> list = new ArrayList<Put>();
        int index = 0;
        for (Map.Entry<String,Map<String,String>> entryItem : map.entrySet())
        {
            String rowKey = entryItem.getKey();
            if( null == rowKey || rowKey.length() == 0 ) {
                LOG.error("bachInsertColumns for rowKey is:" + rowKey);
                continue;
            }
            Put put = new Put(Bytes.toBytes(rowKey));
            Map<String, String> mapInfo = entryItem.getValue();
            for (Map.Entry<String,String> entry : mapInfo.entrySet())
            {
                String key = entry.getKey();
                if(null == key || key.length() == 0 ){
                    LOG.error("bachInsertColumns for child key is:" + rowKey);
                    continue;
                }
                //table+key+data
                put.add(Bytes.toBytes(colFamily), Bytes.toBytes(key), Bytes.toBytes(entry.getValue()));
                list.add(put);
            }

            if (list.size() % 1000 == 0) {
                table.put(list);
                list.clear();
                table.flushCommits();
                LOG.info("第" + (index + 1) + "次插入hbase");
            }
        }
        if (list.size() > 0) {
            table.put(list);
            list.clear();
            table.flushCommits();
            table.close();
            LOG.info("第" + (index + 1) + "次插入hbase");
        }

    }

}
