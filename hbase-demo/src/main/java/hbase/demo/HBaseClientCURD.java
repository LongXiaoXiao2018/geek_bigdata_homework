package hbase.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

public class HBaseClientCURD {

    public static Connection getConn() throws IOException {

        Configuration conf = HBaseConfiguration.create(); // 会自动加载hbase-site.xml
        conf.set("hbase.zookeeper.quorum","localhost:2181"); //如不告知则无法连接
        return ConnectionFactory.createConnection(conf);

    }

    public static void testCreateTable(String tableName, List<String> columnFamilies) throws IOException {

        // 从连接中构造一个DDL操作器
        Admin admin = getConn().getAdmin();

        // 创建一个表定义描述对象
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

        // 创建列族定义描述对象
        columnFamilies.forEach(s->hTableDescriptor.addFamily(new HColumnDescriptor(s)));

        // 用ddl操作器对象: admin来建表
        admin.createTable(hTableDescriptor);

        admin.close();
        getConn().close();

    }

    public static void testPut(String tableName,String rowKey,String colFamily,String colKey,String value) throws Exception{

        Table table = getConn().getTable(TableName.valueOf(tableName));

        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(colKey),Bytes.toBytes(value));

        table.put(put);

        table.close();
        getConn().close();

    }

    public static void testGet(String tableName,String rowKey,String colFamily,String colKey) throws Exception{

        Table table = getConn().getTable(TableName.valueOf(tableName));

        Get get = new Get(rowKey.getBytes());

        Result result = table.get(get);

        //从用户结果中指定某个key的value  取一个值
        result.getValue(colFamily.getBytes(),colKey.getBytes());

        // 遍历所有值
        CellScanner cellScanner = result.cellScanner();
        while(cellScanner.advance()){
            Cell cell = cellScanner.current();

            byte[] rowArray = cell.getRowArray();// current kv
            byte[] familyArray = cell.getFamilyArray();  // 列族名
            byte[] qualiferArray = cell.getQualifierArray(); //列名
            byte[] valueArray = cell.getValueArray(); // value名

            System.out.println("KV: " + new String(rowArray,cell.getRowOffset(),cell.getRowLength()));
            System.out.println("Family Name: " + new String(familyArray,cell.getFamilyOffset(),cell.getFamilyLength()));
            System.out.println("Column Name: " + new String(qualiferArray,cell.getQualifierOffset(),cell.getQualifierLength()));
            System.out.println("Value : " + new String(valueArray,cell.getValueOffset(),cell.getValueLength()));

        }

    }

    public static void testDelete(String tableName,String rowKey) throws Exception{

        Table table = getConn().getTable(TableName.valueOf(tableName));

        Delete delete1 = new Delete(Bytes.toBytes(rowKey));

        ArrayList<Delete> dels = new ArrayList<>();

        dels.add(delete1);

        table.delete(dels);
        table.close();
        getConn().close();

    }

    public static void main(String[] args) throws Exception {

        String tableName = "quchenlong:student";

        List<String> columnFamilies = new ArrayList<>();
        columnFamilies.add("info");
        columnFamilies.add("score");

        // 创建表
        testCreateTable(tableName,columnFamilies);

        String OP_ROW_KEY = "quchenlong";

        String OP_DELETE_TEST = "delete_test";

        // 插入
        Map<String,List<Long>> dataMap = new HashMap<>();
        dataMap.put("Tom", Arrays.asList(20210000000001L, 1L, 75L, 82L));
        dataMap.put("Jerry", Arrays.asList(20210000000002L, 1L, 85L, 67L));
        dataMap.put("Jack", Arrays.asList(20210000000003L, 2L, 80L, 80L));
        dataMap.put("Rose", Arrays.asList(20210000000004L, 2L, 60L, 61L));
        dataMap.put(OP_ROW_KEY, Arrays.asList(20220735030018L, 3L, 66L, 77L));
        dataMap.put(OP_DELETE_TEST, Arrays.asList(0L, 3L, 66L, 77L));

        dataMap.forEach((k,v)->{
            try {
                testPut(tableName,k,"info","student_id",v.get(0).toString());
                testPut(tableName,k,"info","class",v.get(1).toString());
                testPut(tableName,k,"score","understanding",v.get(2).toString());
                testPut(tableName,k,"score","programming",v.get(3).toString());
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        // 查询
        testGet(tableName,OP_ROW_KEY,"info","student_id");

        // 删除
        testDelete(tableName,OP_DELETE_TEST);

        // 查询
        testGet(tableName,OP_ROW_KEY,"info","student_id");

    }

}
