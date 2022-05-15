# geek_bigdata_homework（持续更新中）

·目前仅在本机搭建伪分布式进行了本地开发

·本机的hadoop版本为2.7.3、hbase版本为2.1.6

## Week2 作业 核心代码及结果展示

作业1、统计每一个手机号耗费的总上行流量、下行流量、总流量

1>、核心代码

·在sshdfs项目的src/main/java/mapreduce包下有4个类，分别为：FlowBean（实体类）、FlowBeanMapper（用于切分字符串，将非结构化数据转化为结构化数据）、FlowBeanReducer（按Key对Value进行累加）、FlowBeanDriver（整合整个mapreduce流程、打包后最终通过hadoop命令运行在yarn上的类）

·此处列出FlowBeanDriver的代码

    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME","root");

        Path inputPath = new Path("hdfs://127.0.0.1:9000/user/hive/extwarehouse/ods/HTTP_20130313143750.dat");
        Path outputPath = new Path("hdfs://127.0.0.1:9000/user/hive/extwarehouse/output");

        //作为整个Job的配置
        Configuration conf = new Configuration();

        // 1. 设置job运行时没要访问的默认文件系统】
        conf.set("fs.dafaultFS","hdfs://127.0.0.1:9000");
        // 2. 设置job提交到哪里去运行
        conf.set("mapreduce.framrwork.name","yarn");

        FileSystem fs= FileSystem.get(conf);

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);//保证输出目录不存在
        }

        // ①创建Job
        Job job = Job.getInstance(conf);

        // 1.封装参数:jar包所在位置
        job.setJarByClass(FlowBeanDriver.class);

        // ②设置Job
        // 设置Job运行的Mapper，Reducer类型，Mapper,Reducer输出的key-value类型
        job.setMapperClass(FlowBeanMapper.class);
        job.setReducerClass(FlowBeanReducer.class);

        // Job需要根据Mapper和Reducer输出的Key-value类型准备序列化器，通过序列化器对输出的key-value进行序列化和反序列化
        // 如果Mapper和Reducer输出的Key-value类型一致，直接设置Job最终的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 设置输入目录和输出目录
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // ③运行Job
        // 封装参数: 想要启动reduce task的数量
        job.setNumReduceTasks(1);

        // 提交job给yarn
        boolean res = job.waitForCompletion(true);

        System.exit(res?0:-1);
    }

2>、结果展示

<img width="1197" alt="截屏2022-05-16 上午12 12 11" src="https://user-images.githubusercontent.com/19924975/168482673-40822857-fda5-4632-8320-a3b13667e967.png">

<img width="691" alt="截屏2022-05-16 上午12 15 00" src="https://user-images.githubusercontent.com/19924975/168482796-6863dee8-18b7-494c-8978-daef097e874f.png">

作业2、使用 Java API 操作 HBase 建表，实现插入数据，删除数据，查询等功能。

1>、核心代码（在hbase-demo项目的src/main/java/hbase/demo包下的 HBaseClientCURD.java中）

·先创建命名空间 create_namespace 'quchenlong'

·然后依次进行了建表、插入、删除、查询等操作

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
    

2>、结果展示（此处仅以查询方法的输出结果进行展示）

·查询的的代码

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
·输出的结果

KV: quchenlong

Family Name: info

Column Name: class

Value : 3

KV: quchenlong

Family Name: info

Column Name: student_id

Value : 20220735030018

KV: quchenlong

Family Name: score

Column Name: programming

Value : 77

KV: quchenlong

Family Name: score

Column Name: understanding

Value : 66
