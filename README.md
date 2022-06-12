# geek_bigdata_homework（持续更新中）

## Week6 环境

·基于本地IDEA进行开发与测试

## Week6 作业 核心代码及结果展示

作业一：使用 RDD API 实现带词频的倒排索引

核心代码：

    object invertedIndexTask {
        def main(args: Array[String]): Unit = {
        
            val input = this.getClass.getResource("/") + "data"
        
            val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
            val sc = new SparkContext(sparkConf)
            sc.setLogLevel("WARN")
            //1.获取hadoop操作文件的api
            val fs = FileSystem.get(sc.hadoopConfiguration)
            //2.读取目录下的文件，并生成列表
            val filelist = fs.listFiles(new Path(input), true)
            //3.遍历文件，并读取文件类容成成rdd，结构为（文件名，单词）
            var unionrdd = sc.emptyRDD[(String,String)] // rdd声明变量为 var
            while (filelist.hasNext){
              val abs_path = new Path(filelist.next().getPath.toString)
              val file_name = abs_path.getName //文件名称
              val rdd1 = sc.textFile(abs_path.toString).flatMap(_.split(" ").map((file_name,_)))
              //4.将遍历的多个rdd拼接成1个Rdd
              unionrdd = unionrdd.union(rdd1)
            }
            //5.构建词频（（文件名，单词），词频）
            val rdd2 = unionrdd.map(word => {(word, 1)}).reduceByKey(_ + _)
            //6.//调整输出格式,将（文件名，单词），词频）==》 （单词，（文件名，词频）） ==》 （单词，（文件名，词频））汇总
            val frdd1 = rdd2.map(word =>{(word._1._2,String.format("(%s,%s)",word._1._1,word._2.toString))})
            val frdd2 = frdd1.reduceByKey(_ +"," + _)
            val frdd3 = frdd2.map(word =>String.format("\"%s\",{%s}",word._1,word._2))
            frdd3.foreach(println)
        }
    }

执行结果：

<img width="1440" alt="截屏2022-06-12 下午10 09 17" src="https://user-images.githubusercontent.com/19924975/173239662-6b218d8d-ccc7-4c22-abc3-376156412071.png">

作业二：Distcp 的 Spark 实现
使用 Spark 实现 Hadoop 分布式数据传输工具 DistCp (distributed copy)，只要求实现最基础的 copy 功能，对于 -update、-diff、-p 不做要求。

核心代码：

    object sparkDistCp {
        def main(args: Array[String]): Unit = {
        
            val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
            val sc = new SparkContext(sparkConf)
        
            val conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://localhost:9000")
        
            val srcPath = args(0)
            val targetPath = args(1)
        
            var fsShell: FsShell = null
        
            try {
              fsShell = new FsShell(conf)
              fsShell.run(Array("-rm", "-r", targetPath))
              fsShell.run(Array("-mkdir", "-p", targetPath))
              val code = fsShell.run(Array("-cp", srcPath + "/*", targetPath + "/"))
              println(s"Copy $srcPath to $targetPath ${if (code == 0) "success" else "failed"}.")
            } catch {
              case e: Exception =>
                println(s"Copy $srcPath to $targetPath failed")
                e.printStackTrace()
            } finally {
              if (null != fsShell) {
                fsShell.close()
              }
            }
        
        }
    }

执行结果：

<img width="1440" alt="截屏2022-06-12 下午11 14 27" src="https://user-images.githubusercontent.com/19924975/173239934-e69d1adc-f160-4f80-b4e8-165d3f62c4a9.png">

<img width="1440" alt="截屏2022-06-12 下午11 15 50" src="https://user-images.githubusercontent.com/19924975/173239952-9930d39b-16b1-4175-9b87-7fa2c419e2ca.png">

## Week4 环境

·基于EMR大数据平台（本次作业主要使用了EMR大数据平台上的HUE、HIVE和HDFS）

## Week4 作业 核心代码及结果展示

开发前的准备工作：

1>、hive、hdfs 租户隔离 在hdfs的/user目录下创建自己的工作目录（user/qu_chenlong）、在hive创建自己的database（qu_chenlong）

2>、将data/hive下的文件通过hadoop fs -cp的方式复制一份到自己的工作目录下

3>、在hive中，使用use qu_chenlong，到自己创建的hive数环境中创建t_movie、t_rating、t_user表，建表语句如下：

    -- 建t_movie表
    CREATE external TABLE `qu_chenlong.t_movie`(
    `movie_id` bigint COMMENT '电影id', 
    `movie_name` string COMMENT '电影名字', 
    `movie_type` string COMMENT '电影类型')
    ROW FORMAT SERDE 
    'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' 
    WITH SERDEPROPERTIES ( 
    'field.delim'='::')
    LOCATION
    '/user/qu_chenlong/week4/movies';
    
    -- 建t_rating表
    CREATE external TABLE `qu_chenlong.t_rating`(
    `user_id` int COMMENT '用户id', 
    `movie_id` bigint COMMENT '电影id', 
    `rate` int COMMENT '评分', 
    `times` string COMMENT '评分时间')
    ROW FORMAT SERDE 
    'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' 
    WITH SERDEPROPERTIES ( 
    'field.delim'='::') 
    LOCATION 
    '/user/qu_chenlong/week4/ratings';
    
    -- 建t_user表
    CREATE external TABLE `qu_chenlong.t_user`(
    `user_id` int COMMENT '用户id', 
    `sex` string COMMENT '性别', 
    `age` int COMMENT '年龄', 
    `occupation` string COMMENT '职业', 
    `zip_code` bigint COMMENT '邮编')
    ROW FORMAT SERDE 
    'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' 
    WITH SERDEPROPERTIES ( 
    'field.delim'='::') 
    LOCATION
    '/user/qu_chenlong/week4/users';

作业1、展示电影 ID 为 2116 这部电影各年龄段的平均影评分。

    CREATE TABLE answer1
    AS
    SELECT c.age AS age, avg(b.rate) AS avgrate
    FROM t_movie a
	    JOIN t_rating b ON a.movie_id = b.movie_id
	    JOIN t_user c ON c.user_id = b.user_id
    WHERE a.movie_id = 2116
    GROUP BY c.age
    ORDER BY c.age;

作业2、找出男性评分最高且评分次数超过 50 次的 10 部电影，展示电影名，平均影评分和评分次数。

    CREATE TABLE answer2
    AS
    SELECT 'M' AS sex, c.movie_name AS name, avg(a.rate) AS avgrate
	    , count(c.movie_name) AS total
    FROM t_rating a
	    JOIN t_user b ON a.user_id = b.user_id
	    JOIN t_movie c ON a.movie_id = c.movie_id
    WHERE b.sex = 'M'
    GROUP BY c.movie_name
    HAVING total > 50
    ORDER BY avgrate DESC
    LIMIT 10;

作业3、找出影评次数最多的女士所给出最高分的 10 部电影的平均影评分，展示电影名和平均影评分（可使用多行 SQL）。

    CREATE TABLE answer3
    AS
    SELECT c.movie_name AS moviename, avg(b.rate) AS avgrate
    FROM (
	    SELECT movie_id
	    FROM t_rating
	    WHERE user_id = (
		    SELECT a.user_id
		    FROM t_user a
			    JOIN t_rating b ON a.user_id = b.user_id
		    WHERE a.sex = 'F'
		    GROUP BY a.user_id
		    ORDER BY COUNT(1) DESC
		    LIMIT 1
	    )
	    ORDER BY rate DESC
	    LIMIT 10
    ) a
	    JOIN t_rating b ON a.movie_id = b.movie_id
	    JOIN t_movie c ON b.movie_id = c.movie_id
    GROUP BY b.movie_id, c.movie_name;

具体执行结果可进入服务器的hive，use qu_chenlong，通过 select * from answer1、select * from answer2、select * from answer3 进行查看

## Week2 环境

·目前仅在本机搭建伪分布式进行了本地开发

·本机的hadoop版本为2.7.2、hbase版本为2.1.6

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
