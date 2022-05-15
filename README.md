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

1>、核心代码

·先创建命名空间 create_namespace 'quchenlong'
