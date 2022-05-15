package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowBeanDriver {

    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME","root");

        Path inputPath = new Path("hdfs://127.0.0.1:9000/user/hive/extwarehouse/ods/HTTP_20130313143750.dat");
        Path outputPath = new Path("hdfs://127.0.0.1:9000/user/hive/extwarehouse/output");

        //作为整个Job的配置
        Configuration conf = new Configuration();

        // 1. 设置job运行时没要访问的默认文件系统
//        conf.set("fs.dafaultFS","file:///");
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

}