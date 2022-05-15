package mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowBeanMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private Text outK = new Text();
    private FlowBean outV = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // 单词切分 按空格
        String line = value.toString().trim();
        String[] words = line.split("\t");
        String phone = words[1];
        String upStream = words[8];
        String downStream = words[9];

        outK.set(phone);
        outV.setUpFlow(Long.parseLong(upStream));
        outV.setDownFlow(Long.parseLong(downStream));
        outV.setSumFlow(Long.parseLong(upStream)+Long.parseLong(downStream));

        context.write(outK,outV);
    }
}
