package mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowBeanReducer extends Reducer<Text, FlowBean,Text, FlowBean> {

    private FlowBean outV = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        // 1 遍历集合累加值
        long sumUpFlow = 0;
        long sumDownFlow = 0;

        for (FlowBean flowBean : values) {

            sumUpFlow += flowBean.getUpFlow();
            sumDownFlow += flowBean.getDownFlow();

        }

        outV.setUpFlow(sumUpFlow);
        outV.setDownFlow(sumDownFlow);
        outV.setSumFlow(sumDownFlow + sumUpFlow);

        context.write(key, outV);
    }

}