package com.learn.flowsum;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * Reduce阶段的输入key      （一个切割出来的词语）电话号码                                   Text
 * Reduce阶段的输入value     一个Bean对象                                                                 FlowBean   
 * Reduce阶段的输出key       电话号码                                                                             Text   
 * Reduce阶段的输出value     一个Bean对象                                                                 FlowBean                
 * @author Administrator
 *
 */
public class FlowSumReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
	
	@Override
	protected void reduce(Text key, Iterable<FlowBean> values, Context context)
			throws IOException, InterruptedException {
		long sum_upFlow = 0;
		long sum_downFlow = 0;
		
		// 1:累加求和
		for (FlowBean flowBean : values) {
			sum_upFlow += flowBean.getUpFlow();
			sum_downFlow += flowBean.getDownFlow();
		}
		FlowBean flowBean = new FlowBean(sum_upFlow,sum_downFlow);
		
		// 2:输出
		context.write(key, flowBean);
	}

}
