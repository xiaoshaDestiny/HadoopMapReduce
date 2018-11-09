package com.learn.flowsum;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * Reduce�׶ε�����key      ��һ���и�����Ĵ���绰����                                   Text
 * Reduce�׶ε�����value     һ��Bean����                                                                 FlowBean   
 * Reduce�׶ε����key       �绰����                                                                             Text   
 * Reduce�׶ε����value     һ��Bean����                                                                 FlowBean                
 * @author Administrator
 *
 */
public class FlowSumReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
	
	@Override
	protected void reduce(Text key, Iterable<FlowBean> values, Context context)
			throws IOException, InterruptedException {
		long sum_upFlow = 0;
		long sum_downFlow = 0;
		
		// 1:�ۼ����
		for (FlowBean flowBean : values) {
			sum_upFlow += flowBean.getUpFlow();
			sum_downFlow += flowBean.getDownFlow();
		}
		FlowBean flowBean = new FlowBean(sum_upFlow,sum_downFlow);
		
		// 2:���
		context.write(key, flowBean);
	}

}
