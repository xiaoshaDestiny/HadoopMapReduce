package com.learn.flowsum;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * Map�׶ε�����key     �к�                                                       LongWritable
 * Map�׶ε�����value   һ������                                               Text
 * Map�׶ε����key     �и������һ������ֻ����룩   Text
 * Map�׶ε����value   һ��Bean����                                    FlowBean
 * @author Administrator
 */
public class FlowSumMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
	
	FlowBean v = new FlowBean();
	Text k = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//1:��ȡһ��
		String line = value.toString();
		
		//2:�и������
		String[] fields = line.split(" ");
		
		//3:��װ����
		String phoneNum = fields[1];                               //��ȡ�ֻ�����
		long upFlow = Long.parseLong(fields[fields.length-3]);     //��ȡ��������
		long downFlow = Long.parseLong(fields[fields.length-2]);   //��ȡ��������
		v.set(upFlow, downFlow);
		k.set(phoneNum);

		//4:д������
		context.write(k,v);
	}
}
