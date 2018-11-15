package com.learn.flowsort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowSortMapper extends Mapper<LongWritable, Text, FlowBean, Text>{

//  Ҫ������ļ� �ǳ�������ӵ�
//	13324900262	22	22	44
//	13424900262 33	33	66
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FlowBean, Text>.Context context)
			throws IOException, InterruptedException {
		
		FlowBean k = new FlowBean();
		Text v = new Text();
		//1:��ȡһ��
		String line = value.toString();
		//2:�и�һ��
		String[] fields = line.split("\t");
		
		//3:��װ����
		long upFlow = Long.parseLong(fields[1]);
		long downFlow = Long.parseLong(fields[2]);
		k.set(upFlow, downFlow);
		v.set(fields[0]);
		
		//4:д��
		context.write(k, v);
	}
}
