package com.learn.flowsort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowSortMapper extends Mapper<LongWritable, Text, FlowBean, Text>{

//  要处理的文件 是长这个样子的
//	13324900262	22	22	44
//	13424900262 33	33	66
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FlowBean, Text>.Context context)
			throws IOException, InterruptedException {
		
		FlowBean k = new FlowBean();
		Text v = new Text();
		//1:获取一行
		String line = value.toString();
		//2:切割一行
		String[] fields = line.split("\t");
		
		//3:封装对象
		long upFlow = Long.parseLong(fields[1]);
		long downFlow = Long.parseLong(fields[2]);
		k.set(upFlow, downFlow);
		v.set(fields[0]);
		
		//4:写出
		context.write(k, v);
	}
}
