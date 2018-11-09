package com.learn.flowsum;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * Map阶段的输入key     行号                                                       LongWritable
 * Map阶段的输入value   一行数据                                               Text
 * Map阶段的输出key     切割出来的一个词语（手机号码）   Text
 * Map阶段的输出value   一个Bean对象                                    FlowBean
 * @author Administrator
 */
public class FlowSumMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
	
	FlowBean v = new FlowBean();
	Text k = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//1:获取一行
		String line = value.toString();
		
		//2:切割出数据
		String[] fields = line.split(" ");
		
		//3:封装数据
		String phoneNum = fields[1];                               //获取手机号码
		long upFlow = Long.parseLong(fields[fields.length-3]);     //获取上行流量
		long downFlow = Long.parseLong(fields[fields.length-2]);   //获取下行流量
		v.set(upFlow, downFlow);
		k.set(phoneNum);

		//4:写出数据
		context.write(k,v);
	}
}
