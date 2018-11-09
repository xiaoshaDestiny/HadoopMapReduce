package com.learn.flowsum;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowSumDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		/**
		 * 1:获取job对象
		 * 2:反射加载jar包路径
		 * 3:关联mapper和reducer类
		 * 4:设置Mapper输出的kv类型
		 * 5:设置最终输出的kv类型
		 * 6:设置输入输出路径
		 * 7:提交
		 */
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(FlowSumDriver.class);
		
		job.setMapperClass(FlowSumMapper.class);
		job.setReducerClass(FlowSumReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean result =job.waitForCompletion(true);
		System.exit(result?0:1);
	}

}
