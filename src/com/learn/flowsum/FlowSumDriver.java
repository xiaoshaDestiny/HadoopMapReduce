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
		 * 1:��ȡjob����
		 * 2:�������jar��·��
		 * 3:����mapper��reducer��
		 * 4:����Mapper�����kv����
		 * 5:�������������kv����
		 * 6:�����������·��
		 * 7:�ύ
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
