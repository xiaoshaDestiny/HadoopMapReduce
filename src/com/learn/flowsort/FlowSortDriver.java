package com.learn.flowsort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class FlowSortDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//1:��ȡjob����
				Configuration conf = new Configuration();
				Job job = Job.getInstance(conf);
				
				//2:�������jar��·��
				job.setJarByClass(FlowSortDriver.class);
				
				//3:����mapper��reducer��
				job.setMapperClass(FlowSortMapper.class);
				job.setReducerClass(FlowSortReducer.class);
				
				//4:����Mapper�����kv����
				job.setMapOutputKeyClass(FlowBean.class);
				job.setMapOutputValueClass(Text.class);
				
				//5:�������������kv����
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(FlowBean.class);
				
				// ���÷���   ��������ļ��ĸ���
				job.setPartitionerClass(FlowSortPartitioner.class);
				job.setNumReduceTasks(4);
				
				//6:�����������·��
				FileInputFormat.setInputPaths(job, new Path(args[0]));
				FileOutputFormat.setOutputPath(job, new Path(args[1]));
				
				//7:�ύ
				boolean result =job.waitForCompletion(true);
				System.exit(result?0:1);
	}
}
