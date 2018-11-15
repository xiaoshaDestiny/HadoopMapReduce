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
		//1:获取job对象
				Configuration conf = new Configuration();
				Job job = Job.getInstance(conf);
				
				//2:反射加载jar包路径
				job.setJarByClass(FlowSortDriver.class);
				
				//3:关联mapper和reducer类
				job.setMapperClass(FlowSortMapper.class);
				job.setReducerClass(FlowSortReducer.class);
				
				//4:设置Mapper输出的kv类型
				job.setMapOutputKeyClass(FlowBean.class);
				job.setMapOutputValueClass(Text.class);
				
				//5:设置最终输出的kv类型
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(FlowBean.class);
				
				// 设置分区   决定输出文件的个数
				job.setPartitionerClass(FlowSortPartitioner.class);
				job.setNumReduceTasks(4);
				
				//6:设置输入输出路径
				FileInputFormat.setInputPaths(job, new Path(args[0]));
				FileOutputFormat.setOutputPath(job, new Path(args[1]));
				
				//7:提交
				boolean result =job.waitForCompletion(true);
				System.exit(result?0:1);
	}
}
