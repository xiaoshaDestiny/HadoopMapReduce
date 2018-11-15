package com.learn.flowsort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区类 4 个分区
 * 此时输入到这里的数据 k 是FlowBean
 *                v  是手机号码
 *                所以是对v 进行的一个手机号码的截取
 * @author Administrator
 *
 */
public class FlowSortPartitioner extends Partitioner<FlowBean,Text>{

	@Override
	public int getPartition(FlowBean key, Text value, int numPartitions) {
		
				//1:获取手机号码的前三位
				String preNum = value.toString().substring(0, 3);
				
				//2:返回到不同的分区里
				int partition = 4;
				if("131".equals(preNum)) {
					partition = 0;
				}else if("132".equals(preNum)) {
					partition = 1;
				}
				else if("133".equals(preNum)) {
					partition = 2;
				}
				else if("134".equals(preNum)) {
					partition = 3;
				}
				
				return partition;
	}
	

}
