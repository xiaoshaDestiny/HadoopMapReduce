package com.learn.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<Text, FlowBean>{
	
	
	@Override
	public int getPartition(Text key, FlowBean value, int numPartitions) {
		
		//1:��ȡ�ֻ������ǰ��λ
		String preNum = key.toString().substring(0, 3);
		
		//2:���ص���ͬ�ķ�����
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
