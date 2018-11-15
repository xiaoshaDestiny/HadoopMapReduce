package com.learn.flowsort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * �Զ�������� 4 ������
 * ��ʱ���뵽��������� k ��FlowBean
 *                v  ���ֻ�����
 *                �����Ƕ�v ���е�һ���ֻ�����Ľ�ȡ
 * @author Administrator
 *
 */
public class FlowSortPartitioner extends Partitioner<FlowBean,Text>{

	@Override
	public int getPartition(FlowBean key, Text value, int numPartitions) {
		
				//1:��ȡ�ֻ������ǰ��λ
				String preNum = value.toString().substring(0, 3);
				
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
