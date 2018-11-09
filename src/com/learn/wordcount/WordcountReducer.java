package com.learn.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * Reduce�׶�   �ۺ�
 * �����key��Map�׶ε������key
 * �����value��map�׶������value
 * �����key��  ����
 * �����value �ǵ��ʳ��ֵĸ���
 * @author Administrator
 */
public class WordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	//IntWritable fsum = new IntWritable();
	
	// <hello 1>
	// <hello 1>
	// <hello 1>
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,Context context) 
			throws IOException, InterruptedException {
		//1:����ͳ�Ƶ����ܸ���
		int sum = 0;
		for(IntWritable count : values){
			sum +=count.get();
		}
		//2:��������ܸ���
		context.write(key, new IntWritable(sum));
		//		fsum.set(sum);
		//		context.write(key, fsum);
	}

}
