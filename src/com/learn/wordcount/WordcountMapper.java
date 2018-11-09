package com.learn.wordcount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * �����key   : LongWritable   �к�
 * �����value : Text           һ�е�����
 * �����key   : Text           ����
 * �����value : IntWritable    ���ʵĸ���
 * 
 * ������Ҫ�������л��Ķ�������
 * @author xiaosha
 */
public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	Text k = new Text();
	IntWritable v = new IntWritable(1);
	/**
	 * ���������ļ���һ��һ������
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//1:��һ������ת��ΪString 
		String line = value.toString();
		//2:�и�
		String[] words = line.split(" ");
		//3:ѭ��д������һ���׶�
		for(String word : words){
			k.set(word);
			context.write(k,v); //ע�����������
		}
	}
}
