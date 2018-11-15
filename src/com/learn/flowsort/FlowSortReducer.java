package com.learn.flowsort;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowSortReducer extends Reducer<FlowBean, Text, Text, FlowBean>{
	
	/**
	 * reduce�Ĺ����ܼ�ֻ�ǵ����Ľ� �����k��������v 
	 * 						         �����v��������k	
	 * ����Ҫ�������				
	 */
	@Override
	protected void reduce(FlowBean key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		context.write(values.iterator().next(), key);
	}

}
