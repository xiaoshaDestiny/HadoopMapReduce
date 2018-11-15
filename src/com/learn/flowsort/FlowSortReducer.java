package com.learn.flowsort;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowSortReducer extends Reducer<FlowBean, Text, Text, FlowBean>{
	
	/**
	 * reduce的工作很简单只是单纯的将 输入的k变成输出的v 
	 * 						         输入的v变成输出的k	
	 * 不需要做计算的				
	 */
	@Override
	protected void reduce(FlowBean key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		context.write(values.iterator().next(), key);
	}

}
